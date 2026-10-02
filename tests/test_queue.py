from __future__ import annotations

import asyncio

import aiosqlite

from app.queue import (
    RateLimitExceeded,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    TaskQueue,
    epoch_to_iso,
    utc_now_epoch,
)


async def test_concurrent_idempotent_enqueue_creates_one_row(queue):
    async def submit():
        return await queue.enqueue(
            {"action": "echo", "n": 1}, idempotency_key="key-xyz"
        )

    results = await asyncio.gather(*(submit() for _ in range(8)))

    ids = {job_id for job_id, _ in results}
    assert ids == {results[0][0]}
    assert sum(1 for _, reused in results if not reused) == 1

    async with aiosqlite.connect(queue.db_path) as db:
        job_count = (
            await (await db.execute("SELECT COUNT(*) FROM jobs")).fetchone()
        )[0]
        key_count = (
            await (
                await db.execute(
                    "SELECT COUNT(*) FROM idempotency_keys"
                )
            ).fetchone()
        )[0]
    assert job_count == 1
    assert key_count == 1


async def test_idempotent_repeats_do_not_consume_rate_limit(queue):
    job_id, created = await queue.enqueue({}, idempotency_key="k1")
    assert created is False

    for _ in range(10):
        reused_id, reused = await queue.enqueue(
            {"x": 1}, idempotency_key="k1"
        )
        assert reused is True
        assert reused_id == job_id

    # One new task already in the window -> four more must be accepted.
    for n in range(4):
        await queue.enqueue({}, idempotency_key=f"other-{n}")

    try:
        await queue.enqueue({}, idempotency_key="one-too-many")
    except RateLimitExceeded as exc:
        assert 1 <= exc.retry_after <= 60
    else:
        raise AssertionError("expected RateLimitExceeded on 6th new task")


async def test_rate_limit_blocks_sixth_distinct_task(queue):
    for n in range(5):
        await queue.enqueue({}, idempotency_key=f"new-{n}")
    try:
        await queue.enqueue({})
    except RateLimitExceeded as exc:
        assert 1 <= exc.retry_after <= 60
    else:
        raise AssertionError("6th new task within 60s must be rejected")


async def test_concurrency_cap_is_four_and_excess_jobs_stay_queued(queue):
    released = asyncio.Event()

    async def hold(_payload):
        await released.wait()

    queue.register_action("hold", hold)

    for n in range(5):
        await queue.enqueue({"action": "hold"}, idempotency_key=f"h-{n}")

    deadline = utc_now_epoch() + 5
    while utc_now_epoch() < deadline:
        async with aiosqlite.connect(queue.db_path) as db:
            row = await (
                await db.execute(
                    "SELECT "
                    "SUM(status='running') AS r, "
                    "SUM(status='queued') AS q FROM jobs"
                )
            ).fetchone()
        running, queued = row
        if running == 4 and queued == 1:
            break
        await asyncio.sleep(0.1)
    assert running == 4
    assert queued == 1

    released.set()
    deadline = utc_now_epoch() + 5
    while utc_now_epoch() < deadline:
        job = await queue.get_job((await _first_job_id(queue)))
        if job is None:
            break
        async with aiosqlite.connect(queue.db_path) as db:
            total = (
                await (
                    await db.execute(
                        "SELECT COUNT(*) FROM jobs WHERE status='succeeded'"
                    )
                ).fetchone()
            )[0]
        if total == 5:
            break
        await asyncio.sleep(0.1)
    assert total == 5


async def _first_job_id(queue):
    row = await queue._fetchone("SELECT id FROM jobs LIMIT 1")
    return row["id"]


async def test_failing_job_retries_three_times_then_failed(queue):
    job_id, _ = await queue.enqueue(
        {"action": "fail", "message": "boom"}, idempotency_key="f1"
    )

    deadline = utc_now_epoch() + 20
    job = await queue.get_job(job_id)
    while utc_now_epoch() < deadline and job["status"] != STATUS_FAILED:
        await asyncio.sleep(0.2)
        job = await queue.get_job(job_id)

    assert job["status"] == STATUS_FAILED
    assert job["attempts"] == 3
    assert "max retries (3) exceeded" in job["last_error"]
    assert "boom" in job["last_error"]


async def test_manual_retry_resets_failed_job(queue):
    job_id, _ = await queue.enqueue({"action": "fail"}, idempotency_key="f2")

    deadline = utc_now_epoch() + 20
    while utc_now_epoch() < deadline:
        job = await queue.get_job(job_id)
        if job["status"] == STATUS_FAILED:
            break
        await asyncio.sleep(0.2)

    ok = await queue.retry_failed_job(job_id)
    assert ok is True
    job = await queue.get_job(job_id)
    assert job["status"] in (STATUS_QUEUED, STATUS_RUNNING)
    assert job["attempts"] <= 1
    assert job["last_error"] is None

    assert await queue.retry_failed_job("does-not-exist") is False


async def test_startup_recovery_requeues_stale_and_keeps_succeeded(
    tmp_path,
):
    db_path = str(tmp_path / "recover.db")
    now = utc_now_epoch()

    def iso(seconds_ago):
        return epoch_to_iso(now - seconds_ago)

    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """CREATE TABLE jobs (
                id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT,
                attempts INTEGER DEFAULT 0, enqueued_at TEXT, started_at TEXT,
                finished_at TEXT, available_at TEXT, last_error TEXT
            )"""
        )
        await db.execute(
            """CREATE TABLE idempotency_keys (
                key TEXT PRIMARY KEY, job_id TEXT, created_at TEXT
            )"""
        )
        rows = [
            ("stale-retry", STATUS_RUNNING, 1, iso(120), iso(120), None),
            ("stale-dead", STATUS_RUNNING, 3, iso(200), iso(200), None),
            ("young-running", STATUS_RUNNING, 1, iso(10), iso(10), None),
            ("done", STATUS_SUCCEEDED, 1, iso(300), iso(300), iso(290)),
        ]
        for jid, status, attempts, enq, started, finished in rows:
            await db.execute(
                """INSERT INTO jobs (id, payload, status, attempts,
                    enqueued_at, started_at, finished_at, available_at)
                    VALUES (?, '{}', ?, ?, ?, ?, ?, ?)""",
                (jid, status, attempts, enq, started, finished, enq),
            )
        await db.commit()

    recovered = TaskQueue(db_path)
    released = asyncio.Event()
    recovered.register_action("echo", lambda payload: released.wait())
    await recovered.start()

    stale_retry = await recovered.get_job("stale-retry")
    stale_dead = await recovered.get_job("stale-dead")
    young = await recovered.get_job("young-running")
    done = await recovered.get_job("done")

    assert stale_retry["status"] in (STATUS_QUEUED, STATUS_RUNNING)
    # attempts stays 1 if still queued, becomes 2 once reclaimed.
    assert stale_retry["attempts"] in (1, 2)
    assert "re-enqueued after restart" in stale_retry["last_error"]

    assert stale_dead["status"] == STATUS_FAILED
    assert "marked failed after restart" in stale_dead["last_error"]
    assert stale_dead["finished_at"] is not None

    assert young["status"] == STATUS_RUNNING
    assert young["started_at"] == iso(10)

    assert done["status"] == STATUS_SUCCEEDED

    released.set()
    await recovered.close()
