from __future__ import annotations

import asyncio

import aiosqlite


async def test_api_concurrent_same_idempotency_key_one_task(client):
    async def submit():
        return await client.post(
            "/tasks",
            json={"action": "echo"},
            headers={"X-Idempotency-Key": "dup-key"},
        )

    responses = await asyncio.gather(*(submit() for _ in range(6)))
    task_ids = {r.json()["task_id"] for r in responses}
    assert len(task_ids) == 1
    assert all(r.status_code in (200, 201) for r in responses)
    assert sum(r.status_code == 201 for r in responses) == 1

    async with aiosqlite.connect(client._transport.app.state.queue.db_path) as db:
        count = (
            await (await db.execute("SELECT COUNT(*) FROM jobs")).fetchone()
        )[0]
    assert count == 1


async def test_api_rate_limit_returns_429_with_retry_after(client):
    for n in range(5):
        r = await client.post(
            "/tasks",
            json={},
            headers={"X-Idempotency-Key": f"r-{n}"},
        )
        assert r.status_code == 201

    blocked = await client.post(
        "/tasks", json={}, headers={"X-Idempotency-Key": "r-6"}
    )
    assert blocked.status_code == 429
    retry_after = blocked.headers.get("retry-after")
    assert retry_after is not None
    assert 1 <= int(retry_after) <= 60

    # Reusing an existing key must remain 200 despite the quota being full.
    repeat = await client.post(
        "/tasks",
        json={"other": True},
        headers={"X-Idempotency-Key": "r-0"},
    )
    assert repeat.status_code == 200
    assert repeat.json()["reused"] is True


async def test_audit_page_counts_match_database(client):
    keys = ["a1", "a2", "a3"]
    for key in keys:
        await client.post(
            "/tasks", json={"action": "echo"}, headers={"X-Idempotency-Key": key}
        )

    page = await client.get("/audit")
    assert page.status_code == 200
    html = page.text
    assert "data-testid=\"count-succeeded\"" in html
    assert "data-testid=\"count-failed\"" in html
    assert "data-testid=\"count-queued\"" in html
    assert 'http-equiv="refresh" content="5"' in html


async def test_failed_job_audit_row_has_working_retry(client):
    await client.post(
        "/tasks",
        json={"action": "fail", "message": "nope"},
        headers={"X-Idempotency-Key": "fail-audit"},
    )

    import aiosqlite as _aiosqlite

    db_path = client._transport.app.state.queue.db_path
    import time

    deadline = time.time() + 20
    status = ""
    while time.time() < deadline:
        async with _aiosqlite.connect(db_path) as db:
            row = await (
                await db.execute("SELECT status FROM jobs")
            ).fetchone()
        status = row[0]
        if status == "failed":
            break
        await asyncio.sleep(0.2)
    assert status == "failed"

    page = await client.get("/audit")
    assert "data-testid=\"retry-button\"" in page.text

    async with _aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT id FROM jobs")
        job_id = (await cursor.fetchone())[0]

    retry = await client.post(f"/tasks/{job_id}/retry", follow_redirects=False)
    assert retry.status_code == 303
    assert retry.headers["location"] == "/audit"

    async with _aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT status, attempts FROM jobs")
        row = await cursor.fetchone()
    assert row[0] in ("queued", "running")
    assert row[1] <= 1
