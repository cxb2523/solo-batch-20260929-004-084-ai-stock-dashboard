"""Persistent task orchestration: enqueue, scheduling, retries, recovery.

All queue state lives in a SQLite database. This module owns every piece of
queue logic so HTTP handlers and audit templates stay thin.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from math import ceil
from typing import Any, Awaitable, Callable

import aiosqlite

logger = logging.getLogger("tasksvc.queue")

MAX_ATTEMPTS = 3
MAX_CONCURRENCY = 4
RATE_LIMIT_WINDOW_SECONDS = 60.0
RATE_LIMIT_MAX_NEW_TASKS = 5
STALE_RUNNING_SECONDS = 90
SCHEDULER_TICK_SECONDS = 0.2

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"

Handler = Callable[[dict[str, Any]], Awaitable[Any]]


def utc_now_epoch() -> float:
    return time.time()


def epoch_to_iso(epoch: float) -> str:
    return (
        datetime.fromtimestamp(epoch, timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def iso_to_epoch(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


class RateLimitExceeded(Exception):
    """Raised when the 60s sliding-window quota for new tasks is exhausted."""

    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after
        super().__init__(f"rate limit exceeded, retry after {retry_after}s")


async def _echo_handler(payload: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "payload": payload}


async def _fail_handler(payload: dict[str, Any]) -> None:
    raise RuntimeError(payload.get("message", "forced failure"))


class TaskQueue:
    def __init__(self, db_path: str = "jobs.db") -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()
        self._in_flight: set[asyncio.Task[None]] = set()
        self._scheduler_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._handlers: dict[str, Handler] = {
            "echo": _echo_handler,
            "fail": _fail_handler,
        }
        self._started = False

    def register_action(self, name: str, handler: Handler) -> None:
        self._handlers[name] = handler

    async def start(self) -> None:
        if self._started:
            return
        self._db = await aiosqlite.connect(self.db_path, isolation_level=None)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._init_schema()
        requeued, failed = await self._recover_stale_running()
        if requeued or failed:
            logger.info(
                "startup recovery: requeued=%s failed=%s", requeued, failed
            )
        self._stop.clear()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._started = True

    async def close(self) -> None:
        if not self._started:
            return
        self._started = False
        self._stop.set()
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            await asyncio.gather(self._scheduler_task, return_exceptions=True)
        # Workers are cancelled mid-flight; their rows stay "running" and are
        # re-enqueued by the recovery pass on the next start.
        for task in self._in_flight:
            task.cancel()
        await asyncio.gather(*self._in_flight, return_exceptions=True)
        self._in_flight.clear()
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _init_schema(self) -> None:
        assert self._db is not None
        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id           TEXT PRIMARY KEY,
                payload      TEXT NOT NULL,
                status       TEXT NOT NULL,
                attempts     INTEGER NOT NULL DEFAULT 0,
                enqueued_at  TEXT NOT NULL,
                started_at   TEXT,
                finished_at  TEXT,
                available_at TEXT NOT NULL,
                last_error   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_status
                ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_enqueued_at
                ON jobs(enqueued_at);
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                key        TEXT PRIMARY KEY,
                job_id     TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

    async def _fetchone(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> aiosqlite.Row | None:
        assert self._db is not None
        async with self._db.execute(sql, params) as cursor:
            return await cursor.fetchone()

    async def _fetchall(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[aiosqlite.Row]:
        assert self._db is not None
        async with self._db.execute(sql, params) as cursor:
            return list(await cursor.fetchall())

    # ---------------------------------------------------------------- enqueue

    async def enqueue(
        self,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        """Create a new job. Returns ``(job_id, reused)``.

        A repeat submission carrying the same X-Idempotency-Key reuses the
        original job id and does not consume any rate-limit quota.
        """
        now = utc_now_epoch()
        now_iso = epoch_to_iso(now)
        job_id = uuid.uuid4().hex

        # Single asyncio loop: the write lock serialises submitters; the
        # UNIQUE primary key on idempotency_keys is the durable backstop.
        async with self._write_lock:
            assert self._db is not None
            db = self._db

            if idempotency_key is not None:
                row = await self._fetchone(
                    "SELECT job_id FROM idempotency_keys WHERE key = ?",
                    (idempotency_key,),
                )
                if row is not None:
                    return row["job_id"], True

            await db.execute("BEGIN IMMEDIATE")
            try:
                # Re-check inside the transaction: the idempotency row and
                # the job row are always written atomically together.
                if idempotency_key is not None:
                    row = await self._fetchone(
                        "SELECT job_id FROM idempotency_keys WHERE key = ?",
                        (idempotency_key,),
                    )
                    if row is not None:
                        await db.execute("COMMIT")
                        return row["job_id"], True

                retry_after = await self._rate_limit_retry_after(now)
                if retry_after is not None:
                    await db.execute("ROLLBACK")
                    raise RateLimitExceeded(retry_after)

                await db.execute(
                    """
                    INSERT INTO jobs (id, payload, status, attempts,
                                      enqueued_at, available_at)
                    VALUES (?, ?, ?, 0, ?, ?)
                    """,
                    (
                        job_id,
                        json.dumps(payload, separators=(",", ":")),
                        STATUS_QUEUED,
                        now_iso,
                        now_iso,
                    ),
                )
                if idempotency_key is not None:
                    await db.execute(
                        """
                        INSERT INTO idempotency_keys (key, job_id, created_at)
                        VALUES (?, ?, ?)
                        """,
                        (idempotency_key, job_id, now_iso),
                    )
                await db.execute("COMMIT")
            except RateLimitExceeded:
                raise
            except Exception:
                await db.execute("ROLLBACK")
                raise

        return job_id, False

    async def _rate_limit_retry_after(self, now: float) -> int | None:
        """Sliding 60s window: at most 5 brand-new tasks per window."""
        cutoff_iso = epoch_to_iso(now - RATE_LIMIT_WINDOW_SECONDS)
        row = await self._fetchone(
            """
            SELECT COUNT(*) AS n, MIN(enqueued_at) AS oldest
            FROM jobs
            WHERE enqueued_at >= ?
            """,
            (cutoff_iso,),
        )
        count = row["n"] if row is not None else 0
        if count < RATE_LIMIT_MAX_NEW_TASKS:
            return None
        oldest_epoch = iso_to_epoch(row["oldest"])
        return max(1, ceil(oldest_epoch + RATE_LIMIT_WINDOW_SECONDS - now))

    # ------------------------------------------------------------- scheduling

    async def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._dispatch_ready_jobs()
            except Exception:
                logger.exception("scheduler tick failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), SCHEDULER_TICK_SECONDS
                )
            except asyncio.TimeoutError:
                pass

    async def _dispatch_ready_jobs(self) -> None:
        if len(self._in_flight) >= MAX_CONCURRENCY:
            return
        now_iso = epoch_to_iso(utc_now_epoch())
        claimed: list[str] = []

        async with self._write_lock:
            assert self._db is not None
            db = self._db
            running_row = await self._fetchone(
                "SELECT COUNT(*) AS n FROM jobs WHERE status = ?",
                (STATUS_RUNNING,),
            )
            db_running = running_row["n"] if running_row is not None else 0
            slots = MAX_CONCURRENCY - max(db_running, len(self._in_flight))
            if slots <= 0:
                return

            rows = await self._fetchall(
                """
                SELECT id FROM jobs
                WHERE status = ? AND available_at <= ?
                ORDER BY enqueued_at, id
                LIMIT ?
                """,
                (STATUS_QUEUED, now_iso, slots),
            )
            for row in rows:
                # Conditional claim guards against double dispatch.
                cur = await db.execute(
                    """
                    UPDATE jobs
                    SET status = ?, started_at = ?, attempts = attempts + 1
                    WHERE id = ? AND status = ?
                    """,
                    (STATUS_RUNNING, now_iso, row["id"], STATUS_QUEUED),
                )
                if cur.rowcount == 1:
                    claimed.append(row["id"])

        for job_id in claimed:
            task = asyncio.create_task(self._run_job(job_id))
            self._in_flight.add(task)
            task.add_done_callback(self._in_flight.discard)

    async def _run_job(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job is None or job["status"] != STATUS_RUNNING:
            return
        attempts = job["attempts"]
        try:
            payload = json.loads(job["payload"])
            if isinstance(payload, dict):
                action = payload.get("action", "echo")
            else:
                action = "echo"
            handler = self._handlers.get(action, _echo_handler)
            await handler(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "job %s attempt %s failed: %r", job_id, attempts, exc
            )
            await self._fail_or_requeue(job_id, attempts, exc)
        else:
            async with self._write_lock:
                assert self._db is not None
                await self._db.execute(
                    """
                    UPDATE jobs
                    SET status = ?, finished_at = ?, last_error = NULL
                    WHERE id = ?
                    """,
                    (
                        STATUS_SUCCEEDED,
                        epoch_to_iso(utc_now_epoch()),
                        job_id,
                    ),
                )

    async def _fail_or_requeue(
        self, job_id: str, attempts: int, exc: Exception
    ) -> None:
        message = f"{type(exc).__name__}: {exc}"[:1000]
        async with self._write_lock:
            assert self._db is not None
            if attempts >= MAX_ATTEMPTS:
                await self._db.execute(
                    """
                    UPDATE jobs
                    SET status = ?, finished_at = ?, last_error = ?
                    WHERE id = ?
                    """,
                    (
                        STATUS_FAILED,
                        epoch_to_iso(utc_now_epoch()),
                        f"max retries ({MAX_ATTEMPTS}) exceeded; "
                        f"last error: {message}",
                        job_id,
                    ),
                )
                return

            delay = 2 ** (attempts - 1)  # 1s then 2s backoff
            available_iso = epoch_to_iso(utc_now_epoch() + delay)
            await self._db.execute(
                """
                UPDATE jobs
                SET status = ?, started_at = NULL,
                    available_at = ?, last_error = ?
                WHERE id = ?
                """,
                (STATUS_QUEUED, available_iso, message, job_id),
            )

    # -------------------------------------------------------------- recovery

    async def _recover_stale_running(self) -> tuple[int, int]:
        """Re-enqueue running jobs left over from a previous process.

        Only jobs whose ``started_at`` is older than 90 seconds are touched;
        succeeded jobs are never affected. Jobs already out of retries are
        marked failed with an explanatory reason.
        """
        cutoff = utc_now_epoch() - STALE_RUNNING_SECONDS
        requeued = failed = 0
        async with self._write_lock:
            assert self._db is not None
            rows = await self._fetchall(
                """
                SELECT id, attempts, started_at FROM jobs
                WHERE status = ? AND started_at IS NOT NULL
                """,
                (STATUS_RUNNING,),
            )
            for row in rows:
                if iso_to_epoch(row["started_at"]) >= cutoff:
                    continue
                now_iso = epoch_to_iso(utc_now_epoch())
                if row["attempts"] >= MAX_ATTEMPTS:
                    await self._db.execute(
                        """
                        UPDATE jobs
                        SET status = ?, finished_at = ?, last_error = ?
                        WHERE id = ?
                        """,
                        (
                            STATUS_FAILED,
                            now_iso,
                            "marked failed after restart: job was running "
                            f"for more than {STALE_RUNNING_SECONDS}s and "
                            f"the retry limit ({MAX_ATTEMPTS}) was reached",
                            row["id"],
                        ),
                    )
                    failed += 1
                else:
                    await self._db.execute(
                        """
                        UPDATE jobs
                        SET status = ?, started_at = NULL,
                            available_at = ?,
                            last_error = ?
                        WHERE id = ?
                        """,
                        (
                            STATUS_QUEUED,
                            now_iso,
                            "re-enqueued after restart: previous process "
                            f"died while job was running for more than "
                            f"{STALE_RUNNING_SECONDS}s",
                            row["id"],
                        ),
                    )
                    requeued += 1
        return requeued, failed

    # -------------------------------------------------------------- queries

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        )
        if row is None:
            return None
        return dict(row)

    async def retry_failed_job(self, job_id: str) -> bool:
        """Manual retry of a failed job from the audit page.

        The job keeps its identity and original ``enqueued_at`` so the retry
        never counts as a new task against the rate limit.
        """
        now_iso = epoch_to_iso(utc_now_epoch())
        async with self._write_lock:
            assert self._db is not None
            cur = await self._db.execute(
                """
                UPDATE jobs
                SET status = ?, attempts = 0, started_at = NULL,
                    finished_at = NULL, available_at = ?, last_error = NULL
                WHERE id = ? AND status = ?
                """,
                (STATUS_QUEUED, now_iso, job_id, STATUS_FAILED),
            )
            return cur.rowcount == 1

    async def get_audit_data(self, limit: int = 20) -> dict[str, Any]:
        counts_row = await self._fetchone(
            """
            SELECT
                SUM(status = 'succeeded') AS succeeded,
                SUM(status = 'failed') AS failed,
                SUM(status = 'queued') AS queued
            FROM jobs
            """
        )
        rows = await self._fetchall(
            """
            SELECT id, status, attempts, enqueued_at, last_error
            FROM jobs
            ORDER BY enqueued_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return {
            "counts": {
                "succeeded": counts_row["succeeded"] or 0,
                "failed": counts_row["failed"] or 0,
                "queued": counts_row["queued"] or 0,
            },
            "jobs": [dict(row) for row in rows],
        }
