"""Task orchestration: enqueue, schedule, retry, recovery.

State lives in a SQLite database (jobs.db by default). All queue logic
belongs here; HTTP handlers and templates stay dumb.
"""

import asyncio
import json
import sqlite3
import threading
import time
import uuid

RATE_LIMIT = 5            # max new tasks per window
RATE_WINDOW = 60.0        # sliding window in seconds
MAX_CONCURRENCY = 4       # max jobs executing at once
MAX_RETRIES = 3           # retries before a job is marked failed
STALE_RUNNING_SECONDS = 90.0  # running jobs older than this are recovered

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT,
    status TEXT NOT NULL,
    payload TEXT,
    retries INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL
);
CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    created_at REAL NOT NULL
);
"""


class RateLimited(Exception):
    def __init__(self, retry_after):
        super().__init__("rate limit exceeded")
        self.retry_after = retry_after


class TaskQueue:
    def __init__(self, db_path="jobs.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()
        self._loop = None
        self._pending = None
        self._workers = []
        self.current_running = 0
        self.max_concurrent_observed = 0

    def enqueue(self, key, payload):
        """Enqueue a new task. Returns (task_id, created).

        The idempotency record and the job row are written in the same
        transaction, so concurrent submissions with the same key produce
        exactly one row; later callers reuse the existing task_id and do
        not consume rate-limit quota.
        """
        with self._lock:
            now = time.time()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT task_id FROM idempotency WHERE key = ?", (key,)
                ).fetchone()
                if row is not None:
                    self._conn.commit()
                    return row["task_id"], False

                cutoff = now - RATE_WINDOW
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM jobs WHERE created_at > ?",
                    (cutoff,),
                ).fetchone()["c"]
                if count >= RATE_LIMIT:
                    oldest = self._conn.execute(
                        "SELECT MIN(created_at) AS m FROM jobs WHERE created_at > ?",
                        (cutoff,),
                    ).fetchone()["m"]
                    retry_after = max(1, int(oldest + RATE_WINDOW - now) + 1)
                    self._conn.rollback()
                    raise RateLimited(retry_after)

                task_id = uuid.uuid4().hex[:12]
                self._conn.execute(
                    "INSERT INTO jobs (id, idempotency_key, status, payload,"
                    " retries, created_at) VALUES (?, ?, 'queued', ?, 0, ?)",
                    (task_id, key, payload, now),
                )
                self._conn.execute(
                    "INSERT INTO idempotency (key, task_id, created_at)"
                    " VALUES (?, ?, ?)",
                    (key, task_id, now),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        self._dispatch(task_id)
        return task_id, True

    def recover(self):
        """Requeue running tasks whose started_at is older than the stale
        threshold. Succeeded/failed tasks are never touched."""
        with self._lock:
            now = time.time()
            stale = self._conn.execute(
                "SELECT * FROM jobs WHERE status = 'running'"
                " AND started_at IS NOT NULL AND started_at < ?",
                (now - STALE_RUNNING_SECONDS,),
            ).fetchall()
            for row in stale:
                self._fail_or_requeue_locked(
                    row, "recovered: stale running task after restart"
                )
            queued = self._conn.execute(
                "SELECT id FROM jobs WHERE status = 'queued'"
                " ORDER BY created_at"
            ).fetchall()
        for row in queued:
            self._dispatch(row["id"])

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._pending = asyncio.Queue()
        self.recover()
        self._workers = [
            asyncio.create_task(self._worker()) for _ in range(MAX_CONCURRENCY)
        ]

    async def stop(self):
        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

    def close(self):
        with self._lock:
            self._conn.close()

    async def _worker(self):
        while True:
            task_id = await self._pending.get()
            try:
                await self._run_job(task_id)
            finally:
                self._pending.task_done()

    async def _run_job(self, task_id):
        row = self.get(task_id)
        if row is None or row["status"] != "queued":
            return
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'running', started_at = ?"
                " WHERE id = ? AND status = 'queued'",
                (time.time(), task_id),
            )
            self._conn.commit()
        self.current_running += 1
        self.max_concurrent_observed = max(
            self.max_concurrent_observed, self.current_running
        )
        try:
            payload = json.loads(row["payload"] or "{}")
            await asyncio.sleep(float(payload.get("duration", 0.1)))
            if payload.get("fail"):
                raise RuntimeError("task payload requested failure")
            with self._lock:
                self._conn.execute(
                    "UPDATE jobs SET status = 'succeeded', finished_at = ?,"
                    " error = NULL WHERE id = ?",
                    (time.time(), task_id),
                )
                self._conn.commit()
        except Exception as exc:
            requeued = self._record_failure(task_id, str(exc))
            if requeued:
                self._dispatch(task_id)
        finally:
            self.current_running -= 1

    def _record_failure(self, task_id, reason):
        """Returns True if the job was requeued, False if it failed."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                return False
            return self._fail_or_requeue_locked(row, reason)

    def _fail_or_requeue_locked(self, row, reason):
        if row["retries"] >= MAX_RETRIES:
            self._conn.execute(
                "UPDATE jobs SET status = 'failed', error = ?,"
                " finished_at = ? WHERE id = ?",
                (
                    "retry limit %d exceeded: %s" % (MAX_RETRIES, reason),
                    time.time(),
                    row["id"],
                ),
            )
            self._conn.commit()
            return False
        self._conn.execute(
            "UPDATE jobs SET status = 'queued', retries = retries + 1,"
            " started_at = NULL, error = ? WHERE id = ?",
            (reason, row["id"]),
        )
        self._conn.commit()
        return True

    def retry(self, task_id):
        """Manually requeue a failed task. Returns True on success."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET status = 'queued', retries = 0,"
                " error = NULL, started_at = NULL, finished_at = NULL"
                " WHERE id = ? AND status = 'failed'",
                (task_id,),
            )
            self._conn.commit()
        if cur.rowcount == 0:
            return False
        self._dispatch(task_id)
        return True

    def get(self, task_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (task_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def stats(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS c FROM jobs GROUP BY status"
            ).fetchall()
        counts = {"queued": 0, "running": 0, "succeeded": 0, "failed": 0}
        for row in rows:
            counts[row["status"]] = row["c"]
        return counts

    def recent(self, limit=20):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _dispatch(self, task_id):
        if self._pending is not None and self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._pending.put_nowait, task_id
            )
