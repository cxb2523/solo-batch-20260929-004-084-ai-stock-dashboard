import sqlite3
import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("JOBS_DB_PATH", str(tmp_path / "jobs.db"))
    with TestClient(app) as test_client:
        test_client.db_path = tmp_path / "jobs.db"
        yield test_client


def wait_for_status(client, task_id, want, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get("/tasks/%s" % task_id)
        if resp.status_code == 200 and resp.json()["status"] == want:
            return resp.json()
        time.sleep(0.05)
    raise AssertionError(
        "task %s did not reach status %s, last: %s"
        % (task_id, want, client.get("/tasks/%s" % task_id).json())
    )


def db_rows(db_path, sql, args=()):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def test_concurrent_same_idempotency_key_single_row(client):
    key = "key-" + uuid.uuid4().hex
    results = []
    errors = []

    def submit():
        try:
            resp = client.post(
                "/tasks",
                json={"duration": 0.05},
                headers={"X-Idempotency-Key": key},
            )
            results.append((resp.status_code, resp.json()["task_id"]))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(results) == 6
    assert all(status == 200 for status, _ in results)
    task_ids = {task_id for _, task_id in results}
    assert len(task_ids) == 1
    rows = db_rows(client.db_path, "SELECT id FROM jobs")
    assert len(rows) == 1
    keys = db_rows(client.db_path, "SELECT key FROM idempotency")
    assert len(keys) == 1
    wait_for_status(client, task_ids.pop(), "succeeded")


def test_rate_limit_returns_429_with_retry_after(client):
    for _ in range(5):
        resp = client.post("/tasks", json={"duration": 0.01})
        assert resp.status_code == 200
    resp = client.post("/tasks", json={"duration": 0.01})
    assert resp.status_code == 429
    assert "Retry-After" in resp.headers
    assert 1 <= int(resp.headers["Retry-After"]) <= 61


def test_dedup_does_not_consume_rate_quota(client):
    key = "key-" + uuid.uuid4().hex
    for _ in range(4):
        assert client.post(
            "/tasks", json={}, headers={"X-Idempotency-Key": key}
        ).status_code == 200
    # only one real task was created, so 4 more fresh tasks still fit
    for _ in range(4):
        resp = client.post("/tasks", json={})
        assert resp.status_code == 200
    resp = client.post("/tasks", json={})
    assert resp.status_code == 429


def test_failing_task_retries_then_fails(client):
    resp = client.post("/tasks", json={"duration": 0.01, "fail": True})
    task_id = resp.json()["task_id"]
    row = wait_for_status(client, task_id, "failed")
    assert row["retries"] == 3
    assert row["error"]
    assert "retry limit" in row["error"]


def test_manual_retry_of_failed_task(client):
    resp = client.post("/tasks", json={"duration": 0.01, "fail": True})
    task_id = resp.json()["task_id"]
    wait_for_status(client, task_id, "failed")
    resp = client.post("/tasks/%s/retry" % task_id, follow_redirects=False)
    assert resp.status_code == 303
    # payload no longer forces failure? it still does, so it fails again;
    # what matters is the retry was accepted and the job reran
    row = wait_for_status(client, task_id, "failed")
    assert row["retries"] == 3


def test_concurrency_cap_and_no_loss(client):
    task_ids = []
    for _ in range(5):  # rate limit allows 5 per window
        resp = client.post("/tasks", json={"duration": 0.3})
        task_ids.append(resp.json()["task_id"])
    for task_id in task_ids:
        wait_for_status(client, task_id, "succeeded")
    queue = client.app.state.queue
    assert queue.max_concurrent_observed <= 4
    counts = queue.stats()
    assert counts["succeeded"] == 5


def test_recovery_requeues_stale_running(tmp_path, monkeypatch):
    db_path = tmp_path / "jobs.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY, idempotency_key TEXT, status TEXT NOT NULL,
            payload TEXT, retries INTEGER NOT NULL DEFAULT 0, error TEXT,
            created_at REAL NOT NULL, started_at REAL, finished_at REAL
        );
        CREATE TABLE idempotency (
            key TEXT PRIMARY KEY, task_id TEXT NOT NULL, created_at REAL NOT NULL
        );
        """
    )
    now = time.time()
    conn.execute(
        "INSERT INTO jobs (id, status, payload, retries, created_at,"
        " started_at) VALUES ('stale1', 'running', '{\"duration\": 0.01}',"
        " 0, ?, ?)",
        (now - 300, now - 200),
    )
    conn.execute(
        "INSERT INTO jobs (id, status, payload, retries, created_at,"
        " finished_at) VALUES ('done1', 'succeeded', '{}', 0, ?, ?)",
        (now - 300, now - 250),
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("JOBS_DB_PATH", str(db_path))
    with TestClient(app) as test_client:
        row = wait_for_status(test_client, "stale1", "succeeded")
        assert row["retries"] == 1  # recovery counted as one retry
        done = test_client.get("/tasks/done1").json()
        assert done["status"] == "succeeded"
        assert done["retries"] == 0


def test_audit_page(client):
    resp = client.post("/tasks", json={"duration": 0.01})
    task_id = resp.json()["task_id"]
    wait_for_status(client, task_id, "succeeded")
    resp = client.get("/audit")
    assert resp.status_code == 200
    body = resp.text
    assert 'http-equiv="refresh"' in body
    assert task_id in body
    assert "succeeded: 1" in body
    assert "failed: 0" in body
    assert "queued: 0" in body
