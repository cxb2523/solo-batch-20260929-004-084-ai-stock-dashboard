"""HTTP entry point: uvicorn app.main:app --port 8000"""

import html
import json
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .queue import RateLimited, TaskQueue


@asynccontextmanager
async def lifespan(app):
    queue = TaskQueue(os.environ.get("JOBS_DB_PATH", "jobs.db"))
    await queue.start()
    app.state.queue = queue
    yield
    await queue.stop()
    queue.close()


app = FastAPI(lifespan=lifespan)


@app.post("/tasks")
async def create_task(request: Request, x_idempotency_key: str = Header(None)):
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    key = x_idempotency_key or uuid.uuid4().hex
    queue = request.app.state.queue
    try:
        task_id, created = queue.enqueue(key, json.dumps(payload))
    except RateLimited as exc:
        return JSONResponse(
            {"detail": "rate limit exceeded, retry later"},
            status_code=429,
            headers={"Retry-After": str(exc.retry_after)},
        )
    return {"task_id": task_id, "created": created}


@app.get("/tasks/{task_id}")
async def get_task(request: Request, task_id: str):
    row = request.app.state.queue.get(task_id)
    if row is None:
        return JSONResponse({"detail": "not found"}, status_code=404)
    return row


@app.post("/tasks/{task_id}/retry")
async def retry_task(request: Request, task_id: str):
    ok = request.app.state.queue.retry(task_id)
    if not ok:
        return JSONResponse(
            {"detail": "task not found or not failed"}, status_code=409
        )
    return RedirectResponse("/audit", status_code=303)


AUDIT_PAGE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>Task Audit</title>
<style>
body {{ font-family: sans-serif; margin: 2em; background: #111; color: #eee; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #444; padding: 6px 10px; text-align: left; }}
th {{ background: #222; }}
.counts span {{ display: inline-block; margin-right: 1.5em; padding: 6px 12px;
  border-radius: 6px; background: #222; }}
.succeeded {{ color: #6f6; }} .failed {{ color: #f66; }}
.queued {{ color: #fc6; }} .running {{ color: #6cf; }}
button {{ cursor: pointer; }}
</style>
</head>
<body>
<h1>Task Audit</h1>
<div class="counts">
  <span class="succeeded">succeeded: {succeeded}</span>
  <span class="failed">failed: {failed}</span>
  <span class="queued">queued: {queued}</span>
</div>
<p>auto-refresh every 5s</p>
<table>
<tr><th>id</th><th>status</th><th>retries</th><th>enqueued at</th><th></th></tr>
{rows}
</table>
</body>
</html>
"""

ROW_TEMPLATE = (
    '<tr><td>{tid}</td><td class="{status}">{status}</td>'
    "<td>{retries}</td><td>{created}</td><td>{action}</td></tr>"
)


def _fmt_time(epoch):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))


@app.get("/audit", response_class=HTMLResponse)
async def audit(request: Request):
    queue = request.app.state.queue
    counts = queue.stats()
    rows = []
    for job in queue.recent(20):
        action = ""
        if job["status"] == "failed":
            action = (
                '<form method="post" action="/tasks/{tid}/retry">'
                "<button type=\"submit\">retry</button></form>"
            ).format(tid=html.escape(job["id"]))
        rows.append(
            ROW_TEMPLATE.format(
                tid=html.escape(job["id"]),
                status=html.escape(job["status"]),
                retries=job["retries"],
                created=html.escape(_fmt_time(job["created_at"])),
                action=action,
            )
        )
    return AUDIT_PAGE.format(rows="\n".join(rows), **counts)
