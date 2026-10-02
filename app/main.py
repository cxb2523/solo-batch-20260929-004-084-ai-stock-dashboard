"""FastAPI entry point: uvicorn app.main:app --port 8000."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.queue import RateLimitExceeded, TaskQueue

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def create_app(db_path: str | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("JOBS_DB", os.path.join(BASE_DIR, "jobs.db"))
    queue = TaskQueue(db_path)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await queue.start()
        try:
            yield
        finally:
            await queue.close()

    app = FastAPI(title="tasksvc", lifespan=lifespan)
    app.state.queue = queue

    @app.post("/tasks")
    async def create_task(
        request: Request,
        x_idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid JSON body")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="body must be a JSON object")
        try:
            job_id, reused = await queue.enqueue(payload, x_idempotency_key)
        except RateLimitExceeded as exc:
            raise HTTPException(
                status_code=429,
                detail="rate limit exceeded: max 5 new tasks per 60s window",
                headers={"Retry-After": str(exc.retry_after)},
            )
        job = await queue.get_job(job_id)
        return JSONResponse(
            status_code=200 if reused else 201,
            content={
                "task_id": job_id,
                "reused": reused,
                "status": job["status"] if job else "queued",
            },
        )

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        job = await queue.get_job(task_id)
        if job is None:
            raise HTTPException(status_code=404, detail="task not found")
        return job

    @app.post("/tasks/{task_id}/retry")
    async def retry_task(task_id: str) -> RedirectResponse:
        if not await queue.retry_failed_job(task_id):
            raise HTTPException(
                status_code=409,
                detail="task is not in failed state or does not exist",
            )
        return RedirectResponse(url="/audit", status_code=303)

    @app.get("/audit", response_class=HTMLResponse)
    async def audit_page(request: Request) -> HTMLResponse:
        data = await queue.get_audit_data(limit=20)
        return templates.TemplateResponse(
            request,
            "audit.html",
            {"counts": data["counts"], "jobs": data["jobs"]},
        )

    @app.get("/")
    async def index() -> RedirectResponse:
        return RedirectResponse(url="/audit", status_code=302)

    return app


app = create_app()
