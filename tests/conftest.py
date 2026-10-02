from __future__ import annotations

import sys
from pathlib import Path

import pytest
import pytest_asyncio

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import create_app  # noqa: E402
from app.queue import TaskQueue  # noqa: E402


@pytest_asyncio.fixture
async def queue(tmp_path):
    q = TaskQueue(str(tmp_path / "jobs.db"))
    await q.start()
    yield q
    await q.close()


@pytest_asyncio.fixture
async def client(tmp_path):
    from httpx import ASGITransport, AsyncClient

    app = create_app(str(tmp_path / "jobs-api.db"))
    q: TaskQueue = app.state.queue
    await q.start()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    await q.close()
