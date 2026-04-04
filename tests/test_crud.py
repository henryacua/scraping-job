"""Tests para backend.app.crud con SQLite async temporal."""
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app import crud
from backend.app.models import Business, BusinessStatus


@pytest.fixture
async def session(tmp_path):
    path = tmp_path / "test.db"
    url = f"sqlite+aiosqlite:///{path}"
    engine = create_async_engine(url, connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    async with AsyncSession(engine, expire_on_commit=False) as sess:
        yield sess

    await engine.dispose()


@pytest.mark.asyncio
async def test_initialize_via_metadata(session):
    import aiosqlite
    path = session.bind.url.database  # type: ignore[attr-defined]
    async with aiosqlite.connect(path) as db:
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='businesses'"
        )
        assert await cur.fetchone() is not None


@pytest.mark.asyncio
async def test_enqueue_returns_business_with_id(session):
    biz = Business(name="Test Corp", phone="123", search_query="test")
    out = await crud.enqueue(session, biz)
    assert out.id is not None
    assert out.id > 0


@pytest.mark.asyncio
async def test_enqueue_batch(session):
    businesses = [Business(name=f"Corp {i}", search_query="test") for i in range(5)]
    count = await crud.enqueue_batch(session, businesses)
    assert count == 5


@pytest.mark.asyncio
async def test_enqueue_batch_empty(session):
    count = await crud.enqueue_batch(session, [])
    assert count == 0


@pytest.mark.asyncio
async def test_dequeue_returns_pending(session):
    businesses = [Business(name=f"Corp {i}", search_query="test") for i in range(3)]
    await crud.enqueue_batch(session, businesses)

    dequeued = await crud.dequeue(session, limit=2)
    assert len(dequeued) == 2
    assert all(b.name.startswith("Corp") for b in dequeued)


@pytest.mark.asyncio
async def test_dequeue_marks_processing(session):
    biz = Business(name="Solo Corp", search_query="test")
    await crud.enqueue(session, biz)

    dequeued = await crud.dequeue(session, limit=10)
    assert len(dequeued) == 1

    dequeued_again = await crud.dequeue(session, limit=10)
    assert len(dequeued_again) == 0


@pytest.mark.asyncio
async def test_update_status(session):
    biz = Business(name="Status Corp", search_query="test")
    saved = await crud.enqueue(session, biz)
    assert saved.id is not None

    await crud.update_status(session, saved.id, BusinessStatus.LEAD_QUALIFIED)

    leads = await crud.get_qualified_leads(session)
    assert len(leads) == 1
    assert leads[0].name == "Status Corp"


@pytest.mark.asyncio
async def test_get_stats(session):
    businesses = [Business(name=f"Corp {i}", search_query="test") for i in range(4)]
    await crud.enqueue_batch(session, businesses)

    dequeued = await crud.dequeue(session, limit=2)
    for b in dequeued:
        await crud.update_status(session, b.id, BusinessStatus.LEAD_QUALIFIED)  # type: ignore[arg-type]

    stats = await crud.get_stats(session)
    assert stats["PENDING"] == 2
    assert stats["LEAD_QUALIFIED"] == 2
    assert stats["TOTAL"] == 4


@pytest.mark.asyncio
async def test_get_all_businesses(session):
    businesses = [
        Business(name="A", search_query="q1"),
        Business(name="B", search_query="q2"),
        Business(name="C", search_query="q1"),
    ]
    await crud.enqueue_batch(session, businesses)

    all_biz = await crud.get_all_businesses(session)
    assert len(all_biz) == 3

    filtered = await crud.get_all_businesses(session, search_query="q1")
    assert len(filtered) == 2


@pytest.mark.asyncio
async def test_delete_by_query(session):
    businesses = [
        Business(name="A", search_query="delete_me"),
        Business(name="B", search_query="keep_me"),
        Business(name="C", search_query="delete_me"),
    ]
    await crud.enqueue_batch(session, businesses)

    deleted = await crud.delete_by_query(session, "delete_me")
    assert deleted == 2

    remaining = await crud.get_all_businesses(session)
    assert len(remaining) == 1
    assert remaining[0].name == "B"
