"""Tests para src/queue_manager.py — usa SQLite in-memory."""
import pytest
from src.models import Business, BusinessStatus
from src.queue_manager import QueueManager


@pytest.fixture
async def queue(tmp_path):
    """Crea un QueueManager con DB temporal."""
    db_path = str(tmp_path / "test_queue.db")
    qm = QueueManager(db_path)
    await qm.initialize()
    return qm


@pytest.mark.asyncio
async def test_initialize_creates_table(queue):
    """La tabla businesses debe existir tras initialize."""
    import aiosqlite
    async with aiosqlite.connect(queue.db_path) as db:
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='businesses'"
        )
        row = await cursor.fetchone()
        assert row is not None


@pytest.mark.asyncio
async def test_enqueue_single(queue):
    biz = Business(name="Test Corp", phone="123", search_query="test")
    row_id = await queue.enqueue(biz)
    assert row_id is not None
    assert row_id > 0


@pytest.mark.asyncio
async def test_enqueue_batch(queue):
    businesses = [
        Business(name=f"Corp {i}", search_query="test") for i in range(5)
    ]
    count = await queue.enqueue_batch(businesses)
    assert count == 5


@pytest.mark.asyncio
async def test_enqueue_batch_empty(queue):
    count = await queue.enqueue_batch([])
    assert count == 0


@pytest.mark.asyncio
async def test_dequeue_returns_pending(queue):
    businesses = [
        Business(name=f"Corp {i}", search_query="test") for i in range(3)
    ]
    await queue.enqueue_batch(businesses)

    dequeued = await queue.dequeue(limit=2)
    assert len(dequeued) == 2
    # Los dequeued deben tener nombre
    assert all(b.name.startswith("Corp") for b in dequeued)


@pytest.mark.asyncio
async def test_dequeue_marks_processing(queue):
    biz = Business(name="Solo Corp", search_query="test")
    await queue.enqueue(biz)

    dequeued = await queue.dequeue(limit=10)
    assert len(dequeued) == 1

    # Intentar dequeue de nuevo no debe retornar nada (ya están PROCESSING)
    dequeued_again = await queue.dequeue(limit=10)
    assert len(dequeued_again) == 0


@pytest.mark.asyncio
async def test_update_status(queue):
    biz = Business(name="Status Corp", search_query="test")
    row_id = await queue.enqueue(biz)

    await queue.update_status(row_id, BusinessStatus.LEAD_QUALIFIED)

    leads = await queue.get_qualified_leads()
    assert len(leads) == 1
    assert leads[0].name == "Status Corp"


@pytest.mark.asyncio
async def test_get_stats(queue):
    businesses = [
        Business(name=f"Corp {i}", search_query="test") for i in range(4)
    ]
    await queue.enqueue_batch(businesses)

    # Marcar algunos
    dequeued = await queue.dequeue(limit=2)
    for b in dequeued:
        await queue.update_status(b.id, BusinessStatus.LEAD_QUALIFIED)

    stats = await queue.get_stats()
    assert stats["PENDING"] == 2
    assert stats["LEAD_QUALIFIED"] == 2
    assert stats["TOTAL"] == 4


@pytest.mark.asyncio
async def test_get_all_businesses(queue):
    businesses = [
        Business(name="A", search_query="q1"),
        Business(name="B", search_query="q2"),
        Business(name="C", search_query="q1"),
    ]
    await queue.enqueue_batch(businesses)

    all_biz = await queue.get_all_businesses()
    assert len(all_biz) == 3

    filtered = await queue.get_all_businesses(search_query="q1")
    assert len(filtered) == 2


@pytest.mark.asyncio
async def test_delete_by_query(queue):
    businesses = [
        Business(name="A", search_query="delete_me"),
        Business(name="B", search_query="keep_me"),
        Business(name="C", search_query="delete_me"),
    ]
    await queue.enqueue_batch(businesses)

    deleted = await queue.delete_by_query("delete_me")
    assert deleted == 2

    remaining = await queue.get_all_businesses()
    assert len(remaining) == 1
    assert remaining[0].name == "B"
