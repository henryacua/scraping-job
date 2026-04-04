"""Tests para src/processor.py — Pipeline de acciones."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.models import Business, BusinessStatus
from src.processor import LeadProcessor
from src.queue_manager import QueueManager


@pytest.fixture
def mock_queue():
    qm = AsyncMock(spec=QueueManager)
    qm.dequeue = AsyncMock(return_value=[])
    qm.update_status = AsyncMock()
    qm.update_filter_reason = AsyncMock()
    return qm


@pytest.fixture
def mock_action_pass():
    """Acción que siempre pasa."""
    action = AsyncMock()
    action.execute = AsyncMock(return_value=(True, None))
    action.name = "MockPassAction"
    return action


@pytest.fixture
def mock_action_fail():
    """Acción que siempre filtra."""
    action = AsyncMock()
    action.execute = AsyncMock(return_value=(False, "Razón del filtrado"))
    action.name = "MockFailAction"
    return action


@pytest.fixture
def processor(mock_queue, mock_action_pass):
    return LeadProcessor(mock_queue, [mock_action_pass], batch_size=10)


class TestLeadProcessor:
    """Tests para el flujo de procesamiento."""

    @pytest.mark.asyncio
    async def test_run_empty_queue(self, processor, mock_queue):
        """Sin items PENDING, retorna contadores vacíos."""
        mock_queue.dequeue.return_value = []
        results = await processor.run()
        assert results["passed"] == 0
        assert results["filtered_out"] == 0

    @pytest.mark.asyncio
    async def test_business_passes_all_actions(self, mock_queue, mock_action_pass):
        """Negocio que pasa todas las acciones se marca LEAD_QUALIFIED."""
        biz = Business(id=1, name="Good Corp", phone="3001112222", search_query="test")
        mock_queue.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_queue, [mock_action_pass], batch_size=10)
        results = await processor.run()

        assert results["passed"] == 1
        mock_queue.update_status.assert_any_call(1, BusinessStatus.LEAD_QUALIFIED)

    @pytest.mark.asyncio
    async def test_business_filtered_by_action(self, mock_queue, mock_action_fail):
        """Negocio filtrado se marca FILTERED_OUT con razón."""
        biz = Business(id=2, name="Bad Corp", phone="123", search_query="test")
        mock_queue.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_queue, [mock_action_fail], batch_size=10)
        results = await processor.run()

        assert results["filtered_out"] == 1
        mock_queue.update_status.assert_any_call(2, BusinessStatus.FILTERED_OUT)
        mock_queue.update_filter_reason.assert_called_once()

    @pytest.mark.asyncio
    async def test_pipeline_stops_at_first_failure(
        self, mock_queue, mock_action_fail, mock_action_pass
    ):
        """Si la primera acción filtra, la segunda no se ejecuta."""
        biz = Business(id=3, name="Filtered Corp", phone="", search_query="test")
        mock_queue.dequeue.side_effect = [[biz], []]

        # Fail primero, pass después
        processor = LeadProcessor(
            mock_queue, [mock_action_fail, mock_action_pass], batch_size=10
        )
        results = await processor.run()

        assert results["filtered_out"] == 1
        mock_action_pass.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_actions_all_pass(self, mock_queue):
        """Negocio que pasa múltiples acciones se marca LEAD_QUALIFIED."""
        action1 = AsyncMock()
        action1.execute = AsyncMock(return_value=(True, None))
        action1.name = "Action1"

        action2 = AsyncMock()
        action2.execute = AsyncMock(return_value=(True, None))
        action2.name = "Action2"

        biz = Business(id=4, name="Great Corp", phone="3001234567", search_query="test")
        mock_queue.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_queue, [action1, action2], batch_size=10)
        results = await processor.run()

        assert results["passed"] == 1
        action1.execute.assert_called_once()
        action2.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_queue):
        """Error en una acción marca el negocio como ERROR."""
        action = AsyncMock()
        action.execute = AsyncMock(side_effect=Exception("Boom"))
        action.name = "BrokenAction"

        biz = Business(id=5, name="Error Corp", phone="3001234567", search_query="test")
        mock_queue.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_queue, [action], batch_size=10)
        results = await processor.run()

        assert results["errors"] == 1
        mock_queue.update_status.assert_any_call(5, BusinessStatus.ERROR)
