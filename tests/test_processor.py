"""Tests para backend.app.services.processor — pipeline de acciones (CRUD mockeado)."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from backend.app.models import Business, BusinessStatus
from backend.app.services.processor import LeadProcessor


@pytest.fixture
def mock_session():
    return MagicMock()


@pytest.fixture
def mock_crud(monkeypatch):
    m = MagicMock()
    m.dequeue = AsyncMock(return_value=[])
    m.update_status = AsyncMock()
    m.update_filter_reason = AsyncMock()
    monkeypatch.setattr("backend.app.services.processor.crud", m)
    return m


@pytest.fixture
def mock_action_pass():
    action = AsyncMock()
    action.execute = AsyncMock(return_value=(True, None))
    action.name = "MockPassAction"
    return action


@pytest.fixture
def mock_action_fail():
    action = AsyncMock()
    action.execute = AsyncMock(return_value=(False, "Razón del filtrado"))
    action.name = "MockFailAction"
    return action


@pytest.fixture
def processor(mock_crud, mock_session, mock_action_pass):
    return LeadProcessor(mock_session, [mock_action_pass], batch_size=10)


class TestLeadProcessor:
    @pytest.mark.asyncio
    async def test_run_empty_queue(self, processor, mock_crud):
        mock_crud.dequeue.return_value = []
        results = await processor.run()
        assert results["passed"] == 0
        assert results["filtered_out"] == 0

    @pytest.mark.asyncio
    async def test_business_passes_all_actions(
        self, mock_crud, mock_session, mock_action_pass
    ):
        biz = Business(
            id=1,
            name="Good Corp",
            phone="3001112222",
            search_query="test",
            status=BusinessStatus.PENDING.value,
        )
        mock_crud.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_session, [mock_action_pass], batch_size=10)
        results = await processor.run()

        assert results["passed"] == 1
        mock_crud.update_status.assert_any_call(
            mock_session, 1, BusinessStatus.LEAD_QUALIFIED
        )

    @pytest.mark.asyncio
    async def test_business_filtered_by_action(
        self, mock_crud, mock_session, mock_action_fail
    ):
        biz = Business(
            id=2,
            name="Bad Corp",
            phone="123",
            search_query="test",
            status=BusinessStatus.PENDING.value,
        )
        mock_crud.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_session, [mock_action_fail], batch_size=10)
        results = await processor.run()

        assert results["filtered_out"] == 1
        mock_crud.update_status.assert_any_call(
            mock_session, 2, BusinessStatus.FILTERED_OUT
        )
        mock_crud.update_filter_reason.assert_called_once()

    @pytest.mark.asyncio
    async def test_pipeline_stops_at_first_failure(
        self, mock_crud, mock_session, mock_action_fail, mock_action_pass
    ):
        biz = Business(
            id=3,
            name="Filtered Corp",
            phone="",
            search_query="test",
            status=BusinessStatus.PENDING.value,
        )
        mock_crud.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(
            mock_session, [mock_action_fail, mock_action_pass], batch_size=10
        )
        results = await processor.run()

        assert results["filtered_out"] == 1
        mock_action_pass.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_actions_all_pass(self, mock_crud, mock_session):
        action1 = AsyncMock()
        action1.execute = AsyncMock(return_value=(True, None))
        action1.name = "Action1"

        action2 = AsyncMock()
        action2.execute = AsyncMock(return_value=(True, None))
        action2.name = "Action2"

        biz = Business(
            id=4,
            name="Great Corp",
            phone="3001234567",
            search_query="test",
            status=BusinessStatus.PENDING.value,
        )
        mock_crud.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_session, [action1, action2], batch_size=10)
        results = await processor.run()

        assert results["passed"] == 1
        action1.execute.assert_called_once()
        action2.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_crud, mock_session):
        action = AsyncMock()
        action.execute = AsyncMock(side_effect=Exception("Boom"))
        action.name = "BrokenAction"

        biz = Business(
            id=5,
            name="Error Corp",
            phone="3001234567",
            search_query="test",
            status=BusinessStatus.PENDING.value,
        )
        mock_crud.dequeue.side_effect = [[biz], []]

        processor = LeadProcessor(mock_session, [action], batch_size=10)
        results = await processor.run()

        assert results["errors"] == 1
        mock_crud.update_status.assert_any_call(mock_session, 5, BusinessStatus.ERROR)
