"""Tests para backend.app.services.strategies — acciones del pipeline."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from backend.app.models import Business, BusinessStatus
from backend.app.services.strategies import (
    AVAILABLE_STRATEGIES,
    FilterInvalidPhoneAction,
    FilterNoWhatsAppAction,
    get_all_strategies,
    get_strategy,
)


@pytest.fixture
def sample_business():
    return Business(
        id=1,
        name="Test Dental",
        phone="+57 300 111 2222",
        address="Calle 50 #10-20, Medellín",
        website=None,
        status=BusinessStatus.PENDING.value,
        search_query="Dentistas en Medellín",
        category="Dentista",
    )


class TestFilterInvalidPhoneAction:
    @pytest.fixture
    def action(self):
        return FilterInvalidPhoneAction()

    @pytest.mark.asyncio
    async def test_valid_colombian_mobile(self, action, sample_business):
        sample_business.phone = "300 111 2222"
        passed, reason = await action.execute(sample_business)
        assert passed is True
        assert reason is None

    @pytest.mark.asyncio
    async def test_valid_with_country_code(self, action, sample_business):
        sample_business.phone = "+57 300 111 2222"
        passed, reason = await action.execute(sample_business)
        assert passed is True
        assert reason is None

    @pytest.mark.asyncio
    async def test_valid_no_spaces(self, action, sample_business):
        sample_business.phone = "3001112222"
        passed, reason = await action.execute(sample_business)
        assert passed is True

    @pytest.mark.asyncio
    async def test_no_phone_is_filtered(self, action, sample_business):
        sample_business.phone = None
        passed, reason = await action.execute(sample_business)
        assert passed is False
        assert reason and "numero" in reason.lower()

    @pytest.mark.asyncio
    async def test_empty_phone_is_filtered(self, action, sample_business):
        sample_business.phone = "   "
        passed, reason = await action.execute(sample_business)
        assert passed is False
        assert reason and "numero" in reason.lower()

    @pytest.mark.asyncio
    async def test_short_number_is_filtered(self, action, sample_business):
        sample_business.phone = "12345"
        passed, reason = await action.execute(sample_business)
        assert passed is False
        assert reason and "invalido" in reason.lower()

    @pytest.mark.asyncio
    async def test_landline_is_filtered(self, action, sample_business):
        sample_business.phone = "4441234567"
        passed, reason = await action.execute(sample_business)
        assert passed is False

    def test_action_name(self, action):
        assert "telefono" in action.name.lower()


class TestFilterNoWhatsAppAction:
    @pytest.fixture
    def action(self):
        return FilterNoWhatsAppAction(
            api_token="test-token",
            phone_number_id="123456",
        )

    @pytest.mark.asyncio
    async def test_not_configured_passes_all(self, sample_business):
        action = FilterNoWhatsAppAction(api_token="", phone_number_id="")
        passed, reason = await action.execute(sample_business)
        assert passed is True

    @pytest.mark.asyncio
    async def test_no_phone_is_filtered(self, action, sample_business):
        sample_business.phone = None
        passed, reason = await action.execute(sample_business)
        assert passed is False

    @pytest.mark.asyncio
    async def test_valid_whatsapp_contact(self, action, sample_business):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"contacts": [{"status": "valid", "wa_id": "573001112222"}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "backend.app.services.strategies.aiohttp.ClientSession",
            return_value=mock_session,
        ):
            passed, reason = await action.execute(sample_business)
            assert passed is True

    @pytest.mark.asyncio
    async def test_invalid_whatsapp_contact(self, action, sample_business):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"contacts": [{"status": "invalid"}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "backend.app.services.strategies.aiohttp.ClientSession",
            return_value=mock_session,
        ):
            passed, reason = await action.execute(sample_business)
            assert passed is False
            assert reason and "WhatsApp" in reason

    def test_action_name(self, action):
        assert "WhatsApp" in action.name


class TestGetStrategy:
    def test_valid_strategies(self):
        for name in AVAILABLE_STRATEGIES:
            strategy = get_strategy(name)
            assert strategy is not None

    def test_invalid_strategy_raises(self):
        with pytest.raises(ValueError, match="no encontrada"):
            get_strategy("NonExistent")

    def test_get_all_strategies(self):
        strategies = get_all_strategies()
        assert len(strategies) == len(AVAILABLE_STRATEGIES)
