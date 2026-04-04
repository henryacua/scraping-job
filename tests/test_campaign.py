"""Tests para src/campaign.py — WhatsApp Cloud API con mock de aiohttp."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.models import Business, BusinessStatus
from src.campaign import WhatsAppCloudAPI, WhatsAppAPIError, CampaignStats
from src.utils import normalize_phone


# ── Tests para normalize_phone ──────────────────────────


class TestNormalizePhone:
    def test_none_returns_none(self):
        assert normalize_phone(None) is None

    def test_empty_returns_none(self):
        assert normalize_phone("") is None
        assert normalize_phone("   ") is None

    def test_only_text_returns_none(self):
        assert normalize_phone("sin número") is None

    def test_colombian_10_digit(self):
        assert normalize_phone("300 123 4567") == "573001234567"
        assert normalize_phone("3001234567") == "573001234567"

    def test_colombian_with_country_code(self):
        assert normalize_phone("+57 300 123 4567") == "573001234567"

    def test_colombian_7_digit_fixed(self):
        assert normalize_phone("444 1234") == "5744441234"

    def test_international_already_complete(self):
        result = normalize_phone("+1 555 123 4567")
        assert result == "15551234567"

    def test_strips_special_chars(self):
        assert normalize_phone("(300) 123-4567") == "573001234567"

    def test_custom_country_code(self):
        assert normalize_phone("3001234567", default_country="1") == "13001234567"


# ── Fixtures ────────────────────────────────────────────


@pytest.fixture
def sample_leads():
    return [
        Business(id=1, name="Dental Corp", phone="+57 300 111 2222", search_query="test"),
        Business(id=2, name="No Phone Inc", phone=None, search_query="test"),
        Business(id=3, name="Restaurante ABC", phone="310 555 6666", search_query="test"),
    ]


@pytest.fixture
def mock_log_callback():
    return AsyncMock()


def _make_mock_response(status: int, json_data: dict):
    """Crea un mock de aiohttp response."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=json_data)
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)
    return mock_resp


def _make_mock_session(mock_resp):
    """Crea un mock de aiohttp ClientSession."""
    mock_session = AsyncMock()
    mock_session.post = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


# ── Tests de envío individual ───────────────────────────


class TestSendText:
    @pytest.mark.asyncio
    async def test_send_text_success(self):
        """Envío de texto exitoso retorna datos de la API."""
        api_response = {
            "messaging_product": "whatsapp",
            "contacts": [{"wa_id": "573001112222"}],
            "messages": [{"id": "wamid.abc123"}],
        }
        mock_resp = _make_mock_response(200, api_response)
        mock_session = _make_mock_session(mock_resp)

        client = WhatsAppCloudAPI(
            api_token="test-token",
            phone_number_id="12345",
        )

        with patch("src.campaign.aiohttp.ClientSession", return_value=mock_session):
            result = await client.send_text("573001112222", "Hola mundo")

        assert result == api_response
        mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_text_api_error_raises(self):
        """Error 401 de la API lanza WhatsAppAPIError."""
        error_response = {
            "error": {
                "message": "Invalid access token",
                "type": "OAuthException",
                "code": 190,
            }
        }
        mock_resp = _make_mock_response(401, error_response)
        mock_session = _make_mock_session(mock_resp)

        client = WhatsAppCloudAPI(
            api_token="bad-token",
            phone_number_id="12345",
        )

        with patch("src.campaign.aiohttp.ClientSession", return_value=mock_session):
            with pytest.raises(WhatsAppAPIError) as exc_info:
                await client.send_text("573001112222", "Hola")

            assert exc_info.value.status_code == 401
            assert "Invalid access token" in str(exc_info.value)


class TestSendTemplate:
    @pytest.mark.asyncio
    async def test_send_template_success(self):
        """Envío de template exitoso."""
        api_response = {
            "messaging_product": "whatsapp",
            "messages": [{"id": "wamid.template123"}],
        }
        mock_resp = _make_mock_response(200, api_response)
        mock_session = _make_mock_session(mock_resp)

        client = WhatsAppCloudAPI(
            api_token="test-token",
            phone_number_id="12345",
        )

        with patch("src.campaign.aiohttp.ClientSession", return_value=mock_session):
            result = await client.send_template(
                "573001112222",
                template_name="hello_world",
                language_code="es",
                body_parameters=["Juan"],
            )

        assert result == api_response

        # Verificar que el payload incluye los parámetros
        call_kwargs = mock_session.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["type"] == "template"
        assert payload["template"]["name"] == "hello_world"
        assert payload["template"]["components"][0]["parameters"][0]["text"] == "Juan"


# ── Tests de envío masivo ───────────────────────────────


class TestSendBulk:
    @pytest.mark.asyncio
    async def test_not_configured_returns_empty_stats(self, sample_leads, mock_log_callback):
        """Sin credenciales, retorna stats vacías sin enviar nada."""
        client = WhatsAppCloudAPI(api_token="", phone_number_id="")
        stats = await client.send_bulk(
            sample_leads, "Hola {nombre}", mock_log_callback,
            delay_min=0, delay_max=0,
        )
        assert stats.sent == 0
        assert stats.total == 3
        mock_log_callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_skip_lead_without_phone(self, mock_log_callback):
        """Lead sin teléfono se cuenta como skipped."""
        leads = [Business(id=1, name="No Phone", phone=None, search_query="test")]

        api_response = {"messaging_product": "whatsapp", "messages": [{"id": "x"}]}
        mock_resp = _make_mock_response(200, api_response)
        mock_session = _make_mock_session(mock_resp)

        client = WhatsAppCloudAPI(api_token="token", phone_number_id="12345")

        with patch("src.campaign.aiohttp.ClientSession", return_value=mock_session):
            stats = await client.send_bulk(
                leads, "Hola {nombre}", mock_log_callback,
                delay_min=0, delay_max=0,
            )

        assert stats.skipped == 1
        assert stats.sent == 0
        mock_log_callback.assert_awaited_once_with(1, "SKIPPED", "Hola {nombre}")

    @pytest.mark.asyncio
    async def test_bulk_mixed_results(self, mock_log_callback):
        """Batch con 1 éxito, 1 sin teléfono → verifica contadores."""
        leads = [
            Business(id=1, name="OK Corp", phone="3001112222", search_query="test"),
            Business(id=2, name="No Phone", phone=None, search_query="test"),
        ]

        api_response = {"messaging_product": "whatsapp", "messages": [{"id": "x"}]}
        mock_resp = _make_mock_response(200, api_response)
        mock_session = _make_mock_session(mock_resp)

        client = WhatsAppCloudAPI(api_token="token", phone_number_id="12345")

        with patch("src.campaign.aiohttp.ClientSession", return_value=mock_session):
            stats = await client.send_bulk(
                leads, "Hola {nombre}", mock_log_callback,
                use_template_mode=False,
                delay_min=0, delay_max=0,
            )

        assert stats.sent == 1
        assert stats.skipped == 1
        assert stats.total == 2

    @pytest.mark.asyncio
    async def test_api_failure_counts_as_failed(self, mock_log_callback):
        """Error API se cuenta como failed."""
        leads = [Business(id=1, name="Corp", phone="3001112222", search_query="test")]

        error_response = {"error": {"message": "Rate limit"}}
        mock_resp = _make_mock_response(429, error_response)
        mock_session = _make_mock_session(mock_resp)

        client = WhatsAppCloudAPI(api_token="token", phone_number_id="12345")

        with patch("src.campaign.aiohttp.ClientSession", return_value=mock_session):
            stats = await client.send_bulk(
                leads, "Hola {nombre}", mock_log_callback,
                delay_min=0, delay_max=0,
            )

        assert stats.failed == 1
        assert stats.sent == 0


# ── Tests de propiedades ────────────────────────────────


class TestWhatsAppCloudAPIProperties:
    def test_is_configured_true(self):
        client = WhatsAppCloudAPI(api_token="tok", phone_number_id="123")
        assert client.is_configured is True

    def test_is_configured_false_no_token(self):
        client = WhatsAppCloudAPI(api_token="", phone_number_id="123")
        assert client.is_configured is False

    def test_is_configured_false_no_phone(self):
        client = WhatsAppCloudAPI(api_token="tok", phone_number_id="")
        assert client.is_configured is False

    def test_base_url_format(self):
        client = WhatsAppCloudAPI(
            api_token="tok",
            phone_number_id="12345",
            api_version="v21.0",
        )
        assert client._base_url == "https://graph.facebook.com/v21.0/12345/messages"
