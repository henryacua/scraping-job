"""
Acciones — Pipeline de filtros y transformaciones sobre datos scrapeados.

Define la interfaz abstracta `Action` y acciones concretas que procesan
los negocios extraídos del scraping.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Optional

import aiohttp

from backend.app.models import Business
from backend.app.services.utils import normalize_phone, setup_logger

logger = setup_logger(__name__)


class Action(ABC):
    """Interfaz base para acciones del pipeline de procesamiento."""

    @abstractmethod
    async def execute(self, business: Business) -> tuple[bool, str | None]:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...


class FilterInvalidPhoneAction(Action):
    """Filtra negocios sin teléfono válido (celular colombiano)."""

    _PHONE_PATTERN = re.compile(
        r"^(?:\+?57\s*)?3\d{2}[\s\-]?\d{3}[\s\-]?\d{4}$"
    )

    @property
    def name(self) -> str:
        return "Filtrar telefonos invalidos"

    async def execute(self, business: Business) -> tuple[bool, str | None]:
        if not business.phone or not business.phone.strip():
            return False, "Sin numero de telefono"

        phone = business.phone.strip()
        if not self._is_valid_phone(phone):
            return False, f"Numero invalido: '{phone}'"

        return True, None

    def _is_valid_phone(self, phone: str) -> bool:
        cleaned = phone.replace("(", "").replace(")", "").replace(".", "")
        if self._PHONE_PATTERN.match(cleaned):
            return True

        digits = re.sub(r"[^\d]", "", phone)

        if len(digits) == 10 and digits.startswith("3"):
            return True
        if len(digits) == 12 and digits.startswith("573"):
            return True

        return False


class FilterNoWhatsAppAction(Action):
    """Filtra negocios cuyo número no está registrado en WhatsApp."""

    def __init__(
        self,
        *,
        api_token: str = "",
        phone_number_id: str = "",
        api_version: str = "v21.0",
    ) -> None:
        from backend.app.core.config import settings

        self.api_token = api_token or settings.WA_API_TOKEN
        self.phone_number_id = phone_number_id or settings.WA_PHONE_NUMBER_ID
        self.api_version = api_version
        self._base_url = (
            f"https://graph.facebook.com/{self.api_version}/{self.phone_number_id}"
        )

    @property
    def name(self) -> str:
        return "Filtrar sin WhatsApp"

    @property
    def is_configured(self) -> bool:
        return bool(self.api_token and self.phone_number_id)

    async def execute(self, business: Business) -> tuple[bool, str | None]:
        if not self.is_configured:
            logger.warning(
                "WhatsApp API no configurada — saltando verificacion"
            )
            return True, None

        if not business.phone:
            return False, "Sin numero para verificar WhatsApp"

        phone_clean = normalize_phone(business.phone)
        if not phone_clean:
            return False, "Numero invalido para WhatsApp"

        has_wa = await self._check_whatsapp(phone_clean)
        if not has_wa:
            return False, f"Numero {phone_clean} no tiene WhatsApp"

        return True, None

    async def _check_whatsapp(self, phone: str) -> bool:
        url = f"{self._base_url}/contacts"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "blocking": "wait",
            "contacts": [f"+{phone}"],
            "force_check": True,
        }
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status >= 400:
                        data = await response.json()
                        error_msg = data.get("error", {}).get("message", str(data))
                        logger.warning("Error verificando WhatsApp para %s: %s", phone, error_msg)
                        return True

                    data = await response.json()
                    contacts = data.get("contacts", [])
                    if contacts:
                        return contacts[0].get("status", "") == "valid"
                    return False
        except Exception as e:
            logger.warning("Error conectando a WhatsApp API para %s: %s", phone, e)
            return True


AVAILABLE_STRATEGIES: dict[str, type[Action]] = {
    "FilterInvalidPhone": FilterInvalidPhoneAction,
    "FilterNoWhatsApp": FilterNoWhatsAppAction,
}


def get_strategy(name: str, **kwargs) -> Action:
    cls = AVAILABLE_STRATEGIES.get(name)
    if cls is None:
        available = ", ".join(AVAILABLE_STRATEGIES.keys())
        raise ValueError(f"Estrategia '{name}' no encontrada. Disponibles: {available}")
    return cls(**kwargs)


def get_all_strategies(**kwargs) -> list[Action]:
    return [cls(**kwargs) for cls in AVAILABLE_STRATEGIES.values()]
