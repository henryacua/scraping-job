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

from src.models import Business, BusinessStatus
from src.utils import setup_logger, normalize_phone

logger = setup_logger(__name__)


# ── Interfaz Abstracta ──────────────────────────────────

class Action(ABC):
    """Interfaz base para acciones del pipeline de procesamiento."""

    @abstractmethod
    async def execute(self, business: Business) -> tuple[bool, str | None]:
        """
        Ejecuta la acción sobre un negocio.

        Args:
            business: Negocio a procesar.

        Returns:
            Tupla (passed, reason):
                - passed: True si el negocio pasa el filtro / acción.
                - reason: Razón del filtrado si passed=False, None si pasó.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Nombre legible de la acción."""
        ...


# ── Acciones Concretas ──────────────────────────────────


class FilterInvalidPhoneAction(Action):
    """
    Filtra negocios que no tienen un número de teléfono válido.

    Valida:
        1. Que el campo phone no esté vacío.
        2. Que el número tenga formato de teléfono válido (celular colombiano
           de 10 dígitos con prefijo 3, o con código de país +57).
    """

    # Patrón: celular colombiano (3xx) con 10 dígitos, opcionalmente con +57
    _PHONE_PATTERN = re.compile(
        r"^(?:\+?57\s*)?3\d{2}[\s\-]?\d{3}[\s\-]?\d{4}$"
    )

    @property
    def name(self) -> str:
        return "Filtrar teléfonos inválidos"

    async def execute(self, business: Business) -> tuple[bool, str | None]:
        # Sin teléfono → filtrar
        if not business.phone or not business.phone.strip():
            return False, "Sin número de teléfono"

        phone = business.phone.strip()

        # Limpiar para validar
        if not self._is_valid_phone(phone):
            return False, f"Número inválido: '{phone}'"

        return True, None

    def _is_valid_phone(self, phone: str) -> bool:
        """Verifica si el número tiene formato de celular colombiano válido."""
        # Limpiar caracteres comunes de formato
        cleaned = phone.replace("(", "").replace(")", "").replace(".", "")

        if self._PHONE_PATTERN.match(cleaned):
            return True

        # Fallback: extraer solo dígitos y verificar longitud
        digits = re.sub(r"[^\d]", "", phone)

        # 10 dígitos empezando con 3 → celular colombiano
        if len(digits) == 10 and digits.startswith("3"):
            return True

        # 12 dígitos empezando con 573 → con código de país
        if len(digits) == 12 and digits.startswith("573"):
            return True

        return False


class FilterNoWhatsAppAction(Action):
    """
    Filtra negocios cuyo número no está registrado en WhatsApp.

    Usa la WhatsApp Business Cloud API para verificar contactos.
    Si la API no está configurada, se hace skip (pasa todos).
    """

    def __init__(
        self,
        *,
        api_token: str = "",
        phone_number_id: str = "",
        api_version: str = "v21.0",
    ) -> None:
        from config import settings

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
                "WhatsApp API no configurada — saltando verificación de WhatsApp"
            )
            return True, None

        if not business.phone:
            return False, "Sin número para verificar WhatsApp"

        phone_clean = normalize_phone(business.phone)
        if not phone_clean:
            return False, "Número inválido para WhatsApp"

        has_wa = await self._check_whatsapp(phone_clean)
        if not has_wa:
            return False, f"Número {phone_clean} no tiene WhatsApp"

        return True, None

    async def _check_whatsapp(self, phone: str) -> bool:
        """
        Verifica si un número está registrado en WhatsApp usando la API de contactos.

        Usa el endpoint POST /{phone_number_id}/contacts de la API.
        """

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
                async with session.post(
                    url, json=payload, headers=headers
                ) as response:
                    if response.status >= 400:
                        data = await response.json()
                        error_msg = data.get("error", {}).get("message", str(data))
                        logger.warning(
                            "Error verificando WhatsApp para %s: %s", phone, error_msg
                        )
                        # En caso de error de API, dejamos pasar (no filtrar)
                        return True

                    data = await response.json()
                    contacts = data.get("contacts", [])
                    if contacts:
                        status = contacts[0].get("status", "")
                        return status == "valid"

                    return False

        except Exception as e:
            logger.warning("Error conectando a WhatsApp API para %s: %s", phone, e)
            # En caso de error de conexión, dejamos pasar
            return True


# ── Registry de acciones disponibles ────────────────────

AVAILABLE_STRATEGIES: dict[str, type[Action]] = {
    "FilterInvalidPhone": FilterInvalidPhoneAction,
    "FilterNoWhatsApp": FilterNoWhatsAppAction,
}


def get_strategy(name: str, **kwargs) -> Action:
    """
    Factory: retorna una instancia de la estrategia por nombre.

    Args:
        name: Clave del registro (ej: 'FilterInvalidPhone').

    Raises:
        ValueError: Si el nombre no está registrado.
    """
    cls = AVAILABLE_STRATEGIES.get(name)
    if cls is None:
        available = ", ".join(AVAILABLE_STRATEGIES.keys())
        raise ValueError(
            f"Estrategia '{name}' no encontrada. Disponibles: {available}"
        )
    return cls(**kwargs)


def get_all_strategies(**kwargs) -> list[Action]:
    """Retorna instancias de todas las estrategias registradas."""
    return [cls(**kwargs) for cls in AVAILABLE_STRATEGIES.values()]
