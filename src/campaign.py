"""
Campaign Automation Module — WhatsApp Business Cloud API

Envía mensajes masivos a leads cualificados usando la API oficial de WhatsApp Cloud
(Meta for Developers). Soporta envío por template aprobado o texto libre.

Requisitos:
    - WA_API_TOKEN: Token de acceso (System User permanente o temporal de sandbox)
    - WA_PHONE_NUMBER_ID: ID del número de teléfono registrado en Meta
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Callable, Optional

import aiohttp

from src.models import Business
from src.utils import setup_logger, normalize_phone
from config import settings

logger = setup_logger(__name__)

# Endpoint base de la API de WhatsApp Cloud
WA_API_BASE = "https://graph.facebook.com"


@dataclass
class CampaignStats:
    total: int = 0
    sent: int = 0
    failed: int = 0
    skipped: int = 0


class WhatsAppCloudAPI:
    """Cliente async para la WhatsApp Business Cloud API."""

    def __init__(
        self,
        *,
        api_token: str = settings.WA_API_TOKEN,
        phone_number_id: str = settings.WA_PHONE_NUMBER_ID,
        api_version: str = settings.WA_API_VERSION,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.api_token = api_token
        self.phone_number_id = phone_number_id
        self.api_version = api_version
        self._on_progress = on_progress
        self._base_url = f"{WA_API_BASE}/{api_version}/{phone_number_id}/messages"

    @property
    def is_configured(self) -> bool:
        """Retorna True si las credenciales están configuradas."""
        return bool(self.api_token and self.phone_number_id)

    def _emit(self, msg: str) -> None:
        if self._on_progress:
            self._on_progress(msg)
        logger.info(msg)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    # ── Envío individual ────────────────────────────────

    async def send_text(self, phone: str, message: str) -> dict:
        """
        Envía un mensaje de texto libre.

        NOTA: Solo funciona dentro de la ventana de 24h
        (el destinatario debe haber escrito primero).

        Args:
            phone: Número en formato internacional sin '+' (ej: '573001234567').
            message: Texto del mensaje.

        Returns:
            Respuesta de la API como diccionario.
        """
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": phone,
            "type": "text",
            "text": {"preview_url": False, "body": message},
        }
        return await self._send_request(payload)

    async def send_template(
        self,
        phone: str,
        template_name: str = settings.WA_TEMPLATE_NAME,
        language_code: str = settings.WA_TEMPLATE_LANG,
        body_parameters: Optional[list[str]] = None,
    ) -> dict:
        """
        Envía un mensaje de template aprobado.

        Puede iniciar conversaciones (fuera de la ventana de 24h).

        Args:
            phone: Número en formato internacional sin '+'.
            template_name: Nombre del template aprobado en Meta.
            language_code: Código de idioma del template (ej: 'es').
            body_parameters: Lista de parámetros para el body del template.

        Returns:
            Respuesta de la API como diccionario.
        """
        template: dict = {
            "name": template_name,
            "language": {"code": language_code},
        }

        # Agregar parámetros del body si los hay
        if body_parameters:
            template["components"] = [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": param}
                        for param in body_parameters
                    ],
                }
            ]

        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": phone,
            "type": "template",
            "template": template,
        }
        return await self._send_request(payload)

    async def _send_request(self, payload: dict) -> dict:
        """Ejecuta el POST a la API de WhatsApp Cloud."""
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self._base_url,
                json=payload,
                headers=self._headers(),
            ) as response:
                data = await response.json()

                if response.status >= 400:
                    error_msg = data.get("error", {}).get("message", str(data))
                    raise WhatsAppAPIError(
                        f"API error {response.status}: {error_msg}",
                        status_code=response.status,
                        response_data=data,
                    )

                return data

    # ── Envío masivo ────────────────────────────────────

    async def send_bulk(
        self,
        leads: list[Business],
        template: str,
        log_callback: Callable[[int, str, str], None],
        *,
        use_template_mode: bool = True,
        template_name: str = settings.WA_TEMPLATE_NAME,
        template_lang: str = settings.WA_TEMPLATE_LANG,
        delay_min: float = settings.WA_SEND_DELAY_MIN,
        delay_max: float = settings.WA_SEND_DELAY_MAX,
    ) -> CampaignStats:
        """
        Envía mensajes a una lista de leads.

        Args:
            leads: Lista de objetos Business.
            template: Plantilla de texto con {nombre} (para modo texto libre).
            log_callback: Función async (business_id, status, template) para guardar en DB.
            use_template_mode: Si True, usa template aprobado. Si False, envía texto libre.
            template_name: Nombre del template de Meta (solo modo template).
            template_lang: Idioma del template (solo modo template).
            delay_min: Delay mínimo entre mensajes (segundos).
            delay_max: Delay máximo entre mensajes (segundos).
        """
        stats = CampaignStats(total=len(leads))

        if not self.is_configured:
            self._emit("❌ Credenciales de WhatsApp API no configuradas.")
            return stats

        mode_label = "Template" if use_template_mode else "Texto libre"
        self._emit(f"📢 Comenzando campaña ({mode_label}) para {len(leads)} contactos...")

        for i, lead in enumerate(leads):
            if not lead.phone:
                self._emit(f"⏭️ [{i+1}/{len(leads)}] {lead.name}: Sin teléfono. Saltando.")
                stats.skipped += 1
                await log_callback(lead.id, "SKIPPED", template)  # type: ignore
                continue

            phone_clean = normalize_phone(lead.phone)

            if not phone_clean:
                self._emit(f"⏭️ [{i+1}/{len(leads)}] {lead.name}: Teléfono inválido. Saltando.")
                stats.skipped += 1
                await log_callback(lead.id, "SKIPPED", template)  # type: ignore
                continue

            try:
                self._emit(f"📨 [{i+1}/{len(leads)}] Enviando a {lead.name} ({phone_clean})...")

                if use_template_mode:
                    # Modo template: enviar template con nombre como parámetro
                    await self.send_template(
                        phone=phone_clean,
                        template_name=template_name,
                        language_code=template_lang,
                        body_parameters=[lead.name or "Amigo"],
                    )
                else:
                    # Modo texto libre: reemplazar {nombre} y enviar
                    message = template.replace("{nombre}", lead.name or "Amigo")
                    await self.send_text(phone=phone_clean, message=message)

                self._emit(f"✅ [{i+1}/{len(leads)}] Enviado correctamente.")
                stats.sent += 1
                await log_callback(lead.id, "SENT", template)  # type: ignore

            except WhatsAppAPIError as e:
                self._emit(f"❌ [{i+1}/{len(leads)}] Error API: {e}")
                stats.failed += 1
                await log_callback(lead.id, f"FAILED_{e.status_code}", template)  # type: ignore

            except Exception as e:
                self._emit(f"❌ [{i+1}/{len(leads)}] Error inesperado: {e}")
                stats.failed += 1
                await log_callback(lead.id, "ERROR", template)  # type: ignore

            # Delay entre mensajes para respetar rate limits
            if i < len(leads) - 1:
                wait_time = random.uniform(delay_min, delay_max)
                self._emit(f"   💤 Esperando {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

        self._emit(
            f"🏁 Campaña finalizada. Enviados: {stats.sent}, "
            f"Fallidos: {stats.failed}, Saltados: {stats.skipped}"
        )
        return stats


class WhatsAppAPIError(Exception):
    """Error específico de la API de WhatsApp Cloud."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        response_data: Optional[dict] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data or {}
