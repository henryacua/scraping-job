"""
Utilidades comunes: logging y helpers.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from config import settings


def setup_logger(name: str, log_file: str | None = None) -> logging.Logger:
    """
    Configura y retorna un logger con formato estructurado.

    Args:
        name: Nombre del logger (normalmente __name__ del módulo).
        log_file: Ruta opcional a archivo de log.

    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL, logging.INFO))

    # Evitar agregar handlers duplicados
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(name)-20s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (opcional)
    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(file_path), encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def sanitize_text(text: str | None) -> str | None:
    """Limpia y normaliza texto extraído."""
    if text is None:
        return None
    cleaned = text.strip().replace("\n", " ").replace("\t", " ")
    # Colapsar espacios múltiples
    while "  " in cleaned:
        cleaned = cleaned.replace("  ", " ")
    return cleaned if cleaned else None


def normalize_url(url: str | None) -> str | None:
    """Normaliza una URL agregando https:// si falta."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


def normalize_phone(phone: str | None, default_country: str = "57") -> str | None:
    """
    Normaliza un número de teléfono para uso con APIs.

    Elimina caracteres no numéricos y agrega código de país si falta.

    Args:
        phone: Número de teléfono en cualquier formato.
        default_country: Código de país por defecto (sin '+').

    Returns:
        Número limpio (solo dígitos, con código de país) o None si es inválido.
    """
    import re

    if not phone:
        return None

    # Eliminar todo excepto dígitos
    phone_clean = re.sub(r"[^\d]", "", phone)

    if not phone_clean:
        return None

    # Heurísticas para Colombia (+57)
    if len(phone_clean) == 10:
        # Número local colombiano (ej: 3001234567)
        phone_clean = default_country + phone_clean
    elif len(phone_clean) == 7:
        # Número fijo colombiano (ej: 4441234) → 574441234
        phone_clean = default_country + "4" + phone_clean

    return phone_clean
