"""
Utilidades comunes: logging y helpers.
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

from backend.app.core.config import settings


def setup_logger(name: str, log_file: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL, logging.INFO))

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(file_path), encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def sanitize_text(text: str | None) -> str | None:
    if text is None:
        return None
    cleaned = text.strip().replace("\n", " ").replace("\t", " ")
    while "  " in cleaned:
        cleaned = cleaned.replace("  ", " ")
    return cleaned if cleaned else None


def normalize_url(url: str | None) -> str | None:
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
    Heurísticas para Colombia (+57).
    """
    if not phone:
        return None

    phone_clean = re.sub(r"[^\d]", "", phone)
    if not phone_clean:
        return None

    if len(phone_clean) == 10:
        phone_clean = default_country + phone_clean
    elif len(phone_clean) == 7:
        phone_clean = default_country + "4" + phone_clean

    return phone_clean
