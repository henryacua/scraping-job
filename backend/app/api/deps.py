"""
Dependencias compartidas para todos los routers de FastAPI.
"""
from __future__ import annotations

from typing import Optional

from fastapi import Header, HTTPException

from backend.app.core.config import settings


def verify_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    """Valida X-Api-Key si está configurada. Sin clave configurada, acepta todo."""
    expected = settings.API_KEY
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
