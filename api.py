"""
Compatibilidad con despliegues que usan: uvicorn api:app

La aplicación real vive en backend.app.main. Este módulo solo re-exporta `app`.
"""
from __future__ import annotations

from backend.app.main import app

__all__ = ["app"]
