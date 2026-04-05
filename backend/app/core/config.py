"""
Configuración centralizada con Pydantic Settings.

Lee variables de entorno desde .env (raíz del monorepo) con validación de tipos.
Si una variable requerida falta, la app falla al arrancar con un error claro.
"""
from __future__ import annotations

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # DB — SQLAlchemy async URL
    # Local/tests: sqlite+aiosqlite:///./queue.db
    # Supabase:    postgresql+asyncpg://user:pass@host:5432/postgres
    DATABASE_URL: str = f"sqlite+aiosqlite:///{_PROJECT_ROOT / 'queue.db'}"

    # Postgres/Supabase: true = TLS cifrado sin verificar el certificado del servidor.
    DATABASE_SSL_INSECURE: bool = False

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def use_asyncpg_for_plain_postgresql(cls, v: object) -> object:
        """Supabase/Render suelen pegar postgresql://; el engine async exige +asyncpg."""
        if not isinstance(v, str):
            return v
        s = v.strip()
        if s.startswith("postgres://"):
            return "postgresql+asyncpg://" + s[len("postgres://") :]
        if s.startswith("postgresql://") and not s.startswith("postgresql+"):
            return "postgresql+asyncpg://" + s[len("postgresql://") :]
        return s

    # Maps source: "playwright" (scraper local) o "places_api" (Google API)
    MAPS_SOURCE: str = "playwright"

    # Google Maps Platform — solo necesario si MAPS_SOURCE=places_api
    GOOGLE_MAPS_API_KEY: str = ""

    # Playwright / Scraper
    HEADLESS: bool = True
    MAX_SCROLL_ATTEMPTS: int = 20
    SCROLL_PAUSE_SECONDS: float = 2.0
    CLICK_DELAY_MS: int = 2000

    # HTTP
    HTTP_TIMEOUT: int = 10
    BATCH_SIZE: int = 10

    # Dashboard: "local" ejecuta todo en proceso; "remote" delega a Render vía HTTP.
    DASHBOARD_MODE: str = "local"

    # API interna (Render ↔ Streamlit Cloud) — solo se usa si DASHBOARD_MODE=remote
    RENDER_API_URL: str = ""
    API_KEY: str = ""

    # WhatsApp Cloud API
    WA_API_TOKEN: str = ""
    WA_PHONE_NUMBER_ID: str = ""
    WA_API_VERSION: str = "v21.0"
    WA_TEMPLATE_NAME: str = "hello_world"
    WA_TEMPLATE_LANG: str = "es"
    WA_SEND_DELAY_MIN: float = 1.5
    WA_SEND_DELAY_MAX: float = 3.0

    # Logging
    LOG_LEVEL: str = "INFO"


settings = Settings()
