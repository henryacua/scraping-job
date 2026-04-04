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

    # Solo desarrollo / redes con inspección SSL: no verificar cadena TLS (riesgo MITM).
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

    # Búsqueda
    SEARCH_QUERY: str = "Dentistas en Medellín"

    # Playwright / Scraper
    HEADLESS: bool = True
    MAX_SCROLL_ATTEMPTS: int = 20
    SCROLL_PAUSE_SECONDS: float = 2.0

    # HTTP
    HTTP_TIMEOUT: int = 10
    BATCH_SIZE: int = 10

    # Salida
    OUTPUT_DIR: str = str(_PROJECT_ROOT / "output")

    # API interna (Render ↔ Streamlit Cloud)
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
