"""
Configuración centralizada del proyecto.
Lee variables de entorno desde .env o usa valores por defecto.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


# ── Búsqueda ────────────────────────────────────────────
SEARCH_QUERY: str = os.getenv("SEARCH_QUERY", "Dentistas en Medellín")

# ── Base de datos ───────────────────────────────────────
DB_PATH: str = os.getenv("DB_PATH", str(_PROJECT_ROOT / "queue.db"))

# ── Playwright / Scraper ────────────────────────────────
HEADLESS: bool = os.getenv("HEADLESS", "true").lower() == "true"
MAX_SCROLL_ATTEMPTS: int = int(os.getenv("MAX_SCROLL_ATTEMPTS", "20"))
SCROLL_PAUSE_SECONDS: float = float(os.getenv("SCROLL_PAUSE_SECONDS", "2"))

# ── Verificación HTTP ───────────────────────────────────
HTTP_TIMEOUT: int = int(os.getenv("HTTP_TIMEOUT", "10"))
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "10"))

# ── Salida ──────────────────────────────────────────────
OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", str(_PROJECT_ROOT / "output"))

# ── WhatsApp Cloud API ─────────────────────────────────
WA_API_TOKEN: str = os.getenv("WA_API_TOKEN", "")
WA_PHONE_NUMBER_ID: str = os.getenv("WA_PHONE_NUMBER_ID", "")
WA_API_VERSION: str = os.getenv("WA_API_VERSION", "v21.0")
WA_TEMPLATE_NAME: str = os.getenv("WA_TEMPLATE_NAME", "hello_world")
WA_TEMPLATE_LANG: str = os.getenv("WA_TEMPLATE_LANG", "es")
WA_SEND_DELAY_MIN: float = float(os.getenv("WA_SEND_DELAY_MIN", "1.5"))
WA_SEND_DELAY_MAX: float = float(os.getenv("WA_SEND_DELAY_MAX", "3.0"))

# ── Logging ─────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
