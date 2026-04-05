"""
Async database engine y session factory.

SQLAlchemy detecta el backend automáticamente por el prefijo de DATABASE_URL:
- sqlite+aiosqlite:///  → SQLite (local / tests)
- postgresql+asyncpg:// → Postgres (Supabase / producción)

Supabase exige TLS; asyncpg debe recibir `ssl`. El pooler en :6543 (PgBouncer
modo transacción) requiere `statement_cache_size=0` con asyncpg.
"""
from __future__ import annotations

import ssl
import sys
from collections.abc import AsyncGenerator

from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool, StaticPool
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app.core.config import settings


def _asyncpg_ssl_context() -> ssl.SSLContext:
    """TLS para asyncpg. Con DATABASE_SSL_INSECURE=true no se verifica el cert del servidor."""
    if settings.DATABASE_SSL_INSECURE:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    return ssl.create_default_context()


_connect_args: dict = {}
_pool_class = None

if settings.DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}
    _pool_class = StaticPool
elif "postgresql" in settings.DATABASE_URL:
    _connect_args["ssl"] = _asyncpg_ssl_context()
    try:
        _parsed = make_url(settings.DATABASE_URL)
        _host = (_parsed.host or "").lower()
        _port = _parsed.port or 5432
        _qdict = dict(_parsed.query) if _parsed.query else {}
        _pgbouncer = str(_qdict.get("pgbouncer", "")).lower() in (
            "true",
            "1",
            "yes",
            "on",
        )
        if _port == 6543 or "pooler" in _host or _pgbouncer:
            _connect_args["statement_cache_size"] = 0
    except Exception:
        pass
    # Streamlit: un loop nuevo por run_async(); sin NullPool, asyncpg deja TLS
    # colgando al cerrar el loop (UI gris / RuntimeError).
    if "streamlit" in sys.modules:
        _pool_class = NullPool

_engine_kw: dict = {"connect_args": _connect_args}
if _pool_class is not None:
    _engine_kw["poolclass"] = _pool_class

engine = create_async_engine(settings.DATABASE_URL, **_engine_kw)


async def create_db_and_tables() -> None:
    """Crea todas las tablas declaradas en SQLModel.metadata (desarrollo / tests)."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency inyectable de FastAPI que provee una session async."""
    async with AsyncSession(engine) as session:
        yield session
