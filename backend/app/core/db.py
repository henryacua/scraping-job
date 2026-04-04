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
from collections.abc import AsyncGenerator

from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app.core.config import settings

_connect_args: dict = {}
_pool_class = None

if settings.DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}
    _pool_class = StaticPool
elif "postgresql" in settings.DATABASE_URL:
    if settings.DATABASE_SSL_INSECURE:
        _ctx = ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode = ssl.CERT_NONE
        _connect_args["ssl"] = _ctx
    else:
        try:
            import certifi

            _connect_args["ssl"] = ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            _connect_args["ssl"] = ssl.create_default_context()
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

engine = create_async_engine(
    settings.DATABASE_URL,
    connect_args=_connect_args,
    poolclass=_pool_class,
)


async def create_db_and_tables() -> None:
    """Crea todas las tablas declaradas en SQLModel.metadata (desarrollo / tests)."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency inyectable de FastAPI que provee una session async."""
    async with AsyncSession(engine) as session:
        yield session
