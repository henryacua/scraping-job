"""
Async database engine y session factory.

SQLAlchemy detecta el backend automáticamente por el prefijo de DATABASE_URL:
- sqlite+aiosqlite:///  → SQLite (local / tests)
- postgresql+asyncpg:// → Postgres (Supabase / producción)
"""
from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app.core.config import settings

_connect_args = {}
_pool_class = None

if settings.DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}
    _pool_class = StaticPool

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
