"""
CRUD — Operaciones de base de datos con SQLModel async.

Reemplaza el QueueManager anterior. Usa AsyncSession (SQLAlchemy 2.0)
por lo que el SQL se genera automáticamente para Postgres y SQLite.
"""
from __future__ import annotations

import logging
from typing import Optional, Sequence

from sqlalchemy import func
from sqlmodel import delete, select
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app.models import Business, BusinessStatus, MessageLog, utc_now

logger = logging.getLogger(__name__)


# ── Escritura ─────────────────────────────────────────────


async def enqueue(session: AsyncSession, business: Business) -> Business:
    """Inserta un negocio con estado PENDING. Retorna el objeto con ID asignado."""
    business.status = BusinessStatus.PENDING.value
    business.created_at = utc_now()
    business.updated_at = utc_now()
    session.add(business)
    await session.commit()
    await session.refresh(business)
    return business


async def enqueue_batch(session: AsyncSession, businesses: list[Business]) -> int:
    """Inserta múltiples negocios en una sola transacción."""
    if not businesses:
        return 0
    now = utc_now()
    for b in businesses:
        b.status = BusinessStatus.PENDING.value
        b.created_at = now
        b.updated_at = now
        session.add(b)
    await session.commit()
    logger.info("Encolados %d negocios", len(businesses))
    return len(businesses)


# ── Lectura / Dequeue ─────────────────────────────────────


async def dequeue(session: AsyncSession, limit: int = 10) -> list[Business]:
    """Lee hasta `limit` items PENDING, los marca PROCESSING y los retorna."""
    stmt = (
        select(Business)
        .where(Business.status == BusinessStatus.PENDING.value)
        .order_by(Business.id)
        .limit(limit)
    )
    result = await session.exec(stmt)
    rows = list(result.all())

    if not rows:
        return []

    now = utc_now()
    for row in rows:
        row.status = BusinessStatus.PROCESSING.value
        row.updated_at = now
        session.add(row)
    await session.commit()
    return rows


# ── Actualización ─────────────────────────────────────────


async def update_status(
    session: AsyncSession, business_id: int, status: BusinessStatus
) -> None:
    """Actualiza el estado de un negocio."""
    biz = await session.get(Business, business_id)
    if biz:
        biz.status = status.value
        biz.updated_at = utc_now()
        session.add(biz)
        await session.commit()


async def update_filter_reason(
    session: AsyncSession, business_id: int, reason: str
) -> None:
    """Registra la razón de filtrado de un negocio."""
    biz = await session.get(Business, business_id)
    if biz:
        biz.filter_reason = reason
        biz.updated_at = utc_now()
        session.add(biz)
        await session.commit()


# ── Consultas ─────────────────────────────────────────────


async def get_qualified_leads(session: AsyncSession) -> Sequence[Business]:
    stmt = (
        select(Business)
        .where(Business.status == BusinessStatus.LEAD_QUALIFIED.value)
        .order_by(Business.id)
    )
    result = await session.exec(stmt)
    return result.all()


async def get_all_businesses(
    session: AsyncSession, search_query: Optional[str] = None
) -> Sequence[Business]:
    stmt = select(Business).order_by(Business.id)
    if search_query:
        stmt = stmt.where(Business.search_query == search_query)
    result = await session.exec(stmt)
    return result.all()


async def get_stats(
    session: AsyncSession, search_query: Optional[str] = None
) -> dict[str, int]:
    stmt = select(Business.status, func.count()).group_by(Business.status)
    if search_query:
        stmt = stmt.where(Business.search_query == search_query)
    result = await session.exec(stmt)
    stats = {row[0]: row[1] for row in result.all()}
    stats["TOTAL"] = sum(stats.values())
    return stats


async def delete_by_query(session: AsyncSession, search_query: str) -> int:
    stmt = delete(Business).where(Business.search_query == search_query)
    result = await session.exec(stmt)
    await session.commit()
    deleted = int(getattr(result, "rowcount", None) or 0)
    logger.info("Eliminados %d registros para query '%s'", deleted, search_query)
    return deleted


async def get_recent_queries(session: AsyncSession) -> list[str]:
    stmt = (
        select(Business.search_query)
        .where(Business.search_query.is_not(None))  # type: ignore[union-attr]
        .where(Business.search_query != "")
        .group_by(Business.search_query)
        .order_by(func.max(Business.id).desc())
    )
    result = await session.exec(stmt)
    return list(result.all())


# ── Message Logs ──────────────────────────────────────────


async def log_message(
    session: AsyncSession, business_id: int, status: str, template: str = ""
) -> None:
    entry = MessageLog(
        business_id=business_id,
        status=status,
        sent_at=utc_now(),
        message_template=template,
    )
    session.add(entry)
    await session.commit()


async def get_message_logs(session: AsyncSession) -> dict[int, str]:
    stmt = select(MessageLog.business_id, MessageLog.status)
    result = await session.exec(stmt)
    return {row[0]: row[1] for row in result.all()}
