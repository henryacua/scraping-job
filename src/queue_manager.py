"""
QueueManager — Capa de persistencia async con SQLite.

Actúa como una cola de tareas simple: los negocios se insertan con estado
PENDING y se procesan transicionando por los estados del enum BusinessStatus.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional

import aiosqlite

from src.models import Business, BusinessStatus
from src.utils import setup_logger

logger = setup_logger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS businesses (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    phone         TEXT,
    address       TEXT,
    website       TEXT,
    email         TEXT,
    status        TEXT    NOT NULL DEFAULT 'PENDING',
    search_query  TEXT,
    rating        TEXT,
    reviews_count TEXT,
    category      TEXT,
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS message_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id     INTEGER NOT NULL,
    status          TEXT    NOT NULL, -- 'SENT', 'FAILED', 'SKIPPED'
    sent_at         TEXT    NOT NULL,
    message_template TEXT,
    FOREIGN KEY(business_id) REFERENCES businesses(id)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses(status);
CREATE INDEX IF NOT EXISTS idx_businesses_query  ON businesses(search_query);
CREATE INDEX IF NOT EXISTS idx_message_logs_biz  ON message_logs(business_id);
"""

# ... (QueueManager methods unchanged until end of class) ...




class QueueManager:
    """Wrapper async sobre SQLite para gestionar la cola de negocios."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    # ── Lifecycle ───────────────────────────────────────

    async def initialize(self) -> None:
        """Crea la tabla y los índices si no existen."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(_CREATE_TABLE_SQL)
            await db.executescript(_CREATE_INDEX_SQL)
            
            # Migración simple: verificar si columna email existe
            try:
                await db.execute("ALTER TABLE businesses ADD COLUMN email TEXT")
            except Exception:
                pass  # Columna ya existe

            # Migración: columna filter_reason
            try:
                await db.execute("ALTER TABLE businesses ADD COLUMN filter_reason TEXT")
            except Exception:
                pass  # Columna ya existe

            await db.commit()
        logger.info("Base de datos inicializada en %s", self.db_path)

    # ── Escritura ───────────────────────────────────────

    async def enqueue(self, business: Business) -> int:
        """Inserta un negocio con estado PENDING. Retorna el ID asignado."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO businesses
                   (name, phone, address, website, email, status, search_query,
                    rating, reviews_count, category, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    business.name,
                    business.phone,
                    business.address,
                    business.website,
                    business.email,
                    BusinessStatus.PENDING.value,
                    business.search_query,
                    business.rating,
                    business.reviews_count,
                    business.category,
                    now,
                    now,
                ),
            )
            await db.commit()
            return cursor.lastrowid  # type: ignore[return-value]

    async def enqueue_batch(self, businesses: list[Business]) -> int:
        """Inserta múltiples negocios en una sola transacción. Retorna la cantidad insertada."""
        if not businesses:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (
                b.name, b.phone, b.address, b.website, b.email,
                BusinessStatus.PENDING.value, b.search_query,
                b.rating, b.reviews_count, b.category, now, now,
            )
            for b in businesses
        ]

        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany(
                """INSERT INTO businesses
                   (name, phone, address, website, email, status, search_query,
                    rating, reviews_count, category, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            await db.commit()

        logger.info("Encolados %d negocios", len(rows))
        return len(rows)

    # ── Lectura / Dequeue ───────────────────────────────

    async def dequeue(self, limit: int = 10) -> list[Business]:
        """
        Lee hasta `limit` items PENDING, los marca como PROCESSING y los retorna.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM businesses WHERE status = ? ORDER BY id LIMIT ?",
                (BusinessStatus.PENDING.value, limit),
            )
            rows = await cursor.fetchall()

            if not rows:
                return []

            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" for _ in ids)
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                f"UPDATE businesses SET status = ?, updated_at = ? WHERE id IN ({placeholders})",
                [BusinessStatus.PROCESSING.value, now, *ids],
            )
            await db.commit()

            return [Business.from_dict(dict(row)) for row in rows]

    # ── Actualización ───────────────────────────────────

    async def update_status(
        self, business_id: int, status: BusinessStatus
    ) -> None:
        """Actualiza el estado de un negocio."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE businesses SET status = ?, updated_at = ? WHERE id = ?",
                (status.value, now, business_id),
            )
            await db.commit()

    async def update_filter_reason(
        self, business_id: int, reason: str
    ) -> None:
        """Registra la razón de filtrado de un negocio."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE businesses SET filter_reason = ?, updated_at = ? WHERE id = ?",
                (reason, now, business_id),
            )
            await db.commit()

    # ── Consultas ───────────────────────────────────────

    async def get_qualified_leads(self) -> list[Business]:
        """Retorna todos los leads cualificados."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM businesses WHERE status = ? ORDER BY id",
                (BusinessStatus.LEAD_QUALIFIED.value,),
            )
            rows = await cursor.fetchall()
            return [Business.from_dict(dict(row)) for row in rows]

    async def get_all_businesses(
        self, search_query: Optional[str] = None
    ) -> list[Business]:
        """Retorna todos los negocios, opcionalmente filtrados por búsqueda."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if search_query:
                cursor = await db.execute(
                    "SELECT * FROM businesses WHERE search_query = ? ORDER BY id",
                    (search_query,),
                )
            else:
                cursor = await db.execute(
                    "SELECT * FROM businesses ORDER BY id"
                )
            rows = await cursor.fetchall()
            return [Business.from_dict(dict(row)) for row in rows]

    async def get_stats(self, search_query: Optional[str] = None) -> dict[str, int]:
        """Retorna conteos agrupados por estado."""
        async with aiosqlite.connect(self.db_path) as db:
            if search_query:
                cursor = await db.execute(
                    "SELECT status, COUNT(*) as count FROM businesses WHERE search_query = ? GROUP BY status",
                    (search_query,),
                )
            else:
                cursor = await db.execute(
                    "SELECT status, COUNT(*) as count FROM businesses GROUP BY status"
                )
            rows = await cursor.fetchall()
            stats = {row[0]: row[1] for row in rows}
            stats["TOTAL"] = sum(stats.values())
            return stats

    async def delete_by_query(self, search_query: str) -> int:
        """Elimina todos los registros de una búsqueda. Retorna cantidad eliminada."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM businesses WHERE search_query = ?",
                (search_query,),
            )
            await db.commit()
            deleted = cursor.rowcount
            logger.info("Eliminados %d registros para query '%s'", deleted, search_query)
            return deleted  # type: ignore[return-value]

    async def get_recent_queries(self) -> list[str]:
        """Retorna las búsquedas únicas realizadas, ordenadas por la más reciente."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT search_query FROM businesses "
                "WHERE search_query IS NOT NULL AND search_query != '' "
                "GROUP BY search_query "
                "ORDER BY MAX(id) DESC"
            )
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def log_message(self, business_id: int, status: str, template: str = "") -> None:
        """Registra el resultado de un envío de mensaje."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO message_logs (business_id, status, sent_at, message_template) VALUES (?, ?, ?, ?)",
                (business_id, status, now, template),
            )
            await db.commit()

    async def get_message_logs(self) -> dict[int, str]:
        """Retorna un mapa {business_id: status} de los mensajes enviados."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT business_id, status FROM message_logs")
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}

