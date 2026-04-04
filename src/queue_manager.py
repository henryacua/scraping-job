"""
QueueManager — Capa de persistencia async.

Backend seleccionado automáticamente por el prefijo de database_url:
- postgresql:// o postgres:// → asyncpg (Supabase / producción)
- cualquier otra cosa          → aiosqlite (SQLite local / tests)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from src.models import Business, BusinessStatus
from src.utils import setup_logger

logger = setup_logger(__name__)


def _is_postgres_url(url: str) -> bool:
    return url.startswith(("postgresql://", "postgres://"))


# ── DDL SQLite ────────────────────────────────────────────

_SQLITE_DDL = """
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
    filter_reason TEXT,
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS message_logs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id      INTEGER NOT NULL,
    status           TEXT    NOT NULL,
    sent_at          TEXT    NOT NULL,
    message_template TEXT,
    FOREIGN KEY(business_id) REFERENCES businesses(id)
);
CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses(status);
CREATE INDEX IF NOT EXISTS idx_businesses_query  ON businesses(search_query);
CREATE INDEX IF NOT EXISTS idx_message_logs_biz  ON message_logs(business_id);
"""

# ── DDL PostgreSQL ────────────────────────────────────────

_PG_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS businesses (
        id            SERIAL PRIMARY KEY,
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
        filter_reason TEXT,
        created_at    TEXT    NOT NULL,
        updated_at    TEXT    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS message_logs (
        id               SERIAL PRIMARY KEY,
        business_id      INTEGER NOT NULL REFERENCES businesses(id),
        status           TEXT    NOT NULL,
        sent_at          TEXT    NOT NULL,
        message_template TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses(status)",
    "CREATE INDEX IF NOT EXISTS idx_businesses_query  ON businesses(search_query)",
    "CREATE INDEX IF NOT EXISTS idx_message_logs_biz  ON message_logs(business_id)",
]


class QueueManager:
    """
    Wrapper async sobre SQLite o Postgres para gestionar la cola de negocios.

    El backend se selecciona automáticamente por el prefijo de database_url:
    - postgresql:// o postgres:// → asyncpg (Supabase / producción)
    - cualquier otra cosa         → aiosqlite (SQLite local / tests)
    """

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pg_pool = None

        if _is_postgres_url(database_url):
            self._backend = "postgres"
            self.db_path: Optional[str] = None
        else:
            self._backend = "sqlite"
            self.db_path = (
                database_url[len("sqlite:///"):]
                if database_url.startswith("sqlite:///")
                else database_url
            )

    # ── Lifecycle ─────────────────────────────────────────

    async def _get_pg_pool(self):
        if self._pg_pool is None:
            import asyncpg
            self._pg_pool = await asyncpg.create_pool(
                self.database_url, min_size=1, max_size=5
            )
        return self._pg_pool

    async def initialize(self) -> None:
        """Crea las tablas e índices si no existen."""
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                for stmt in _PG_TABLES:
                    try:
                        await conn.execute(stmt)
                    except Exception:
                        pass
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                await db.executescript(_SQLITE_DDL)
                for col_def in [
                    "ALTER TABLE businesses ADD COLUMN email TEXT",
                    "ALTER TABLE businesses ADD COLUMN filter_reason TEXT",
                ]:
                    try:
                        await db.execute(col_def)
                    except Exception:
                        pass
                await db.commit()
        logger.info("Base de datos inicializada (%s)", self._backend)

    # ── Escritura ─────────────────────────────────────────

    async def enqueue(self, business: Business) -> int:
        """Inserta un negocio con estado PENDING. Retorna el ID asignado."""
        now = datetime.now(timezone.utc).isoformat()
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    """INSERT INTO businesses
                       (name, phone, address, website, email, status, search_query,
                        rating, reviews_count, category, created_at, updated_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id""",
                    business.name, business.phone, business.address, business.website,
                    business.email, BusinessStatus.PENDING.value, business.search_query,
                    business.rating, business.reviews_count, business.category, now, now,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    """INSERT INTO businesses
                       (name, phone, address, website, email, status, search_query,
                        rating, reviews_count, category, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (business.name, business.phone, business.address, business.website,
                     business.email, BusinessStatus.PENDING.value, business.search_query,
                     business.rating, business.reviews_count, business.category, now, now),
                )
                await db.commit()
                return cursor.lastrowid  # type: ignore[return-value]

    async def enqueue_batch(self, businesses: list[Business]) -> int:
        """Inserta múltiples negocios en una sola transacción. Retorna la cantidad insertada."""
        if not businesses:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (b.name, b.phone, b.address, b.website, b.email,
             BusinessStatus.PENDING.value, b.search_query,
             b.rating, b.reviews_count, b.category, now, now)
            for b in businesses
        ]
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO businesses
                       (name, phone, address, website, email, status, search_query,
                        rating, reviews_count, category, created_at, updated_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                    rows,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(
                    """INSERT INTO businesses
                       (name, phone, address, website, email, status, search_query,
                        rating, reviews_count, category, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    rows,
                )
                await db.commit()
        logger.info("Encolados %d negocios", len(rows))
        return len(rows)

    # ── Lectura / Dequeue ─────────────────────────────────

    async def dequeue(self, limit: int = 10) -> list[Business]:
        """Lee hasta `limit` items PENDING, los marca PROCESSING y los retorna."""
        now = datetime.now(timezone.utc).isoformat()
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    rows = await conn.fetch(
                        "SELECT * FROM businesses WHERE status=$1 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED",
                        BusinessStatus.PENDING.value, limit,
                    )
                    if not rows:
                        return []
                    ids = [row["id"] for row in rows]
                    await conn.execute(
                        "UPDATE businesses SET status=$1, updated_at=$2 WHERE id = ANY($3::int[])",
                        BusinessStatus.PROCESSING.value, now, ids,
                    )
                    return [Business.from_dict(dict(row)) for row in rows]
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM businesses WHERE status=? ORDER BY id LIMIT ?",
                    (BusinessStatus.PENDING.value, limit),
                )
                rows = await cursor.fetchall()
                if not rows:
                    return []
                ids = [row["id"] for row in rows]
                placeholders = ",".join("?" for _ in ids)
                await db.execute(
                    f"UPDATE businesses SET status=?, updated_at=? WHERE id IN ({placeholders})",
                    [BusinessStatus.PROCESSING.value, now, *ids],
                )
                await db.commit()
                return [Business.from_dict(dict(row)) for row in rows]

    # ── Actualización ─────────────────────────────────────

    async def update_status(self, business_id: int, status: BusinessStatus) -> None:
        """Actualiza el estado de un negocio."""
        now = datetime.now(timezone.utc).isoformat()
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE businesses SET status=$1, updated_at=$2 WHERE id=$3",
                    status.value, now, business_id,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE businesses SET status=?, updated_at=? WHERE id=?",
                    (status.value, now, business_id),
                )
                await db.commit()

    async def update_filter_reason(self, business_id: int, reason: str) -> None:
        """Registra la razón de filtrado de un negocio."""
        now = datetime.now(timezone.utc).isoformat()
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE businesses SET filter_reason=$1, updated_at=$2 WHERE id=$3",
                    reason, now, business_id,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE businesses SET filter_reason=?, updated_at=? WHERE id=?",
                    (reason, now, business_id),
                )
                await db.commit()

    # ── Consultas ─────────────────────────────────────────

    async def get_qualified_leads(self) -> list[Business]:
        """Retorna todos los leads cualificados."""
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM businesses WHERE status=$1 ORDER BY id",
                    BusinessStatus.LEAD_QUALIFIED.value,
                )
            return [Business.from_dict(dict(row)) for row in rows]
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM businesses WHERE status=? ORDER BY id",
                    (BusinessStatus.LEAD_QUALIFIED.value,),
                )
                rows = await cursor.fetchall()
            return [Business.from_dict(dict(row)) for row in rows]

    async def get_all_businesses(self, search_query: Optional[str] = None) -> list[Business]:
        """Retorna todos los negocios, opcionalmente filtrados por búsqueda."""
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                if search_query:
                    rows = await conn.fetch(
                        "SELECT * FROM businesses WHERE search_query=$1 ORDER BY id",
                        search_query,
                    )
                else:
                    rows = await conn.fetch("SELECT * FROM businesses ORDER BY id")
            return [Business.from_dict(dict(row)) for row in rows]
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                if search_query:
                    cursor = await db.execute(
                        "SELECT * FROM businesses WHERE search_query=? ORDER BY id",
                        (search_query,),
                    )
                else:
                    cursor = await db.execute("SELECT * FROM businesses ORDER BY id")
                rows = await cursor.fetchall()
            return [Business.from_dict(dict(row)) for row in rows]

    async def get_stats(self, search_query: Optional[str] = None) -> dict[str, int]:
        """Retorna conteos agrupados por estado."""
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                if search_query:
                    rows = await conn.fetch(
                        "SELECT status, COUNT(*) as count FROM businesses WHERE search_query=$1 GROUP BY status",
                        search_query,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT status, COUNT(*) as count FROM businesses GROUP BY status"
                    )
            stats = {row["status"]: row["count"] for row in rows}
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                if search_query:
                    cursor = await db.execute(
                        "SELECT status, COUNT(*) as count FROM businesses WHERE search_query=? GROUP BY status",
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
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM businesses WHERE search_query=$1",
                    search_query,
                )
            deleted = int(result.split()[-1])
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    "DELETE FROM businesses WHERE search_query=?",
                    (search_query,),
                )
                await db.commit()
                deleted = cursor.rowcount  # type: ignore[assignment]
        logger.info("Eliminados %d registros para query '%s'", deleted, search_query)
        return deleted

    async def get_recent_queries(self) -> list[str]:
        """Retorna las búsquedas únicas realizadas, ordenadas por la más reciente."""
        sql = (
            "SELECT search_query FROM businesses "
            "WHERE search_query IS NOT NULL AND search_query != '' "
            "GROUP BY search_query ORDER BY MAX(id) DESC"
        )
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql)
            return [row["search_query"] for row in rows]
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(sql)
                rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def log_message(self, business_id: int, status: str, template: str = "") -> None:
        """Registra el resultado de un envío de mensaje."""
        now = datetime.now(timezone.utc).isoformat()
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO message_logs (business_id, status, sent_at, message_template) VALUES ($1,$2,$3,$4)",
                    business_id, status, now, template,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO message_logs (business_id, status, sent_at, message_template) VALUES (?,?,?,?)",
                    (business_id, status, now, template),
                )
                await db.commit()

    async def get_message_logs(self) -> dict[int, str]:
        """Retorna un mapa {business_id: status} de los mensajes enviados."""
        if self._backend == "postgres":
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT business_id, status FROM message_logs")
            return {row["business_id"]: row["status"] for row in rows}
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT business_id, status FROM message_logs")
                rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}
