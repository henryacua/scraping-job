"""
Modelos SQLModel — tablas de DB y schemas de API.

Reemplaza el dataclass anterior por SQLModel tables que sirven tanto
para el ORM (SQLAlchemy) como para la validación (Pydantic).
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Field, SQLModel


class BusinessStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    LEAD_QUALIFIED = "LEAD_QUALIFIED"
    HAS_WEBSITE = "HAS_WEBSITE"
    FILTERED_OUT = "FILTERED_OUT"
    ERROR = "ERROR"


def utc_now() -> datetime:
    """UTC sin tzinfo: Postgres/asyncpg con TIMESTAMP sin time zone y SQLite lo exigen así."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ── Table models ──────────────────────────────────────────


class Business(SQLModel, table=True):
    __tablename__ = "businesses"
    # Streamlit reejecuta el script; evita "Table already defined" en SQLModel.metadata.
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=255)
    phone: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    status: str = Field(default=BusinessStatus.PENDING.value, index=True)
    search_query: Optional[str] = Field(default=None, index=True)
    rating: Optional[str] = None
    reviews_count: Optional[str] = None
    category: Optional[str] = None
    filter_reason: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class MessageLog(SQLModel, table=True):
    __tablename__ = "message_logs"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    business_id: int = Field(foreign_key="businesses.id", index=True)
    status: str
    sent_at: datetime = Field(default_factory=utc_now)
    message_template: Optional[str] = None


# ── API schemas (sin table=True) ─────────────────────────


class BusinessPublic(SQLModel):
    id: int
    name: str
    phone: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    status: str
    search_query: Optional[str] = None
    rating: Optional[str] = None
    reviews_count: Optional[str] = None
    category: Optional[str] = None
    filter_reason: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class BusinessCreate(SQLModel):
    name: str
    phone: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    search_query: str = ""
    rating: Optional[str] = None
    reviews_count: Optional[str] = None
    category: Optional[str] = None
