"""
Modelos de dominio para el sistema de scraping.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


class BusinessStatus(str, enum.Enum):
    """Estado de un negocio dentro de la cola de procesamiento."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    LEAD_QUALIFIED = "LEAD_QUALIFIED"
    HAS_WEBSITE = "HAS_WEBSITE"
    FILTERED_OUT = "FILTERED_OUT"
    ERROR = "ERROR"


@dataclass
class Business:
    """Representa un negocio extraído de Google Maps."""

    name: str
    phone: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None  # Nuevo campo
    status: BusinessStatus = BusinessStatus.PENDING
    search_query: str = ""
    rating: Optional[str] = None
    reviews_count: Optional[str] = None
    category: Optional[str] = None
    filter_reason: Optional[str] = None
    id: Optional[int] = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        """Serializa el negocio a diccionario."""
        return {
            "id": self.id,
            "name": self.name,
            "phone": self.phone,
            "address": self.address,
            "website": self.website,
            "email": self.email,
            "status": self.status.value if isinstance(self.status, BusinessStatus) else self.status,
            "search_query": self.search_query,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "category": self.category,
            "filter_reason": self.filter_reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Business:
        """Crea un Business desde un diccionario (fila de DB)."""
        status_val = data.get("status", "PENDING")
        return cls(
            id=data.get("id"),
            name=data.get("name", ""),
            phone=data.get("phone"),
            address=data.get("address"),
            website=data.get("website"),
            email=data.get("email"),
            status=BusinessStatus(status_val) if status_val else BusinessStatus.PENDING,
            search_query=data.get("search_query", ""),
            rating=data.get("rating"),
            reviews_count=data.get("reviews_count"),
            category=data.get("category"),
            filter_reason=data.get("filter_reason"),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
        )
