"""
Protocol y factory para producers de negocios (Google Maps).

Define la interfaz comun que deben cumplir todos los producers
y un factory que instancia la implementacion correcta segun configuracion.
"""
from __future__ import annotations

from typing import Callable, Literal, Optional, Protocol, runtime_checkable

from sqlmodel.ext.asyncio.session import AsyncSession

MapsSource = Literal["playwright", "places_api"]


@runtime_checkable
class MapsProducer(Protocol):
    """Contrato que deben cumplir todos los producers de negocios."""

    async def run(self, search_query: str) -> int:
        """Busca negocios y los persiste via CRUD. Retorna cantidad encolada."""
        ...


def create_producer(
    source: MapsSource,
    session: AsyncSession,
    *,
    on_progress: Optional[Callable[[str], None]] = None,
    max_results: int = 60,
    headless: bool = True,
    max_scroll_attempts: int = 20,
    scroll_pause: float = 2.0,
    click_delay_ms: int = 2000,
) -> MapsProducer:
    """Instancia el producer adecuado segun `source`."""

    if source == "playwright":
        from backend.app.services.scraper import GoogleMapsScraper

        return GoogleMapsScraper(
            session,
            headless=headless,
            max_results=max_results,
            max_scroll_attempts=max_scroll_attempts,
            scroll_pause=scroll_pause,
            click_delay_ms=click_delay_ms,
            on_progress=on_progress,
        )

    if source == "places_api":
        from backend.app.services.places_producer import PlacesApiProducer

        return PlacesApiProducer(
            session,
            max_results=max_results,
            on_progress=on_progress,
        )

    raise ValueError(f"Source desconocido: {source!r}. Usa 'playwright' o 'places_api'.")
