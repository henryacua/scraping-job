"""
Producer — PlacesApiProducer

Busca negocios en Google Maps mediante la Places API (Text Search + Place Details)
y los persiste vía CRUD.  Alternativa al scraper Playwright que funciona desde
cualquier entorno (datacenter incluido) sin navegador.

Requiere GOOGLE_MAPS_API_KEY en las variables de entorno.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

import googlemaps
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app import crud
from backend.app.core.config import settings
from backend.app.models import Business

logger = logging.getLogger(__name__)

TEXT_SEARCH_FIELDS = [
    "name",
    "formatted_address",
    "place_id",
    "rating",
    "user_ratings_total",
    "types",
]

DETAIL_FIELDS = [
    "name",
    "formatted_address",
    "formatted_phone_number",
    "international_phone_number",
    "website",
    "rating",
    "user_ratings_total",
    "types",
]


class PlacesApiProducer:
    """Producer: Google Maps Places API → persiste vía CRUD."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        max_results: int = 60,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.session = session
        self.max_results = max_results
        self._on_progress = on_progress

        api_key = settings.GOOGLE_MAPS_API_KEY
        if not api_key:
            raise ValueError(
                "GOOGLE_MAPS_API_KEY no configurada. "
                "Agrégala en .env o en las variables de entorno."
            )
        self._client = googlemaps.Client(key=api_key)

    def _emit(self, message: str) -> None:
        logger.info(message)
        if self._on_progress:
            self._on_progress(message)

    async def run(self, search_query: str) -> int:
        self._emit(f"Iniciando busqueda via Places API para: '{search_query}'")

        raw_places = await self._text_search(search_query)
        if not raw_places:
            self._emit("No se encontraron resultados en Places API")
            return 0

        self._emit(f"{len(raw_places)} resultados obtenidos, consultando detalles...")
        businesses = await self._fetch_details(raw_places, search_query)

        if businesses:
            count = await crud.enqueue_batch(self.session, businesses)
            self._emit(f"{count} negocios encolados con estado PENDING")
            return count

        self._emit("No se pudieron extraer negocios de los resultados")
        return 0

    async def _text_search(self, query: str) -> list[dict]:
        """Ejecuta Text Search con paginación hasta max_results."""
        all_results: list[dict] = []
        page_token: Optional[str] = None
        page = 0

        while len(all_results) < self.max_results:
            page += 1
            self._emit(f"Text Search página {page}...")

            response = await asyncio.to_thread(
                self._client.places,
                query=query,
                language="es",
                page_token=page_token,
            )

            results = response.get("results", [])
            if not results:
                break

            all_results.extend(results)
            self._emit(f"  Página {page}: {len(results)} resultados (total: {len(all_results)})")

            page_token = response.get("next_page_token")
            if not page_token:
                break

            # Google requiere ~2 s antes de poder usar el next_page_token
            await asyncio.sleep(2.0)

        trimmed = all_results[: self.max_results]
        self._emit(f"Text Search completado: {len(trimmed)} resultados")
        return trimmed

    async def _fetch_details(
        self, places: list[dict], search_query: str
    ) -> list[Business]:
        """Consulta Place Details para cada resultado y mapea a Business."""
        businesses: list[Business] = []
        total = len(places)

        for idx, place in enumerate(places):
            place_id = place.get("place_id")
            name_hint = place.get("name", "?")

            if not place_id:
                self._emit(f"  [{idx + 1}/{total}] Sin place_id, omitiendo")
                continue

            try:
                detail = await asyncio.to_thread(
                    self._client.place,
                    place_id=place_id,
                    fields=DETAIL_FIELDS,
                    language="es",
                )
                result = detail.get("result", {})
                biz = self._map_to_business(result, search_query)
                if biz:
                    businesses.append(biz)
                    self._emit(f"  [{idx + 1}/{total}] {biz.name}")
                else:
                    self._emit(f"  [{idx + 1}/{total}] {name_hint} — sin datos suficientes")
            except Exception as exc:
                logger.warning("Error obteniendo detalle de %s: %s", place_id, exc)
                self._emit(f"  [{idx + 1}/{total}] {name_hint} — error: {exc}")

        return businesses

    @staticmethod
    def _map_to_business(result: dict, search_query: str) -> Optional[Business]:
        """Convierte la respuesta de Place Details a un modelo Business."""
        name = result.get("name")
        if not name:
            return None

        phone = (
            result.get("international_phone_number")
            or result.get("formatted_phone_number")
        )

        types = result.get("types", [])
        category = types[0].replace("_", " ").title() if types else None

        rating_val = result.get("rating")
        rating_str = str(rating_val) if rating_val is not None else None

        reviews_count = result.get("user_ratings_total")
        reviews_str = str(reviews_count) if reviews_count is not None else None

        return Business(
            name=name,
            phone=phone,
            address=result.get("formatted_address"),
            website=result.get("website"),
            email=None,  # Places API no expone emails
            search_query=search_query,
            rating=rating_str,
            reviews_count=reviews_str,
            category=category,
        )
