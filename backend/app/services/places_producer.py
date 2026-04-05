"""
Producer — PlacesApiProducer

Places API (New) vía SDK oficial `google-maps-places`: Text Search + Place Details.
Persistencia vía CRUD. No usa el cliente legacy `googlemaps`.

Requiere GOOGLE_MAPS_API_KEY y el producto Places API (New) habilitado en GCP.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import google.api_core.exceptions
from google.maps import places_v1
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app import crud
from backend.app.core.config import settings
from backend.app.models import Business
from backend.app.services.utils import normalize_url

logger = logging.getLogger(__name__)

# Field masks sin espacios (obligatorios en la API nueva).
_SEARCH_MASK = (
    "places.id,places.name,places.displayName,places.formattedAddress,"
    "places.types,places.rating,places.userRatingCount"
)
_DETAIL_MASK = (
    "displayName,formattedAddress,nationalPhoneNumber,internationalPhoneNumber,"
    "websiteUri,rating,userRatingCount,types,primaryType"
)
_METADATA_SEARCH = [("x-goog-fieldmask", _SEARCH_MASK)]
_METADATA_DETAIL = [("x-goog-fieldmask", _DETAIL_MASK)]

# El proto Python actual limita Text Search a max_result_count <= 20 (sin page_token en respuesta).
_MAX_TEXT_RESULTS = 20


def _localized_text(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    text = getattr(obj, "text", None)
    if text:
        return str(text).strip() or None
    return None


def _place_to_business(place: Any, search_query: str) -> Optional[Business]:
    name = _localized_text(getattr(place, "display_name", None))
    if not name:
        return None

    phone = (
        (getattr(place, "international_phone_number", None) or "").strip()
        or (getattr(place, "national_phone_number", None) or "").strip()
        or None
    )

    types = list(getattr(place, "types", []) or [])
    primary = getattr(place, "primary_type", None) or ""
    category = None
    if primary:
        category = primary.replace("_", " ").title()
    elif types:
        category = types[0].replace("_", " ").title()

    rating = getattr(place, "rating", None)
    rating_str = str(rating) if rating is not None else None

    urc = getattr(place, "user_rating_count", None)
    reviews_str = str(urc) if urc is not None else None

    raw_web = getattr(place, "website_uri", None)
    website = normalize_url(raw_web) if raw_web else None

    return Business(
        name=name,
        phone=phone or None,
        address=(getattr(place, "formatted_address", None) or "").strip() or None,
        website=website,
        email=None,
        search_query=search_query,
        rating=rating_str,
        reviews_count=reviews_str,
        category=category,
    )


class PlacesApiProducer:
    """Producer: Places API (New) → persiste vía CRUD."""

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
        self._api_key = api_key

    def _emit(self, message: str) -> None:
        logger.info(message)
        if self._on_progress:
            self._on_progress(message)

    async def run(self, search_query: str) -> int:
        self._emit(f"Iniciando busqueda via Places API (New) para: '{search_query}'")

        per_query = min(_MAX_TEXT_RESULTS, max(1, self.max_results))
        if self.max_results > _MAX_TEXT_RESULTS:
            self._emit(
                f"Nota: la API de texto devuelve como maximo {_MAX_TEXT_RESULTS} "
                f"resultados por consulta (pedidos: {self.max_results})."
            )

        businesses: list[Business] = []
        try:
            async with places_v1.PlacesAsyncClient(
                client_options={"api_key": self._api_key}
            ) as client:
                search_req = places_v1.SearchTextRequest(
                    text_query=search_query,
                    language_code="es",
                    max_result_count=per_query,
                )
                search_resp = await client.search_text(
                    request=search_req,
                    metadata=_METADATA_SEARCH,
                )
                raw_places = list(search_resp.places)

                if not raw_places:
                    self._emit("No se encontraron resultados")
                    return 0

                self._emit(f"{len(raw_places)} resultados; consultando detalles...")
                total = len(raw_places)

                for idx, summary in enumerate(raw_places):
                    resource = getattr(summary, "name", None) or ""
                    hint = _localized_text(
                        getattr(summary, "display_name", None)
                    ) or resource or "?"

                    if not resource.startswith("places/"):
                        self._emit(
                            f"  [{idx + 1}/{total}] Sin nombre de recurso, omitiendo"
                        )
                        continue

                    try:
                        detail_req = places_v1.GetPlaceRequest(
                            name=resource,
                            language_code="es",
                        )
                        detail = await client.get_place(
                            request=detail_req,
                            metadata=_METADATA_DETAIL,
                        )
                        biz = _place_to_business(detail, search_query)
                        if biz:
                            businesses.append(biz)
                            self._emit(f"  [{idx + 1}/{total}] {biz.name}")
                        else:
                            self._emit(
                                f"  [{idx + 1}/{total}] {hint} — sin datos suficientes"
                            )
                    except google.api_core.exceptions.GoogleAPICallError as exc:
                        logger.warning("get_place %s: %s", resource, exc)
                        self._emit(f"  [{idx + 1}/{total}] {hint} — error: {exc}")
        except google.api_core.exceptions.GoogleAPICallError as exc:
            logger.error("Places API fallo: %s", exc, exc_info=True)
            self._emit(f"Error API Places: {exc}")
            raise

        if businesses:
            count = await crud.enqueue_batch(self.session, businesses)
            self._emit(f"{count} negocios encolados con estado PENDING")
            return count

        self._emit("No se pudieron extraer negocios de los resultados")
        return 0
