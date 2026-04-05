"""
Producer — PlacesApiProducer

Places API (New): Text Search vía **REST** (paginación `nextPageToken`) y
Place Details vía SDK `google-maps-places` (`get_place`).

Requiere GOOGLE_MAPS_API_KEY y Places API (New) habilitada en GCP.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

import aiohttp
import google.api_core.exceptions
from google.maps import places_v1
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app import crud
from backend.app.core.config import settings
from backend.app.models import Business
from backend.app.services.utils import normalize_url

logger = logging.getLogger(__name__)

_SEARCH_TEXT_URL = "https://places.googleapis.com/v1/places:searchText"
# Field mask REST = mismo formato que gRPC (sin espacios).
_SEARCH_MASK = (
    "places.id,places.name,places.displayName,places.formattedAddress,"
    "places.types,places.rating,places.userRatingCount"
)
_DETAIL_MASK = (
    "displayName,formattedAddress,nationalPhoneNumber,internationalPhoneNumber,"
    "websiteUri,rating,userRatingCount,types,primaryType"
)
_METADATA_DETAIL = [("x-goog-fieldmask", _DETAIL_MASK)]

# Hasta N lugares en total (varias páginas de 20). Límite para coste/latencia.
_MAX_TOTAL_RESULTS_CAP = 140
_PAGE_SIZE = 20
# Google: esperar antes de usar nextPageToken (evita INVALID_ARGUMENT).
_PAGE_TOKEN_DELAY_SEC = 2.0


def _localized_text(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    text = getattr(obj, "text", None)
    if text:
        return str(text).strip() or None
    return None


def _json_display_hint(place: dict[str, Any]) -> str:
    dn = place.get("displayName")
    if isinstance(dn, dict):
        t = dn.get("text")
        if t:
            return str(t)
    return place.get("name") or "?"


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


async def _search_text_rest_pages(
    *,
    api_key: str,
    text_query: str,
    max_results: int,
    http: aiohttp.ClientSession,
) -> list[dict[str, Any]]:
    """Varias páginas de Text Search (REST); como mucho 20 resultados por página."""
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": _SEARCH_MASK,
    }
    cap = max(1, min(max_results, _MAX_TOTAL_RESULTS_CAP))
    collected: list[dict[str, Any]] = []
    page_token: str | None = None
    page_num = 0

    while len(collected) < cap:
        page_size = min(_PAGE_SIZE, cap - len(collected))
        body: dict[str, Any] = {
            "textQuery": text_query,
            "languageCode": "es",
            "pageSize": page_size,
        }
        if page_token:
            body["pageToken"] = page_token

        async with http.post(_SEARCH_TEXT_URL, headers=headers, json=body) as resp:
            if resp.status != 200:
                raw = await resp.text()
                logger.error("searchText HTTP %s: %s", resp.status, raw[:800])
                raise google.api_core.exceptions.GoogleAPICallError(
                    f"Places searchText HTTP {resp.status}: {raw[:200]}"
                )
            data = await resp.json()

        places = data.get("places") or []
        page_num += 1
        for p in places:
            if len(collected) >= cap:
                break
            if isinstance(p, dict):
                collected.append(p)

        page_token = data.get("nextPageToken") or data.get("next_page_token")
        if not page_token or not places:
            break
        if len(collected) >= cap:
            break
        await asyncio.sleep(_PAGE_TOKEN_DELAY_SEC)

    logger.info(
        "Places searchText: %d lugares en %d pagina(s) (tope pedido %d)",
        len(collected),
        page_num,
        cap,
    )
    return collected


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

        want = max(1, min(self.max_results, _MAX_TOTAL_RESULTS_CAP))
        if self.max_results > _MAX_TOTAL_RESULTS_CAP:
            self._emit(
                f"Nota: se limita a {_MAX_TOTAL_RESULTS_CAP} resultados por busqueda "
                f"(pedidos: {self.max_results})."
            )

        # Varias páginas (p. ej. 140÷20) + pausa 2s entre tokens → margen amplio.
        _pages = max(1, (want + _PAGE_SIZE - 1) // _PAGE_SIZE)
        _search_budget = 45.0 * _pages + _PAGE_TOKEN_DELAY_SEC * max(0, _pages - 1)
        timeout = aiohttp.ClientTimeout(
            total=min(900.0, max(120.0, _search_budget)),
            connect=float(settings.HTTP_TIMEOUT),
        )

        businesses: list[Business] = []
        try:
            async with aiohttp.ClientSession(timeout=timeout) as http:
                raw_places = await _search_text_rest_pages(
                    api_key=self._api_key,
                    text_query=search_query,
                    max_results=want,
                    http=http,
                )

            if not raw_places:
                self._emit("No se encontraron resultados")
                return 0

            self._emit(
                f"{len(raw_places)} resultado(s) en lista; consultando detalles (1 llamada por sitio)..."
            )
            total = len(raw_places)

            async with places_v1.PlacesAsyncClient(
                client_options={"api_key": self._api_key}
            ) as client:
                for idx, summary in enumerate(raw_places):
                    resource = summary.get("name") or ""
                    hint = _json_display_hint(summary)

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
