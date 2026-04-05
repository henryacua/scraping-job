"""
Producer — PlacesApiProducer

Places API (New): Text Search vía **REST** (paginación `nextPageToken`) y
Place Details vía SDK `google-maps-places` (`get_place`).

Requiere GOOGLE_MAPS_API_KEY y Places API (New) habilitada en GCP.
"""
from __future__ import annotations

import asyncio
import json
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
# nextPageToken debe ir en la máscara o la API no lo devuelve y la paginación se queda en 20.
# Ver: https://developers.google.com/maps/documentation/places/web-service/text-search
_SEARCH_MASK = (
    "places.id,places.name,places.displayName,places.formattedAddress,"
    "places.types,places.rating,places.userRatingCount,nextPageToken"
)
_DETAIL_MASK = (
    "displayName,formattedAddress,nationalPhoneNumber,internationalPhoneNumber,"
    "websiteUri,rating,userRatingCount,types,primaryType"
)
_METADATA_DETAIL = [("x-goog-fieldmask", _DETAIL_MASK)]

# Tope por corrida en esta app (coste/latencia). Text Search pagina de a 20.
_MAX_TOTAL_RESULTS_CAP = 140
_PAGE_SIZE = 20
# Google: esperar antes de usar nextPageToken (evita INVALID_ARGUMENT).
_PAGE_TOKEN_DELAY_SEC = 2.0
# Por cadena de paginación (misma sesión de búsqueda) Google suele devolver pocas páginas;
# la documentación de uso cita a menudo ~60 resultados totales. Los lotes (token nuevo) pueden sumar más.
TEXT_SEARCH_TYPICAL_MAX_PER_CHAIN = 60
# Reintentos si la petición con pageToken falla (token a veces no válido al instante).
_PAGE_TOKEN_MAX_ATTEMPTS = 3
_PAGE_TOKEN_RETRY_BASE_SEC = 2.0


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


async def _post_search_text(
    http: aiohttp.ClientSession,
    headers: dict[str, str],
    body: dict[str, Any],
    *,
    uses_page_token: bool,
) -> dict[str, Any]:
    """POST searchText; reintenta si falla una petición paginada (token aún no listo)."""
    attempts = _PAGE_TOKEN_MAX_ATTEMPTS if uses_page_token else 1
    last_status: int | None = None
    last_snippet = ""
    for attempt in range(attempts):
        if attempt:
            delay = _PAGE_TOKEN_RETRY_BASE_SEC * attempt
            logger.warning(
                "searchText reintento %d/%d tras esperar %.1fs (pageToken=%s)",
                attempt + 1,
                attempts,
                delay,
                uses_page_token,
            )
            await asyncio.sleep(delay)
        async with http.post(_SEARCH_TEXT_URL, headers=headers, json=body) as resp:
            raw = await resp.text()
            last_status = resp.status
            last_snippet = raw[:800]
            if resp.status == 200:
                return json.loads(raw)
        if uses_page_token and attempt < attempts - 1 and last_status in (
            400,
            429,
            500,
            503,
        ):
            continue
        break
    logger.error("searchText HTTP %s: %s", last_status, last_snippet[:800])
    raise google.api_core.exceptions.GoogleAPICallError(
        f"Places searchText HTTP {last_status}: {last_snippet[:200]}"
    )


async def _search_text_rest_pages(
    *,
    api_key: str,
    text_query: str,
    max_results: int,
    http: aiohttp.ClientSession,
    start_page_token: str | None = None,
    on_progress: Optional[Callable[[str], None]] = None,
) -> tuple[list[dict[str, Any]], str | None, int]:
    """Varias páginas de Text Search (REST); como mucho 20 resultados por llamada.

    Devuelve ``(lugares, token_siguiente, num_paginas)``. ``pageSize`` por petición
    es como mucho 20 (límite de la API). Tras cada página (salvo la última) se espera
    antes de usar ``nextPageToken``.
    """
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": _SEARCH_MASK,
    }
    cap = max(1, min(max_results, _MAX_TOTAL_RESULTS_CAP))
    collected: list[dict[str, Any]] = []
    page_token: str | None = start_page_token
    page_num = 0
    continuation: str | None = None

    def _page_msg(pn: int, n_pl: int, has_next: bool) -> str:
        return (
            f"Text Search página {pn}: {n_pl} lugar(es) en esta respuesta; "
            f"nextPageToken={'sí' if has_next else 'no'}"
        )

    while len(collected) < cap:
        # Máx. 20 por llamada (API); en la última página puede pedirse menos.
        page_size = min(_PAGE_SIZE, cap - len(collected))
        body: dict[str, Any] = {
            "textQuery": text_query,
            "languageCode": "es",
            "pageSize": page_size,
        }
        if page_token:
            body["pageToken"] = page_token

        uses_token = bool(page_token)
        data = await _post_search_text(http, headers, body, uses_page_token=uses_token)

        places = data.get("places") or []
        page_num += 1
        next_tok = data.get("nextPageToken") or data.get("next_page_token")
        has_next = bool(next_tok)
        line = _page_msg(page_num, len(places), has_next)
        logger.info("Places %s (acumulado %d / objetivo %d)", line, len(collected), cap)
        if on_progress:
            on_progress(line)

        for p in places:
            if len(collected) >= cap:
                break
            if isinstance(p, dict):
                collected.append(p)

        if len(collected) >= cap:
            continuation = next_tok if next_tok else None
            break
        if not places:
            break
        if not next_tok:
            break
        page_token = next_tok
        await asyncio.sleep(_PAGE_TOKEN_DELAY_SEC)

    if len(collected) < cap:
        continuation = None

    logger.info(
        "Places searchText fin: %d lugares en %d pagina(s) (objetivo %d); "
        "hay_token_continuacion=%s",
        len(collected),
        page_num,
        cap,
        continuation is not None,
    )
    return collected, continuation, page_num


class PlacesApiProducer:
    """Producer: Places API (New) → persiste vía CRUD."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        max_results: int = 60,
        places_page_token: str | None = None,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.session = session
        self.max_results = max_results
        self._places_page_token = (places_page_token or "").strip() or None
        self._on_progress = on_progress
        self.last_places_next_page_token: str | None = None

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
        self.last_places_next_page_token = None
        if self._places_page_token:
            self._emit(
                f"Continuando busqueda Places (misma query) con pageToken "
                f"para el siguiente lote..."
            )
        else:
            self._emit(f"Iniciando busqueda via Places API (New) para: '{search_query}'")

        want = max(1, min(self.max_results, _MAX_TOTAL_RESULTS_CAP))
        if self.max_results > _MAX_TOTAL_RESULTS_CAP:
            self._emit(
                f"Nota: se limita a {_MAX_TOTAL_RESULTS_CAP} resultados por busqueda "
                f"(pedidos: {self.max_results})."
            )
        if want > TEXT_SEARCH_TYPICAL_MAX_PER_CHAIN and not self._places_page_token:
            self._emit(
                f"Aviso: Google suele devolver como mucho ~{TEXT_SEARCH_TYPICAL_MAX_PER_CHAIN} "
                "resultados por cadena de Text Search; si te quedas corto, usa otro **lote** "
                "(misma búsqueda) para seguir con el token guardado."
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
                raw_places, next_search_token, ts_pages = await _search_text_rest_pages(
                    api_key=self._api_key,
                    text_query=search_query,
                    max_results=want,
                    http=http,
                    start_page_token=self._places_page_token,
                    on_progress=self._emit,
                )

            self.last_places_next_page_token = next_search_token
            if next_search_token:
                self._emit(
                    "Hay mas resultados en Google para esta query: en la siguiente corrida "
                    "usa el mismo texto de busqueda y el token devuelto por la API/dashboard."
                )

            if not raw_places:
                self._emit("No se encontraron resultados")
                return 0

            self._emit(
                f"Text Search resumen: {len(raw_places)} sitio(s) en {ts_pages} página(s) "
                f"de búsqueda (objetivo hasta {want}; cada página API ≤{_PAGE_SIZE})."
            )
            if (
                want > _PAGE_SIZE
                and ts_pages == 1
                and len(raw_places) < want
                and not next_search_token
            ):
                self._emit(
                    "Google no devolvió más páginas (sin nextPageToken): no hay más resultados "
                    "en esta cadena o la query devolvió pocos sitios. No es un fallo del "
                    "slider de máximo."
                )

            self._emit(
                f"Consultando Place Details (1 llamada por sitio), {len(raw_places)} en cola..."
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
            raw_n = len(raw_places)
            self._emit(
                f"Resumen final: Text Search {raw_n} sitio(s); Place Details → "
                f"{count} negocio(s) encolado(s) PENDING"
                + (
                    f" ({raw_n - count} omitidos o error en detalle)"
                    if count < raw_n
                    else ""
                )
            )
            return count

        self._emit(
            "No se pudieron extraer negocios de los resultados "
            "(revisa errores de Place Details arriba)."
        )
        return 0
