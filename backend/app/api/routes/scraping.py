"""
Router de scraping — lanza jobs de busqueda de negocios en background.

Soporta dos fuentes de datos (source):
  - "playwright": scraper con navegador (requiere Playwright en la imagen;
    en IPs de datacenter Google puede bloquear o capar).
  - "places_api": Places API (New) — requiere GOOGLE_MAPS_API_KEY.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import AliasChoices, BaseModel, Field, model_validator

from backend.app.api.deps import verify_api_key

logger = logging.getLogger(__name__)

router = APIRouter(tags=["scraping"])

_jobs: dict[str, dict] = {}

try:
    import playwright  # noqa: F401

    _PLAYWRIGHT_INSTALLED = True
except ImportError:
    _PLAYWRIGHT_INSTALLED = False

PLAYWRIGHT_AVAILABLE = _PLAYWRIGHT_INSTALLED


class ScrapeRequest(BaseModel):
    query: str
    source: Literal["playwright", "places_api"] = "playwright"
    max_scroll_attempts: int = Field(
        default=20,
        ge=1,
        description=(
            "Solo aplica con source=playwright: cuántas veces hacer scroll en el feed lateral "
            "de resultados. Con places_api se ignora (la búsqueda pagina con ~20 ítems por "
            "página vía nextPageToken)."
        ),
        validation_alias=AliasChoices("max_scroll_attempts", "max_scrolls"),
    )
    max_results: int = Field(
        default=60,
        ge=1,
        le=140,
        description=(
            "Cuántos negocios como máximo traer. Playwright: tope 60. Places API: tope 140 "
            "(paginación interna ~20 por llamada)."
        ),
    )
    headless: bool = True

    @model_validator(mode="after")
    def clamp_max_results_by_source(self):
        if self.source == "playwright" and self.max_results > 60:
            return self.model_copy(update={"max_results": 60})
        return self


class JobResponse(BaseModel):
    job_id: str
    status: str


def _check_source_available(source: str) -> None:
    if source == "playwright" and not PLAYWRIGHT_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=(
                "Playwright no esta instalado en esta imagen. Usa source='places_api' "
                "o despliega el worker con Dockerfile.worker (Chromium incluido)."
            ),
        )


async def _run_scrape(job_id: str, req: ScrapeRequest) -> None:
    _jobs[job_id].update({
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    try:
        from backend.app.core.db import engine
        from backend.app.services.producer import create_producer
        from sqlmodel.ext.asyncio.session import AsyncSession

        async with AsyncSession(engine) as session:
            producer = create_producer(
                source=req.source,
                session=session,
                headless=req.headless,
                max_scroll_attempts=req.max_scroll_attempts,
                max_results=req.max_results,
            )
            count = await producer.run(req.query)

        _jobs[job_id].update({
            "status": "completed",
            "businesses_found": count,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("Job %s completado: %d negocios encontrados", job_id, count)
    except Exception as exc:
        _jobs[job_id].update({
            "status": "failed",
            "error": str(exc),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.error("Job %s fallo: %s", job_id, exc, exc_info=True)


@router.post("/scrape", response_model=JobResponse)
async def scrape(
    req: ScrapeRequest,
    _: None = Depends(verify_api_key),
) -> JobResponse:
    _check_source_available(req.source)

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "status": "queued",
        "query": req.query,
        "source": req.source,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }
    asyncio.create_task(_run_scrape(job_id, req))
    logger.info("Job encolado: %s (source=%s, query=%s)", job_id, req.source, req.query)
    return JobResponse(job_id=job_id, status="queued")


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: str,
    _: None = Depends(verify_api_key),
):
    from fastapi import HTTPException

    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' no encontrado")
    return {"job_id": job_id, **job}
