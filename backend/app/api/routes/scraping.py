"""
Router de scraping — lanza jobs de Google Maps scraping en background.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header
from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app.api.deps import verify_api_key
from backend.app.core.db import get_session
from backend.app.services.scraper import GoogleMapsScraper

logger = logging.getLogger(__name__)

router = APIRouter(tags=["scraping"])

_jobs: dict[str, dict] = {}


class ScrapeRequest(BaseModel):
    query: str
    max_scrolls: int = 20
    headless: bool = True


class JobResponse(BaseModel):
    job_id: str
    status: str


async def _run_scrape(job_id: str, req: ScrapeRequest) -> None:
    _jobs[job_id].update({
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    try:
        from backend.app.core.db import engine
        from sqlmodel.ext.asyncio.session import AsyncSession

        async with AsyncSession(engine) as session:
            scraper = GoogleMapsScraper(
                session,
                headless=req.headless,
                max_scrolls=req.max_scrolls,
            )
            count = await scraper.run(req.query)

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
):
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "status": "queued",
        "query": req.query,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }
    asyncio.create_task(_run_scrape(job_id, req))
    logger.info("Job de scraping encolado: %s (query=%s)", job_id, req.query)
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
