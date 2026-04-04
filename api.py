"""
api.py — FastAPI entry point para Render.

Expone endpoints HTTP que el dashboard (Streamlit Cloud) consume
para lanzar jobs de scraping y procesamiento de forma remota.

Endpoints:
    GET  /health          — health check
    POST /scrape          — lanzar job de scraping en background
    POST /process         — procesar leads pendientes en background
    GET  /jobs/{job_id}   — consultar estado de un job
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import settings
from src.queue_manager import QueueManager
from src.scraper import GoogleMapsScraper
from src.processor import LeadProcessor
from src.strategies import get_strategy, AVAILABLE_STRATEGIES
from src.utils import setup_logger

logger = setup_logger("api")

app = FastAPI(
    title="scraping-job-ms API",
    version="0.1.0",
    description="Worker API para Google Maps Lead Scraper",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── In-memory job tracker ────────────────────────────────
# Suficiente para setup de un solo worker / proceso.
_jobs: dict[str, dict] = {}


# ── Auth ─────────────────────────────────────────────────

def _verify_api_key(x_api_key: Optional[str]) -> None:
    """Valida la API key si está configurada. Sin clave configurada, acepta todo."""
    expected = settings.API_KEY
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")


# ── Request / Response schemas ───────────────────────────

class ScrapeRequest(BaseModel):
    query: str
    max_scrolls: int = 20
    headless: bool = True


class ProcessRequest(BaseModel):
    strategy: str = "SaveToCSV"


class JobResponse(BaseModel):
    job_id: str
    status: str


# ── Background tasks ─────────────────────────────────────

async def _run_scrape(job_id: str, req: ScrapeRequest) -> None:
    _jobs[job_id].update({"status": "running", "started_at": datetime.now(timezone.utc).isoformat()})
    try:
        queue = QueueManager(settings.DATABASE_URL)
        await queue.initialize()
        scraper = GoogleMapsScraper(
            queue,
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
        logger.error("Job %s falló: %s", job_id, exc, exc_info=True)


async def _run_process(job_id: str, req: ProcessRequest) -> None:
    _jobs[job_id].update({"status": "running", "started_at": datetime.now(timezone.utc).isoformat()})
    try:
        queue = QueueManager(settings.DATABASE_URL)
        await queue.initialize()
        strategy = get_strategy(req.strategy)
        processor = LeadProcessor(queue, strategy)
        results = await processor.run()
        _jobs[job_id].update({
            "status": "completed",
            "results": results,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("Job %s completado: %s", job_id, results)
    except Exception as exc:
        _jobs[job_id].update({
            "status": "failed",
            "error": str(exc),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.error("Job %s falló: %s", job_id, exc, exc_info=True)


# ── Endpoints ────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check para Render y UptimeRobot."""
    return {
        "status": "ok",
        "service": "scraping-job-ms",
        "available_strategies": list(AVAILABLE_STRATEGIES.keys()),
    }


@app.post("/scrape", response_model=JobResponse)
async def scrape(
    req: ScrapeRequest,
    x_api_key: Optional[str] = Header(default=None),
):
    """Lanza un job de scraping en background. Retorna el job_id para polling."""
    _verify_api_key(x_api_key)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "queued", "query": req.query, "queued_at": datetime.now(timezone.utc).isoformat()}
    asyncio.create_task(_run_scrape(job_id, req))
    logger.info("Job de scraping encolado: %s (query=%s)", job_id, req.query)
    return JobResponse(job_id=job_id, status="queued")


@app.post("/process", response_model=JobResponse)
async def process(
    req: ProcessRequest,
    x_api_key: Optional[str] = Header(default=None),
):
    """Lanza un job de procesamiento de leads pendientes en background."""
    _verify_api_key(x_api_key)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "queued", "strategy": req.strategy, "queued_at": datetime.now(timezone.utc).isoformat()}
    asyncio.create_task(_run_process(job_id, req))
    logger.info("Job de procesamiento encolado: %s (strategy=%s)", job_id, req.strategy)
    return JobResponse(job_id=job_id, status="queued")


@app.get("/jobs/{job_id}")
async def get_job(
    job_id: str,
    x_api_key: Optional[str] = Header(default=None),
):
    """Consulta el estado de un job por su ID."""
    _verify_api_key(x_api_key)
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' no encontrado")
    return {"job_id": job_id, **job}
