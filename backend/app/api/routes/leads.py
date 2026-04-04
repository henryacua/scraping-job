"""
Router de leads — procesamiento de negocios pendientes.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app.api.deps import verify_api_key
from backend.app.services.strategies import get_all_strategies

logger = logging.getLogger(__name__)

router = APIRouter(tags=["leads"])

_jobs: dict[str, dict] = {}


class ProcessRequest(BaseModel):
    batch_size: int = 10


class JobResponse(BaseModel):
    job_id: str
    status: str


async def _run_process(job_id: str, req: ProcessRequest) -> None:
    _jobs[job_id].update({
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    try:
        from backend.app.core.db import engine
        from backend.app.services.processor import LeadProcessor

        async with AsyncSession(engine) as session:
            actions = get_all_strategies()
            processor = LeadProcessor(session, actions, batch_size=req.batch_size)
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
        logger.error("Job %s fallo: %s", job_id, exc, exc_info=True)


@router.post("/process", response_model=JobResponse)
async def process(
    req: ProcessRequest,
    _: None = Depends(verify_api_key),
):
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "status": "queued",
        "batch_size": req.batch_size,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }
    asyncio.create_task(_run_process(job_id, req))
    logger.info("Job de procesamiento encolado: %s", job_id)
    return JobResponse(job_id=job_id, status="queued")


@router.get("/process/jobs/{job_id}")
async def get_process_job(
    job_id: str,
    _: None = Depends(verify_api_key),
):
    from fastapi import HTTPException

    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' no encontrado")
    return {"job_id": job_id, **job}
