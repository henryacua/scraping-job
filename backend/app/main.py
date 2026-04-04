"""
FastAPI application entry point.

Registra routers, CORS, eventos de startup y health check.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response

from backend.app.api.routes import leads, scraping
from backend.app.core.db import create_db_and_tables
from backend.app.services.strategies import AVAILABLE_STRATEGIES


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_db_and_tables()
    yield


app = FastAPI(
    title="scraping-job-ms API",
    version="0.2.0",
    description="Worker API para Google Maps Lead Scraper",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(scraping.router)
app.include_router(leads.router)


@app.get("/")
async def root():
    """Abrir la raíz en el navegador lleva a Swagger UI."""
    return RedirectResponse(url="/docs")


@app.head("/")
async def root_head():
    """Render y otros balanceadores suelen hacer HEAD /; sin esto devuelven 404."""
    return Response(status_code=200)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "scraping-job-ms",
        "available_strategies": list(AVAILABLE_STRATEGIES.keys()),
    }


@app.head("/health")
async def health_head():
    return Response(status_code=200)
