#!/usr/bin/env python3
"""
main.py — Orquestador CLI del microservicio scraping-job-ms.

Ejecuta el pipeline Producer-Consumer desde línea de comandos.

Uso:
    python main.py --query "Dentistas en Medellín"
    python main.py --scrape-only --query "Abogados en Bogotá"
    python main.py --process-only
    python main.py --actions FilterInvalidPhone FilterNoWhatsApp
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from backend.app.core.config import settings
from backend.app.core.db import create_db_and_tables, engine
from backend.app import crud
from backend.app.services.producer import create_producer
from backend.app.services.processor import LeadProcessor
from backend.app.services.strategies import (
    AVAILABLE_STRATEGIES,
    get_all_strategies,
    get_strategy,
)
from backend.app.services.utils import setup_logger

from sqlmodel.ext.asyncio.session import AsyncSession

logger = setup_logger("main")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="scraping-job-ms — Google Maps Lead Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python main.py --query "Dentistas en Medellín"
  python main.py --scrape-only --query "Abogados en Bogotá"
  python main.py --process-only
  python main.py --actions FilterInvalidPhone FilterNoWhatsApp
  python main.py --query "Restaurantes en Cali" --no-headless
        """,
    )
    parser.add_argument(
        "--query", "-q",
        type=str,
        default="Dentistas en Medellín",
        help="Término de búsqueda (default: 'Dentistas en Medellín')",
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Solo ejecutar el Producer (scraping), sin procesar",
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="Solo ejecutar el Consumer (procesamiento de leads pendientes)",
    )
    parser.add_argument(
        "--actions", "-a",
        nargs="*",
        default=None,
        help=(
            f"Acciones a ejecutar (default: todas). "
            f"Disponibles: {', '.join(AVAILABLE_STRATEGIES.keys())}"
        ),
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Ejecutar el browser en modo visible (no headless)",
    )
    parser.add_argument(
        "--max-scroll-attempts",
        "--max-scrolls",
        type=int,
        default=settings.MAX_SCROLL_ATTEMPTS,
        dest="max_scroll_attempts",
        help=(
            "Solo Playwright: intentos de scroll en el feed lateral de Maps "
            f"(default: {settings.MAX_SCROLL_ATTEMPTS}). Con --source places_api no aplica."
        ),
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["playwright", "places_api"],
        default=settings.MAPS_SOURCE,
        help=f"Fuente de datos: playwright (scraper) o places_api (Google API) (default: {settings.MAPS_SOURCE})",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=60,
        help=(
            "Máximo de resultados: Playwright tope 60; Places API tope 140 (default: 60)"
        ),
    )
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> None:
    await create_db_and_tables()

    if args.actions is not None:
        actions = [get_strategy(name) for name in args.actions]
    else:
        actions = get_all_strategies()

    action_names = ", ".join(a.name for a in actions)
    logger.info("Acciones seleccionadas: %s", action_names)

    async with AsyncSession(engine) as session:
        if not args.process_only:
            logger.info("=" * 60)
            logger.info("FASE 1: BUSQUEDA (Producer — %s)", args.source)
            logger.info("=" * 60)

            producer = create_producer(
                source=args.source,
                session=session,
                headless=not args.no_headless,
                max_scroll_attempts=args.max_scroll_attempts,
                max_results=args.max_results,
            )
            count = await producer.run(args.query)
            logger.info("Busqueda finalizada: %d negocios extraidos", count)

        if not args.scrape_only:
            logger.info("=" * 60)
            logger.info("FASE 2: PROCESAMIENTO (Consumer)")
            logger.info("=" * 60)

            processor = LeadProcessor(session, actions)
            results = await processor.run()
            logger.info("Procesamiento finalizado: %s", results)

        stats = await crud.get_stats(session)
        logger.info("=" * 60)
        logger.info("RESUMEN FINAL")
        logger.info("=" * 60)
        for status, count in sorted(stats.items()):
            logger.info("  %-20s: %s", status, count)


def main() -> None:
    args = parse_args()

    if args.scrape_only and args.process_only:
        print("Error: No se puede usar --scrape-only y --process-only juntos.")
        sys.exit(1)

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error("Error fatal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
