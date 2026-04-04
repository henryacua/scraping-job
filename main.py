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

from config import settings
from src.queue_manager import QueueManager
from src.scraper import GoogleMapsScraper
from src.processor import LeadProcessor
from src.strategies import get_strategy, get_all_strategies, AVAILABLE_STRATEGIES
from src.utils import setup_logger

logger = setup_logger("main")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="🕷️ scraping-job-ms — Google Maps Lead Scraper",
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
        default=settings.SEARCH_QUERY,
        help=f"Término de búsqueda (default: '{settings.SEARCH_QUERY}')",
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
        "--max-scrolls",
        type=int,
        default=settings.MAX_SCROLL_ATTEMPTS,
        help=f"Máximo de scrolls en el feed de Maps (default: {settings.MAX_SCROLL_ATTEMPTS})",
    )
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> None:
    """Pipeline principal async."""
    # Inicializar cola
    queue = QueueManager(settings.DB_PATH)
    await queue.initialize()

    # Construir lista de acciones
    if args.actions is not None:
        actions = [get_strategy(name) for name in args.actions]
    else:
        actions = get_all_strategies()

    action_names = ", ".join(a.name for a in actions)
    logger.info("Acciones seleccionadas: %s", action_names)

    # ── Fase 1: Scraping ──────────────────
    if not args.process_only:
        logger.info("=" * 60)
        logger.info("FASE 1: SCRAPING (Producer)")
        logger.info("=" * 60)

        scraper = GoogleMapsScraper(
            queue,
            headless=not args.no_headless,
            max_scrolls=args.max_scrolls,
        )
        count = await scraper.run(args.query)
        logger.info("Scraping finalizado: %d negocios extraídos", count)

    # ── Fase 2: Procesamiento ─────────────
    if not args.scrape_only:
        logger.info("=" * 60)
        logger.info("FASE 2: PROCESAMIENTO (Consumer)")
        logger.info("=" * 60)

        processor = LeadProcessor(queue, actions)
        results = await processor.run()
        logger.info("Procesamiento finalizado: %s", results)

    # ── Resumen final ─────────────────────
    stats = await queue.get_stats()
    logger.info("=" * 60)
    logger.info("RESUMEN FINAL")
    logger.info("=" * 60)
    for status, count in sorted(stats.items()):
        logger.info("  %-20s: %d", status, count)


def main() -> None:
    args = parse_args()

    if args.scrape_only and args.process_only:
        print("❌ Error: No se puede usar --scrape-only y --process-only juntos.")
        sys.exit(1)

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        logger.info("\n⛔ Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error("Error fatal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
