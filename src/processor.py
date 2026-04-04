"""
Consumer — LeadProcessor

Lee negocios PENDING de la cola, ejecuta un pipeline de acciones (filtros)
y marca los resultados según pasen o no los filtros.
"""
from __future__ import annotations

import asyncio
from typing import Callable, Optional

from src.models import Business, BusinessStatus
from src.queue_manager import QueueManager
from src.strategies import Action
from src.utils import setup_logger
from config import settings

logger = setup_logger(__name__)


class LeadProcessor:
    """
    Consumer del sistema: ejecuta acciones/filtros sobre los datos scrapeados.

    Uso:
        processor = LeadProcessor(queue, actions=[action1, action2])
        await processor.run()
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        actions: list[Action],
        *,
        batch_size: int = settings.BATCH_SIZE,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.queue = queue_manager
        self.actions = actions
        self.batch_size = batch_size
        self._on_progress = on_progress

    def _emit(self, message: str) -> None:
        logger.info(message)
        if self._on_progress:
            self._on_progress(message)

    # ── Flujo principal ─────────────────────────────────

    async def run(self) -> dict[str, int]:
        """
        Procesa todos los negocios PENDING en batches.

        Returns:
            Diccionario con conteos: passed, filtered_out, errors, processed
        """
        counters = {"passed": 0, "filtered_out": 0, "errors": 0, "processed": 0}

        action_names = ", ".join(a.name for a in self.actions)
        self._emit(f"🔄 Iniciando procesamiento con acciones: [{action_names}]")

        while True:
            batch = await self.queue.dequeue(limit=self.batch_size)
            if not batch:
                self._emit("✅ No hay más items PENDING — procesamiento completo")
                break

            self._emit(f"📦 Procesando batch de {len(batch)} negocios...")

            # Procesar en paralelo con semáforo para limitar concurrencia
            semaphore = asyncio.Semaphore(5)
            tasks = [
                self._process_with_semaphore(semaphore, biz, counters)
                for biz in batch
            ]
            await asyncio.gather(*tasks)

        self._emit(
            f"📊 Resumen: {counters['passed']} pasaron, "
            f"{counters['filtered_out']} filtrados, "
            f"{counters['errors']} errores"
        )
        return counters

    async def _process_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        business: Business,
        counters: dict[str, int],
    ) -> None:
        async with semaphore:
            await self._process_business(business, counters)

    # ── Procesamiento individual ────────────────────────

    async def _process_business(
        self, business: Business, counters: dict[str, int]
    ) -> None:
        """Ejecuta todas las acciones sobre un negocio. Lo filtra si alguna falla."""
        try:
            for action in self.actions:
                passed, reason = await action.execute(business)

                if not passed:
                    # Negocio filtrado por esta acción
                    business.filter_reason = f"[{action.name}] {reason}"
                    await self.queue.update_status(
                        business.id, BusinessStatus.FILTERED_OUT  # type: ignore[arg-type]
                    )
                    await self.queue.update_filter_reason(
                        business.id, business.filter_reason  # type: ignore[arg-type]
                    )
                    counters["filtered_out"] += 1
                    self._emit(
                        f"  🔴 FILTRADO: {business.name} — {business.filter_reason}"
                    )
                    counters["processed"] += 1
                    return

            # Pasó todas las acciones
            await self.queue.update_status(
                business.id, BusinessStatus.LEAD_QUALIFIED  # type: ignore[arg-type]
            )
            counters["passed"] += 1
            self._emit(f"  🟢 PASÓ: {business.name}")
            counters["processed"] += 1

        except Exception as e:
            logger.error("Error procesando '%s': %s", business.name, e)
            self._emit(f"  ❌ ERROR: {business.name} — {e}")
            try:
                await self.queue.update_status(
                    business.id, BusinessStatus.ERROR  # type: ignore[arg-type]
                )
            except Exception:
                pass
            counters["errors"] += 1
