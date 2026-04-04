"""
Consumer — LeadProcessor

Lee negocios PENDING de la cola, ejecuta un pipeline de acciones (filtros)
y marca los resultados según pasen o no. Usa CRUD async + AsyncSession.
"""
from __future__ import annotations

import asyncio
from typing import Callable, Optional

from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app import crud
from backend.app.core.config import settings
from backend.app.models import Business, BusinessStatus
from backend.app.services.strategies import Action
from backend.app.services.utils import setup_logger

logger = setup_logger(__name__)


class LeadProcessor:
    """Consumer: ejecuta acciones/filtros sobre los datos scrapeados."""

    def __init__(
        self,
        session: AsyncSession,
        actions: list[Action],
        *,
        batch_size: int = settings.BATCH_SIZE,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.session = session
        self.actions = actions
        self.batch_size = batch_size
        self._on_progress = on_progress

    def _emit(self, message: str) -> None:
        logger.info(message)
        if self._on_progress:
            self._on_progress(message)

    async def run(self) -> dict[str, int]:
        counters = {"passed": 0, "filtered_out": 0, "errors": 0, "processed": 0}

        action_names = ", ".join(a.name for a in self.actions)
        self._emit(f"Iniciando procesamiento con acciones: [{action_names}]")

        while True:
            batch = await crud.dequeue(self.session, limit=self.batch_size)
            if not batch:
                self._emit("No hay mas items PENDING — procesamiento completo")
                break

            self._emit(f"Procesando batch de {len(batch)} negocios...")

            semaphore = asyncio.Semaphore(5)
            tasks = [
                self._process_with_semaphore(semaphore, biz, counters)
                for biz in batch
            ]
            await asyncio.gather(*tasks)

        self._emit(
            f"Resumen: {counters['passed']} pasaron, "
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

    async def _process_business(
        self, business: Business, counters: dict[str, int]
    ) -> None:
        try:
            for action in self.actions:
                passed, reason = await action.execute(business)

                if not passed:
                    business.filter_reason = f"[{action.name}] {reason}"
                    await crud.update_status(
                        self.session, business.id, BusinessStatus.FILTERED_OUT  # type: ignore[arg-type]
                    )
                    await crud.update_filter_reason(
                        self.session, business.id, business.filter_reason  # type: ignore[arg-type]
                    )
                    counters["filtered_out"] += 1
                    self._emit(f"  FILTRADO: {business.name} — {business.filter_reason}")
                    counters["processed"] += 1
                    return

            await crud.update_status(
                self.session, business.id, BusinessStatus.LEAD_QUALIFIED  # type: ignore[arg-type]
            )
            counters["passed"] += 1
            self._emit(f"  PASO: {business.name}")
            counters["processed"] += 1

        except Exception as e:
            logger.error("Error procesando '%s': %s", business.name, e)
            self._emit(f"  ERROR: {business.name} — {e}")
            try:
                await crud.update_status(
                    self.session, business.id, BusinessStatus.ERROR  # type: ignore[arg-type]
                )
            except Exception:
                pass
            counters["errors"] += 1
