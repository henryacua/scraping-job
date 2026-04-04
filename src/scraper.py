"""
Producer — GoogleMapsScraper

Navega Google Maps con Playwright (async), ejecuta scroll infinito
y extrae información de negocios. Los resultados se encolan como PENDING.
"""
from __future__ import annotations

import asyncio
from typing import Callable, Optional

from playwright.async_api import async_playwright, Page, Locator, TimeoutError as PlaywrightTimeout

from src.models import Business
from src.queue_manager import QueueManager
from src.utils import setup_logger, sanitize_text, normalize_url
from config import settings

logger = setup_logger(__name__)

# ── Selectores CSS de Google Maps (pueden cambiar con el tiempo) ──
SEARCH_INPUT = "#searchboxinput"
SEARCH_BUTTON = "#searchbox-searchbutton"
RESULTS_CONTAINER = 'div[role="feed"]'
RESULT_ITEMS = 'div[role="feed"] > div > div > a'

# Selectores de detalle dentro del panel
DETAIL_NAME = ["h1.DUwDvf", "div[role='main'] h1", "h1"]  # Lista de prioridad
DETAIL_WEBSITE = 'a[data-item-id="authority"]'
DETAIL_PHONE = 'button[data-item-id^="phone"]'
DETAIL_ADDRESS = 'button[data-item-id="address"]'
DETAIL_CATEGORY = 'button[jsaction="pane.rating.category"]'
DETAIL_RATING = 'div.fontDisplayLarge'
DETAIL_EMAIL = 'a[href^="mailto:"]'  # Raro en Maps, pero por si acaso


class GoogleMapsScraper:
    """
    Producer del sistema: scrape de Google Maps.

    Uso:
        scraper = GoogleMapsScraper(queue_manager)
        await scraper.run("Dentistas en Medellín")
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        *,
        headless: bool = settings.HEADLESS,
        max_scrolls: int = settings.MAX_SCROLL_ATTEMPTS,
        scroll_pause: float = settings.SCROLL_PAUSE_SECONDS,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.queue = queue_manager
        self.headless = headless
        self.max_scrolls = max_scrolls
        self.scroll_pause = scroll_pause
        self._on_progress = on_progress

    def _emit(self, message: str) -> None:
        """Emite un mensaje de progreso al callback y al logger."""
        logger.info(message)
        if self._on_progress:
            self._on_progress(message)

    # ── Flujo principal ─────────────────────────────────

    async def run(self, search_query: str) -> int:
        """
        Ejecuta el scraping completo.

        Returns:
            Cantidad de negocios encontrados y encolados.
        """
        self._emit(f"🚀 Iniciando scraping para: '{search_query}'")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                locale="es-CO",
                timezone_id="America/Bogota",
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                java_script_enabled=True,
            )
            # Ocultar indicadores de automatización
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)
            page = await context.new_page()

            try:
                # 1. Navegar directamente a la URL de búsqueda de Google Maps
                #    Esto evita el problema del diálogo de cookies bloqueando #searchboxinput
                #    Usamos 'domcontentloaded' porque Maps nunca alcanza 'networkidle'
                search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
                self._emit(f"📍 Navegando a: {search_url}")
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(3000)

                # 2. Manejar el diálogo de consentimiento de cookies (si aparece)
                await self._handle_consent_dialog(page)

                # 3. Esperar a que aparezca el feed de resultados
                self._emit(f"🔍 Esperando resultados para: {search_query}")
                try:
                    await page.wait_for_selector(RESULTS_CONTAINER, timeout=15000)
                except PlaywrightTimeout:
                    self._emit("⚠️ No se encontró el feed de resultados, reintentando búsqueda manual...")
                    await self._search_manual(page, search_query)

                # 4. Scroll infinito
                self._emit("📜 Cargando resultados (scroll infinito)...")
                await self._scroll_results(page)

                # 4. Extraer negocios
                self._emit("📋 Extrayendo información de negocios...")
                businesses = await self._extract_businesses(page, search_query)

                # 5. Encolar
                if businesses:
                    count = await self.queue.enqueue_batch(businesses)
                    self._emit(f"✅ {count} negocios encolados con estado PENDING")
                    return count
                else:
                    self._emit("⚠️ No se encontraron negocios")
                    return 0

            except Exception as e:
                logger.error("Error durante el scraping: %s", e, exc_info=True)
                self._emit(f"❌ Error: {e}")
                raise
            finally:
                await browser.close()

    async def _handle_consent_dialog(self, page: Page) -> None:
        """Cierra el diálogo de consentimiento de cookies de Google si aparece."""
        # Google muestra diferentes variantes del diálogo de consentimiento
        consent_selectors = [
            'button[aria-label="Aceptar todo"]',
            'button:has-text("Aceptar todo")',
            'button:has-text("Accept all")',
            'button:has-text("Rechazar todo")',
            'form[action*="consent"] button',
            '[aria-label="Reject all"]',
        ]

        for selector in consent_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=2000):
                    self._emit("🍪 Cerrando diálogo de consentimiento de cookies...")
                    await btn.click()
                    await page.wait_for_timeout(2000)
                    return
            except Exception:
                continue

    async def _search_manual(self, page: Page, query: str) -> None:
        """Fallback: escribe la búsqueda manualmente en la barra de Google Maps."""
        try:
            search_box = page.locator(SEARCH_INPUT)
            await search_box.wait_for(state="visible", timeout=10000)
            await search_box.click()
            await search_box.fill(query)
            await page.locator(SEARCH_BUTTON).click()
            await page.wait_for_timeout(3000)

            await page.wait_for_selector(RESULTS_CONTAINER, timeout=15000)
        except PlaywrightTimeout:
            self._emit("⚠️ No se encontró el contenedor de resultados — puede que no haya resultados.")

    # ── Scroll infinito ─────────────────────────────────

    async def _scroll_results(self, page: Page) -> None:
        """Hace scroll en el panel de resultados hasta llegar al final o al máximo."""
        feed = page.locator(RESULTS_CONTAINER)

        previous_count = 0
        stale_rounds = 0

        for i in range(self.max_scrolls):
            # Contar items actuales
            items = page.locator(RESULT_ITEMS)
            current_count = await items.count()

            if current_count == previous_count:
                stale_rounds += 1
                if stale_rounds >= 3:
                    self._emit(f"📜 Fin del scroll detectado (ronda {i + 1}, {current_count} items)")
                    break
            else:
                stale_rounds = 0

            previous_count = current_count
            self._emit(f"📜 Scroll {i + 1}/{self.max_scrolls} — {current_count} resultados cargados")

            # Scroll al fondo del feed
            await feed.evaluate("el => el.scrollTop = el.scrollHeight")
            await page.wait_for_timeout(int(self.scroll_pause * 1000))

            # Detectar el mensaje "Has llegado al final"
            end_marker = page.locator("p.fontBodyMedium span:has-text('final')")
            if await end_marker.count() > 0:
                self._emit(f"📜 Llegamos al final de la lista ({current_count} items)")
                break

    # ── Extracción ──────────────────────────────────────

    async def _extract_businesses(
        self, page: Page, search_query: str
    ) -> list[Business]:
        """Extrae datos de cada negocio clickeando en su panel de detalle."""
        items = page.locator(RESULT_ITEMS)
        total = await items.count()
        self._emit(f"🔎 Procesando {total} resultados...")

        businesses: list[Business] = []

        for idx in range(total):
            try:
                item = items.nth(idx)

                # Scroll al elemento para asegurarse de que es visible
                await item.scroll_into_view_if_needed()
                await item.click()
                await page.wait_for_timeout(1500)

                # Extraer datos del panel de detalle
                biz = await self._extract_detail(page, search_query)
                if biz:
                    businesses.append(biz)
                    self._emit(f"  [{idx + 1}/{total}] ✓ {biz.name}")
                else:
                    self._emit(f"  [{idx + 1}/{total}] ⚠️ No se pudo extraer datos")

                # Volver a la lista
                await page.keyboard.press("Escape")

            except Exception as e:
                logger.warning("Error extrayendo item %d: %s", idx, e)
                self._emit(f"  [{idx + 1}/{total}] ⚠️ Error: {e}")
                try:
                    await page.keyboard.press("Escape")
                except Exception:
                    pass
                continue

        return businesses

    async def _extract_detail(self, page: Page, search_query: str) -> Optional[Business]:
        """Extrae los datos del panel de detalle de un negocio."""
        try:
            # Nombre (obligatorio) - Intentar varios selectores para evitar "Resultados"
            name = None
            for sel in DETAIL_NAME:
                try:
                    locator = page.locator(sel).first
                    if await locator.count() > 0:
                        text = await locator.inner_text(timeout=1000)
                        text = sanitize_text(text)
                        if text and text.lower() != "resultados":
                            name = text
                            break
                except Exception:
                    continue
            
            if not name:
                # Último intento: buscar h1 dentro del main role
                try:
                    name = await page.get_by_role("main").locator("h1").first.inner_text(timeout=1000)
                    name = sanitize_text(name)
                except Exception:
                    pass

            if not name or name.lower() == "resultados":
                return None

            # Teléfono
            phone = await self._safe_extract(page, DETAIL_PHONE)

            # Dirección
            address = await self._safe_extract(page, DETAIL_ADDRESS)

            # Sitio web
            website = None
            website_el = page.locator(DETAIL_WEBSITE)
            if await website_el.count() > 0:
                website = await website_el.first.get_attribute("href")
                website = normalize_url(website)

            # Email (si existe mailto:)
            email = None
            email_el = page.locator(DETAIL_EMAIL)
            if await email_el.count() > 0:
                href = await email_el.first.get_attribute("href")
                if href and "mailto:" in href:
                    email = href.replace("mailto:", "").split("?")[0]

            # Rating
            rating = await self._safe_extract(page, DETAIL_RATING)

            # Categoría
            category = await self._safe_extract(page, DETAIL_CATEGORY)

            return Business(
                name=name,  # type: ignore[arg-type]
                phone=sanitize_text(phone),
                address=sanitize_text(address),
                website=website,
                email=email,
                search_query=search_query,
                rating=sanitize_text(rating),
                category=sanitize_text(category),
            )

        except PlaywrightTimeout:
            return None
        except Exception as e:
            logger.warning("Error extrayendo detalle: %s", e)
            return None

    @staticmethod
    async def _safe_extract(page: Page, selector: str) -> Optional[str]:
        """Intenta extraer texto de un selector, retorna None si no existe."""
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                text = await locator.inner_text(timeout=2000)
                return text.strip() if text else None
        except Exception:
            pass
        return None
