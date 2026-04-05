"""
Producer — GoogleMapsScraper

Navega Google Maps con Playwright (async), ejecuta scroll infinito
y extrae información de negocios. Los resultados se persisten vía CRUD.
"""
from __future__ import annotations

from typing import Callable, Optional

from playwright.async_api import (
    Page,
    TimeoutError as PlaywrightTimeout,
    async_playwright,
)
from sqlmodel.ext.asyncio.session import AsyncSession

from backend.app import crud
from backend.app.core.config import settings
from backend.app.models import Business
from backend.app.services.utils import normalize_url, sanitize_text, setup_logger

logger = setup_logger(__name__)

SEARCH_INPUT = "#searchboxinput"
SEARCH_BUTTON = "#searchbox-searchbutton"
RESULTS_CONTAINER = 'div[role="feed"]'
RESULT_ITEMS = 'div[role="feed"] > div > div > a'

DETAIL_NAME = ["h1.DUwDvf", "div[role='main'] h1", "h1"]
DETAIL_WEBSITE = 'a[data-item-id="authority"]'
DETAIL_PHONE = 'button[data-item-id^="phone"]'
DETAIL_ADDRESS = 'button[data-item-id="address"]'
DETAIL_CATEGORY = 'button[jsaction="pane.rating.category"]'
DETAIL_RATING = 'div.fontDisplayLarge'
DETAIL_EMAIL = 'a[href^="mailto:"]'


class GoogleMapsScraper:
    """Producer: scrape de Google Maps → persiste vía CRUD."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        headless: bool = settings.HEADLESS,
        max_scrolls: int = settings.MAX_SCROLL_ATTEMPTS,
        scroll_pause: float = settings.SCROLL_PAUSE_SECONDS,
        click_delay_ms: int = settings.CLICK_DELAY_MS,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.session = session
        self.headless = headless
        self.max_scrolls = max_scrolls
        self.scroll_pause = scroll_pause
        self.click_delay_ms = click_delay_ms
        self._on_progress = on_progress

    def _emit(self, message: str) -> None:
        logger.info(message)
        if self._on_progress:
            self._on_progress(message)

    async def run(self, search_query: str) -> int:
        self._emit(f"Iniciando scraping para: '{search_query}'")

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
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = await context.new_page()

            try:
                search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
                self._emit(f"Navegando a: {search_url}")
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(3000)

                await self._handle_consent_dialog(page)

                self._emit(f"Esperando resultados para: {search_query}")
                try:
                    await page.wait_for_selector(RESULTS_CONTAINER, timeout=15000)
                except PlaywrightTimeout:
                    self._emit("No se encontro feed, reintentando busqueda manual...")
                    await self._search_manual(page, search_query)

                self._emit("Cargando resultados (scroll infinito)...")
                await self._scroll_results(page)

                self._emit("Extrayendo informacion de negocios...")
                businesses = await self._extract_businesses(page, search_query)

                if businesses:
                    count = await crud.enqueue_batch(self.session, businesses)
                    self._emit(f"{count} negocios encolados con estado PENDING")
                    return count

                self._emit("No se encontraron negocios")
                return 0

            except Exception as e:
                logger.error("Error durante el scraping: %s", e, exc_info=True)
                self._emit(f"Error: {e}")
                raise
            finally:
                await browser.close()

    # ── Helpers privados (sin cambios funcionales) ────────

    async def _handle_consent_dialog(self, page: Page) -> None:
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
                    self._emit("Cerrando dialogo de consentimiento de cookies...")
                    await btn.click()
                    await page.wait_for_timeout(2000)
                    return
            except Exception:
                continue

    async def _search_manual(self, page: Page, query: str) -> None:
        try:
            search_box = page.locator(SEARCH_INPUT)
            await search_box.wait_for(state="visible", timeout=10000)
            await search_box.click()
            await search_box.fill(query)
            await page.locator(SEARCH_BUTTON).click()
            await page.wait_for_timeout(3000)
            await page.wait_for_selector(RESULTS_CONTAINER, timeout=15000)
        except PlaywrightTimeout:
            self._emit("No se encontro el contenedor de resultados.")

    async def _scroll_results(self, page: Page) -> None:
        feed = page.locator(RESULTS_CONTAINER)
        previous_count = 0
        stale_rounds = 0

        for i in range(self.max_scrolls):
            items = page.locator(RESULT_ITEMS)
            current_count = await items.count()

            if current_count == previous_count:
                stale_rounds += 1
                if stale_rounds >= 3:
                    self._emit(f"Fin del scroll (ronda {i + 1}, {current_count} items)")
                    break
            else:
                stale_rounds = 0

            previous_count = current_count
            self._emit(f"Scroll {i + 1}/{self.max_scrolls} — {current_count} resultados")

            await feed.evaluate("el => el.scrollTop = el.scrollHeight")
            await page.wait_for_timeout(int(self.scroll_pause * 1000))

            end_marker = page.locator("p.fontBodyMedium span:has-text('final')")
            if await end_marker.count() > 0:
                self._emit(f"Llegamos al final ({current_count} items)")
                break

    async def _extract_businesses(
        self, page: Page, search_query: str
    ) -> list[Business]:
        items = page.locator(RESULT_ITEMS)
        total = await items.count()
        self._emit(f"Procesando {total} resultados...")

        businesses: list[Business] = []

        for idx in range(total):
            try:
                item = items.nth(idx)
                await item.scroll_into_view_if_needed()
                # Maps es SPA: tras el clic Playwright puede colgarse esperando
                # "scheduled navigations"; no_wait_after + espera fija al panel.
                await item.click(no_wait_after=True, timeout=15000)
                await page.wait_for_timeout(self.click_delay_ms)

                biz = await self._extract_detail(page, search_query)
                if biz:
                    businesses.append(biz)
                    self._emit(f"  [{idx + 1}/{total}] {biz.name}")
                else:
                    self._emit(f"  [{idx + 1}/{total}] No se pudo extraer datos")

                await page.keyboard.press("Escape")
            except Exception as e:
                logger.warning("Error extrayendo item %d: %s", idx, e)
                self._emit(f"  [{idx + 1}/{total}] Error: {e}")
                try:
                    await page.keyboard.press("Escape")
                except Exception:
                    pass
                continue

        return businesses

    async def _extract_detail(
        self, page: Page, search_query: str
    ) -> Optional[Business]:
        try:
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
                try:
                    name = await page.get_by_role("main").locator("h1").first.inner_text(timeout=1000)
                    name = sanitize_text(name)
                except Exception:
                    pass

            if not name or name.lower() == "resultados":
                return None

            phone = await self._safe_extract(page, DETAIL_PHONE)
            address = await self._safe_extract(page, DETAIL_ADDRESS)

            website = None
            website_el = page.locator(DETAIL_WEBSITE)
            if await website_el.count() > 0:
                website = await website_el.first.get_attribute("href")
                website = normalize_url(website)

            email = None
            email_el = page.locator(DETAIL_EMAIL)
            if await email_el.count() > 0:
                href = await email_el.first.get_attribute("href")
                if href and "mailto:" in href:
                    email = href.replace("mailto:", "").split("?")[0]

            rating = await self._safe_extract(page, DETAIL_RATING)
            category = await self._safe_extract(page, DETAIL_CATEGORY)

            return Business(
                name=name,
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
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                text = await locator.inner_text(timeout=2000)
                return text.strip() if text else None
        except Exception:
            pass
        return None
