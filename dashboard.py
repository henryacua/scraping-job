"""
dashboard.py — Centro de mando Streamlit para scraping-job-ms.

Ejecución local:
    streamlit run dashboard.py

Deploy en Streamlit Cloud:
    - Conectar el repo de GitHub
    - Pegar variables en el panel Secrets (TOML); en local usa solo .env en la raíz del repo.

Modos de operación (controlado por DASHBOARD_MODE):
    - DASHBOARD_MODE=local  → ejecuta scraping/procesamiento en el mismo proceso
    - DASHBOARD_MODE=remote → delega al Worker en Render vía HTTP (RENDER_API_URL)
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests as _requests
import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Mismo venv que uvicorn puede dejar instalada la policy de uvloop; Streamlit
# corre en otro hilo y asyncio.get_event_loop() falla. Forzar loop estándar.
asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

from backend.app.core.config import settings
from backend.app.core.db import engine
from backend.app import crud
from backend.app.models import Business, BusinessStatus
from backend.app.services.campaign import WhatsAppCloudAPI
from backend.app.services.utils import normalize_phone
from backend.app.services.strategies import AVAILABLE_STRATEGIES, get_strategy, get_all_strategies


def run_async(coro):
    """Ejecuta una coroutine en un loop dedicado (Streamlit corre en otro hilo).

    Postgres + asyncpg: el engine usa NullPool bajo Streamlit; aun así hacemos
    dispose al inicio y en finally **antes** de cerrar el loop para que los
    transportes SSL no programen callbacks en un loop ya cerrado.
    """
    async def _wrapped():
        try:
            await engine.dispose()
        except Exception:
            pass
        try:
            return await coro
        finally:
            try:
                await engine.dispose()
            except Exception:
                pass

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_exc(lp: asyncio.AbstractEventLoop, ctx: dict) -> None:
        exc = ctx.get("exception")
        if isinstance(exc, RuntimeError) and "Event loop is closed" in str(exc):
            return
        if "Event loop is closed" in str(ctx.get("message", "")):
            return
        lp.default_exception_handler(ctx)

    loop.set_exception_handler(_handle_exc)
    try:
        return loop.run_until_complete(_wrapped())
    finally:
        try:
            loop.close()
        except Exception:
            pass


async def _get_session():
    from sqlmodel.ext.asyncio.session import AsyncSession
    return AsyncSession(engine)


async def _init_db():
    from backend.app.core.db import create_db_and_tables
    await create_db_and_tables()


async def _get_recent_queries():
    async with AsyncSession(engine) as session:
        return await crud.get_recent_queries(session)


async def _get_stats(search_query):
    async with AsyncSession(engine) as session:
        return await crud.get_stats(session, search_query)


async def _get_all_businesses(search_query):
    async with AsyncSession(engine) as session:
        return await crud.get_all_businesses(session, search_query)


async def _get_message_logs():
    async with AsyncSession(engine) as session:
        return await crud.get_message_logs(session)


async def _delete_by_query(search_query):
    async with AsyncSession(engine) as session:
        return await crud.delete_by_query(session, search_query)


async def _log_message(bid, status, tmpl):
    async with AsyncSession(engine) as session:
        return await crud.log_message(session, bid, status, tmpl)


from sqlmodel.ext.asyncio.session import AsyncSession


def _api_headers() -> dict:
    headers = {}
    if settings.API_KEY:
        headers["x-api-key"] = settings.API_KEY
    return headers


def _poll_job(job_id: str, status_widget) -> dict:
    url = f"{settings.RENDER_API_URL}/jobs/{job_id}"
    for _ in range(120):
        time.sleep(3)
        try:
            resp = _requests.get(url, headers=_api_headers(), timeout=10)
            job = resp.json()
            status_widget.write(f"Estado: **{job.get('status', '...')}**")
            if job.get("status") in ("completed", "failed"):
                return job
        except Exception as exc:
            status_widget.write(f"Esperando respuesta... ({exc})")
    return {"status": "failed", "error": "Timeout esperando al worker"}


# ── Configuración de página ───────────────────────────────

st.set_page_config(
    page_title="Scraping Job MS",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .stApp {
        background: radial-gradient(circle at 10% 20%, #1a1a2e 0%, #16213e 90%);
        color: #e0e0e0;
    }
    [data-testid="stSidebar"] {
        background: #0f3460;
        border-right: 1px solid rgba(255,255,255,0.05);
    }
    .kpi-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 1.2rem;
        text-align: center;
        margin-bottom: 0.5rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 15px rgba(0,0,0,0.2);
        background: rgba(255, 255, 255, 0.08);
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #00d2ff, #3a7bd5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .kpi-label {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.7);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 500;
    }
    .dataframe { font-size: 0.9rem !important; width: 100% !important; }
    .table-container {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        margin-top: 1rem;
        border-radius: 8px;
        border: 1px solid rgba(255,255,255,0.1);
    }
    .status-badge {
        padding: 4px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        white-space: nowrap;
    }
    .status-pending   { background: rgba(108,117,125,0.3); color:#e9ecef; border:1px solid rgba(108,117,125,0.5); }
    .status-qualified { background: rgba(40,167,69,0.2);  color:#2ecc71; border:1px solid rgba(40,167,69,0.4);  }
    .status-has-website { background: rgba(0,123,255,0.2); color:#5dade2; border:1px solid rgba(0,123,255,0.4); }
    .status-error     { background: rgba(220,53,69,0.2);  color:#ec7063; border:1px solid rgba(220,53,69,0.4);  }
    .status-processing { background: rgba(111,66,193,0.2); color:#d7bde2; border:1px solid rgba(111,66,193,0.4); }
    .wa-btn {
        display: inline-flex; align-items: center; gap: 6px;
        background: linear-gradient(135deg,#25D366,#128C7E);
        color: white !important; text-decoration: none !important;
        padding: 6px 12px; border-radius: 20px; font-size: 0.8rem;
        font-weight: 600; transition: transform 0.2s, box-shadow 0.2s;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .wa-btn:hover { transform:scale(1.05); box-shadow:0 4px 8px rgba(37,211,102,0.4); color:white !important; }
    .header-title {
        font-size: 2.2rem; font-weight: 800;
        background: linear-gradient(to right,#ffffff,#a5a5a5);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .header-subtitle { color:#8899a6; font-size:1rem; font-weight:400; margin-top:0; }
    .mode-badge {
        display: inline-block; padding: 3px 10px; border-radius: 20px;
        font-size: 0.72rem; font-weight: 600;
    }
    .mode-remote { background: rgba(0,210,255,0.15); color:#00d2ff; border:1px solid rgba(0,210,255,0.3); }
    .mode-local  { background: rgba(255,193,7,0.15);  color:#ffc107; border:1px solid rgba(255,193,7,0.3); }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────

remote_mode = settings.DASHBOARD_MODE.lower() == "remote"

if remote_mode and not settings.RENDER_API_URL:
    st.error(
        "**DASHBOARD_MODE=remote** pero **RENDER_API_URL** está vacía. "
        "Configura la URL del worker en `.env` o cambia a `DASHBOARD_MODE=local`."
    )
    st.stop()

mode_label = "Render (remoto)" if remote_mode else "Local"
mode_class = "mode-remote" if remote_mode else "mode-local"

st.markdown('<p class="header-title">🕷️ Scraping Job MS</p>', unsafe_allow_html=True)
st.markdown(
    f'<p class="header-subtitle">Google Maps Lead Scraper &nbsp;·&nbsp; '
    f'<span class="mode-badge {mode_class}">Worker: {mode_label}</span></p>',
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Inicializar DB ────────────────────────────────────────

run_async(_init_db())
recent_queries = run_async(_get_recent_queries())

# ── Sidebar ───────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Configuración")

    if recent_queries:
        selected_recent = st.selectbox(
            "🕐 Búsquedas recientes",
            options=["— Nueva búsqueda —"] + recent_queries,
            index=0,
        )
        use_recent = selected_recent != "— Nueva búsqueda —"
    else:
        use_recent = False

    if not use_recent:
        col_svc, col_city = st.columns([3, 2])
        with col_svc:
            search_service = st.text_input("🏢 Servicio / Negocio", value="Dentistas", placeholder="Ej: Abogados...")
        with col_city:
            search_city = st.text_input("📍 Ciudad", value="Medellín", placeholder="Ej: Bogotá...")
        search_query = f"{search_service} en {search_city}" if search_service and search_city else ""
    else:
        search_query = selected_recent

    if search_query:
        st.caption(f"🔎 Búsqueda: **{search_query}**")

    strategy_name = st.selectbox(
        "🎯 Estrategia de acción",
        options=list(AVAILABLE_STRATEGIES.keys()),
        format_func=lambda x: f"{x} — {AVAILABLE_STRATEGIES[x].__doc__.strip().split(chr(10))[0] if AVAILABLE_STRATEGIES[x].__doc__ else x}",
    )

    maps_source = st.radio(
        "🗺️ Fuente de datos",
        options=["playwright", "places_api"],
        format_func=lambda x: {
            "playwright": "Playwright (scraper local)",
            "places_api": "Places API (Google)",
        }[x],
        index=0 if settings.MAPS_SOURCE == "playwright" else 1,
        horizontal=True,
    )

    if maps_source == "places_api" and not settings.GOOGLE_MAPS_API_KEY:
        st.warning(
            "Configura **GOOGLE_MAPS_API_KEY** en `.env` para usar Places API.",
            icon="🔑",
        )

    st.markdown("---")
    st.markdown("## 🚀 Ejecución")

    col_a, col_b = st.columns(2)
    with col_a:
        btn_label = "🔍 Buscar (Scrape)" if maps_source == "playwright" else "🔍 Buscar (API)"
        btn_scrape = st.button(btn_label, use_container_width=True, type="primary")
    with col_b:
        btn_process = st.button("⚡ Procesar Leads", use_container_width=True)

    st.markdown("---")
    st.markdown("## 📊 Opciones")

    if maps_source == "playwright":
        headless = st.checkbox("🖥️ Modo Headless", value=True)
        max_scrolls = st.slider("📜 Máx. Scrolls", min_value=5, max_value=50, value=20)
        max_results = 60
    else:
        headless = True
        max_scrolls = 20
        max_results = st.slider("📋 Máx. Resultados", min_value=5, max_value=60, value=20)

    st.markdown("---")
    st.markdown(
        "<div style='text-align:center;color:rgba(255,255,255,0.3);font-size:0.75rem;'>"
        "scraping-job-ms v0.2.0</div>",
        unsafe_allow_html=True,
    )


# ── Acciones: modo remoto (Render API) ───────────────────

def action_scrape_remote():
    try:
        resp = _requests.post(
            f"{settings.RENDER_API_URL}/scrape",
            json={
                "query": search_query,
                "source": maps_source,
                "max_scrolls": max_scrolls,
                "max_results": max_results,
                "headless": headless,
            },
            headers=_api_headers(),
            timeout=15,
        )
        if resp.status_code == 503:
            detail = resp.json().get("detail", "")
            st.warning(
                "**El scraping con Playwright no puede ejecutarse en Render** — "
                "Google bloquea las IPs de datacenters.\n\n"
                "**¿Qué hacer?** Cambia `DASHBOARD_MODE=local` en tu `.env` para "
                "ejecutar el scraping desde tu máquina con IP residencial, o "
                "selecciona **Places API (Google)** como fuente de datos.",
                icon="⚠️",
            )
            return
        resp.raise_for_status()
    except Exception as exc:
        st.error(f"No se pudo contactar el Worker en Render: {exc}")
        return

    job = resp.json()
    job_id = job["job_id"]

    with st.status("Ejecutando scraping en Render...", expanded=True) as status_widget:
        log_area = st.empty()
        result = _poll_job(job_id, log_area)
        if result.get("status") == "completed":
            count = result.get("businesses_found", "?")
            status_widget.update(label=f"Scraping completado: {count} negocios", state="complete")
            st.success(f"Se encontraron **{count}** negocios para *\"{search_query}\"*")
            st.rerun()
        else:
            status_widget.update(label="Error en scraping", state="error")
            st.error(f"El job fallo: {result.get('error', 'Error desconocido')}")


def action_process_remote():
    try:
        resp = _requests.post(
            f"{settings.RENDER_API_URL}/process",
            json={"batch_size": settings.BATCH_SIZE},
            headers=_api_headers(),
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as exc:
        st.error(f"No se pudo contactar el Worker en Render: {exc}")
        return

    job = resp.json()
    job_id = job["job_id"]

    with st.status("Procesando leads en Render...", expanded=True) as status_widget:
        log_area = st.empty()
        result = _poll_job(job_id, log_area)
        if result.get("status") == "completed":
            r = result.get("results", {})
            status_widget.update(label="Procesamiento completado", state="complete")
            st.success(
                f"**{r.get('passed', 0)}** leads cualificados · "
                f"**{r.get('filtered_out', 0)}** filtrados · "
                f"**{r.get('errors', 0)}** errores"
            )
        else:
            status_widget.update(label="Error en procesamiento", state="error")
            st.error(f"El job fallo: {result.get('error', 'Error desconocido')}")


# ── Acciones: modo local ──────────────────────────────────

def action_scrape_local():
    from backend.app.services.producer import create_producer

    existing = run_async(_get_all_businesses(search_query))
    if existing:
        run_async(_delete_by_query(search_query))

    log_container = st.empty()
    logs: list[str] = []

    def on_progress(msg: str):
        logs.append(msg)
        log_container.code("\n".join(logs[-10:]), language="text")

    source_label = "Places API" if maps_source == "places_api" else "scraping"
    progress_bar = st.progress(0, text=f"Iniciando {source_label}...")
    try:
        async def _do_scrape():
            async with AsyncSession(engine) as session:
                producer = create_producer(
                    source=maps_source,
                    session=session,
                    on_progress=on_progress,
                    headless=headless,
                    max_scrolls=max_scrolls,
                    max_results=max_results,
                )
                return await producer.run(search_query)

        count = run_async(_do_scrape())
        progress_bar.progress(100, text=f"✅ {count} negocios encontrados")
        st.success(f"Busqueda completada: **{count}** negocios para *\"{search_query}\"*")
        st.rerun()
    except Exception as exc:
        progress_bar.progress(0, text="❌ Error")
        st.error(f"Error durante la busqueda: {exc}")


def action_process_local():
    from backend.app.services.processor import LeadProcessor

    log_container = st.empty()
    logs: list[str] = []

    def on_progress(msg: str):
        logs.append(msg)
        log_container.code("\n".join(logs[-30:]), language="text")

    progress_bar = st.progress(0, text="Procesando leads...")
    try:
        async def _do_process():
            async with AsyncSession(engine) as session:
                actions = get_all_strategies()
                processor = LeadProcessor(session, actions, on_progress=on_progress)
                return await processor.run()

        results = run_async(_do_process())
        progress_bar.progress(100, text="✅ Procesamiento completo")
        st.success(
            f"**{results['passed']}** leads cualificados · "
            f"**{results['filtered_out']}** filtrados · "
            f"**{results['errors']}** errores"
        )
    except Exception as exc:
        progress_bar.progress(100, text="❌ Error")
        st.error(f"Error durante el procesamiento: {exc}")


# ── Despachar acciones ────────────────────────────────────

if btn_scrape:
    with st.spinner("Iniciando..."):
        if remote_mode:
            action_scrape_remote()
        else:
            action_scrape_local()

if btn_process:
    with st.spinner("Iniciando..."):
        if remote_mode:
            action_process_remote()
        else:
            action_process_local()


# ── KPIs ──────────────────────────────────────────────────

st.markdown("## 📈 Métricas")

stats = run_async(_get_stats(search_query))

total = stats.get("TOTAL", 0)
leads_count = stats.get("LEAD_QUALIFIED", 0)
has_web = stats.get("HAS_WEBSITE", 0)
pending = stats.get("PENDING", 0)
conversion = (leads_count / total * 100) if total > 0 else 0

kpi_cols = st.columns(5)
kpi_data = [
    ("🔎", total, "Total Encontrados"),
    ("🟢", leads_count, "Leads Cualificados"),
    ("🔵", has_web, "Con Sitio Web"),
    ("⏳", pending, "Pendientes"),
    ("📊", f"{conversion:.1f}%", "Tasa de Conversión"),
]
for col, (icon, value, label) in zip(kpi_cols, kpi_data):
    with col:
        st.markdown(
            f'<div class="kpi-card">'
            f'<div style="font-size:1.5rem;">{icon}</div>'
            f'<p class="kpi-value">{value}</p>'
            f'<p class="kpi-label">{label}</p>'
            f'</div>',
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)


# ── Data Grid ─────────────────────────────────────────────

st.markdown("## 📋 Negocios Encontrados")

businesses = run_async(_get_all_businesses(search_query))

if businesses:
    def _biz_to_dict(b: Business) -> dict:
        return {
            "id": b.id,
            "name": b.name,
            "phone": b.phone,
            "address": b.address,
            "website": b.website,
            "email": b.email,
            "status": b.status,
            "search_query": b.search_query,
            "rating": b.rating,
            "reviews_count": b.reviews_count,
            "category": b.category,
            "filter_reason": b.filter_reason,
            "created_at": str(b.created_at),
            "updated_at": str(b.updated_at),
        }

    df = pd.DataFrame([_biz_to_dict(b) for b in businesses])

    c_search, c_filter, c_sort, c_order = st.columns([2, 1.5, 1.5, 1])
    with c_search:
        search_name = st.text_input("Buscar por nombre", value="", placeholder="Escribe para filtrar...")
    with c_filter:
        status_filter = st.multiselect(
            "Estado", options=[s.value for s in BusinessStatus], default=[], placeholder="Todos"
        )
    with c_sort:
        sort_col = st.selectbox(
            "Ordenar por",
            options=["name", "rating", "reviews_count", "status", "category"],
            format_func=lambda x: {"name":"Nombre","rating":"Rating","reviews_count":"Reseñas","status":"Estado","category":"Categoría"}.get(x, x),
            index=1,
        )
    with c_order:
        st.write("")
        st.write("")
        sort_asc = st.radio("Dirección", options=["Asc", "Desc"], index=1, horizontal=True, label_visibility="collapsed")

    df_filtered = df.copy()
    if status_filter:
        df_filtered = df_filtered[df_filtered["status"].isin(status_filter)]
    if search_name:
        df_filtered = df_filtered[df_filtered["name"].str.contains(search_name, case=False, na=False)]
    if sort_col:
        ascending = sort_asc == "Asc"
        if sort_col in ["rating", "reviews_count"]:
            df_filtered[sort_col] = pd.to_numeric(df_filtered[sort_col], errors="coerce")
        df_filtered = df_filtered.sort_values(by=sort_col, ascending=ascending)

    st.markdown("---")

    # ── Sección campaña WhatsApp ──
    with st.container():
        wa_client = WhatsAppCloudAPI()
        api_configured = wa_client.is_configured

        if not api_configured:
            st.warning(
                "⚠️ **WhatsApp API no configurada.** "
                "Agrega `WA_API_TOKEN` y `WA_PHONE_NUMBER_ID` en tu `.env` "
                "para habilitar campañas masivas. "
                "[Obtener credenciales →](https://developers.facebook.com)"
            )

        c_mode, c_msg_input = st.columns([1, 3])
        with c_mode:
            wa_mode = st.radio(
                "Modo de envío", options=["Template", "Texto libre"], index=0,
                help="Template: usa un template aprobado por Meta. Texto libre: solo si el destinatario ya escribió."
            )
            use_template_mode = wa_mode == "Template"

        with c_msg_input:
            if use_template_mode:
                wa_template_name = st.text_input("📋 Nombre del Template", value=settings.WA_TEMPLATE_NAME)
                wa_template_lang = st.text_input("🌐 Idioma del Template", value=settings.WA_TEMPLATE_LANG)
                wa_template = f"[Template: {wa_template_name}]"
            else:
                wa_template = st.text_area(
                    "💬 Mensaje para Campaña WhatsApp",
                    value="Hola {nombre}, vi que no tienes sitio web y puedo ayudarte.",
                    height=100,
                    help="Usa {nombre} para insertar el nombre del negocio automáticamente.",
                )
                wa_template_name = settings.WA_TEMPLATE_NAME
                wa_template_lang = settings.WA_TEMPLATE_LANG

        campaign_logs = run_async(_get_message_logs())
        sent_ids = {bid for bid, status in campaign_logs.items() if status == "SENT"}
        pending_leads_df = df_filtered[~df_filtered["id"].isin(sent_ids)]
        count_pending = len(pending_leads_df)

        if count_pending > 0 and api_configured:
            if st.button(f"🚀 Iniciar Campaña ({count_pending} pendientes)", type="primary", use_container_width=True):
                target_ids = set(pending_leads_df["id"].values)
                target_businesses = [b for b in businesses if b.id in target_ids]

                campaign_prog = st.status("🚀 Ejecutando campaña...", expanded=True)
                log_box = st.empty()
                log_lines: list[str] = []

                def on_campaign_progress(msg: str):
                    log_lines.append(msg)
                    log_box.code("\n".join(log_lines[-10:]), language="text")

                async def persist_log(bid, status, tmpl):
                    await _log_message(bid, status, tmpl)

                async def run_campaign_flow():
                    client = WhatsAppCloudAPI(on_progress=on_campaign_progress)
                    return await client.send_bulk(
                        target_businesses,
                        wa_template,
                        persist_log,
                        use_template_mode=use_template_mode,
                        template_name=wa_template_name,
                        template_lang=wa_template_lang,
                    )

                try:
                    campaign_stats = run_async(run_campaign_flow())
                    campaign_prog.update(label="✅ Campaña finalizada!", state="complete", expanded=False)
                    st.success(f"Resultados: {campaign_stats.sent} enviados, {campaign_stats.failed} fallidos, {campaign_stats.skipped} saltados.")
                    st.rerun()
                except Exception as exc:
                    campaign_prog.update(label="❌ Error en campaña", state="error")
                    st.error(f"Error crítico: {exc}")
        elif count_pending == 0:
            st.info("✅ Todos contactados")

    st.markdown(f"**Viendo {len(df_filtered)} de {len(df)} registros**")

    # ── Tabla ──
    def get_campaign_status(row):
        return campaign_logs.get(row["id"], "—")

    df_filtered = df_filtered.copy()
    df_filtered["campaign_status"] = df_filtered.apply(get_campaign_status, axis=1)

    display_cols = ["name", "phone", "email", "campaign_status", "address", "website", "status", "category", "rating"]
    display_cols = [c for c in display_cols if c in df_filtered.columns]
    df_display = df_filtered[display_cols].copy()

    col_rename = {
        "name": "Nombre", "phone": "Teléfono", "email": "Email",
        "campaign_status": "Estado Campaña", "address": "Dirección",
        "website": "Sitio Web", "status": "Estado", "category": "Categoría", "rating": "Rating",
    }
    df_display = df_display.rename(columns=col_rename)

    def style_status(val):
        status_map = {
            "PENDING": "status-pending", "PROCESSING": "status-processing",
            "LEAD_QUALIFIED": "status-qualified", "HAS_WEBSITE": "status-has-website",
            "ERROR": "status-error",
        }
        css_class = status_map.get(val, "status-pending")
        return f'<span class="status-badge {css_class}">{val}</span>'

    def style_campaign(val):
        if val == "SENT":
            return '<span class="status-badge status-qualified">✅ Enviado</span>'
        elif "FAILED" in str(val):
            return '<span class="status-badge status-error">❌ Fallido</span>'
        elif val == "SKIPPED":
            return '<span class="status-badge status-pending">⏭️ Saltado</span>'
        return '<span style="color:rgba(255,255,255,0.3);">—</span>'

    if "Estado" in df_display.columns:
        df_display["Estado"] = df_display["Estado"].apply(style_status)
    if "Estado Campaña" in df_display.columns:
        df_display["Estado Campaña"] = df_display["Estado Campaña"].apply(style_campaign)
    df_display = df_display.fillna("—")

    def create_wa_link(row):
        status = campaign_logs.get(row["id"])
        if status == "SENT":
            return '<span style="font-size:1.2rem;">✅</span>'
        phone_clean = normalize_phone(str(row.get("phone", "")))
        name = str(row.get("name", ""))
        if not phone_clean:
            return "—"
        from urllib.parse import quote
        msg_text = wa_template.replace("{nombre}", name) if "{nombre}" in wa_template else wa_template
        url = f"https://wa.me/{phone_clean}?text={quote(msg_text)}"
        return f'<a href="{url}" target="_blank" class="wa-btn"><span>📲</span> Enviar</a>'

    df_display["Acción"] = df_filtered.apply(create_wa_link, axis=1)

    st.markdown(
        f'<div class="table-container">{df_display.to_html(escape=False, index=False, classes="dataframe")}</div>',
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    csv_data = df_filtered.to_csv(index=False, encoding="utf-8")
    st.download_button(
        label="📥 Descargar CSV (Filtrado)",
        data=csv_data,
        file_name=f"businesses_{search_query.replace(' ', '_')}_filtered.csv",
        mime="text/csv",
    )

else:
    st.info(
        "📭 No hay datos para esta búsqueda. "
        "Usa el botón **🔍 Buscar (Scrape)** en la barra lateral para comenzar."
    )
