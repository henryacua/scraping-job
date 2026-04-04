"""
dashboard.py — Centro de mando Streamlit para scraping-job-ms.

Ejecutar con:
    streamlit run dashboard.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# ── Asegurar que el directorio raíz esté en el path ────
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from src.models import BusinessStatus
from src.queue_manager import QueueManager
from src.scraper import GoogleMapsScraper
from src.processor import LeadProcessor
from src.campaign import WhatsAppCloudAPI
from src.utils import normalize_phone
from src.strategies import AVAILABLE_STRATEGIES, get_strategy


# ── Helpers para async dentro de Streamlit ──────────────

def run_async(coro):
    """Ejecuta una coroutine de forma segura dentro de Streamlit."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def get_queue() -> QueueManager:
    """Retorna el QueueManager singleton."""
    return QueueManager(settings.DB_PATH)


# ── Configuración de la página ──────────────────────────

st.set_page_config(
    page_title="🕷️ Scraping Job MS",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personalizado ───────────────────────────────────

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

    /* KPI Cards Responsive Grid */
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

    /* Table Styling */
    .dataframe {
        font-size: 0.9rem !important;
        width: 100% !important;
    }
    /* Contenedor para scroll horizontal en móviles */
    .table-container {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        margin-top: 1rem;
        border-radius: 8px;
        border: 1px solid rgba(255,255,255,0.1);
    }

    /* Status Badges */
    .status-badge {
        padding: 4px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        white-space: nowrap;
    }
    .status-pending { background: rgba(108, 117, 125, 0.3); color: #e9ecef; border: 1px solid rgba(108, 117, 125, 0.5); }
    .status-qualified { background: rgba(40, 167, 69, 0.2); color: #2ecc71; border: 1px solid rgba(40, 167, 69, 0.4); }
    .status-has-website { background: rgba(0, 123, 255, 0.2); color: #5dade2; border: 1px solid rgba(0, 123, 255, 0.4); }
    .status-error { background: rgba(220, 53, 69, 0.2); color: #ec7063; border: 1px solid rgba(220, 53, 69, 0.4); }
    .status-processing { background: rgba(111, 66, 193, 0.2); color: #d7bde2; border: 1px solid rgba(111, 66, 193, 0.4); }

    /* Action Buttons */
    .wa-btn {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: linear-gradient(135deg, #25D366, #128C7E);
        color: white !important;
        text-decoration: none !important;
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        transition: transform 0.2s, box-shadow 0.2s;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .wa-btn:hover {
        transform: scale(1.05);
        box-shadow: 0 4px 8px rgba(37, 211, 102, 0.4);
        color: white !important;
    }

    .header-title {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(to right, #ffffff, #a5a5a5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .header-subtitle {
        color: #8899a6;
        font-size: 1rem;
        font-weight: 400;
        margin-top: 0;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ──────────────────────────────────────────────

st.markdown('<p class="header-title">🕷️ Scraping Job MS</p>', unsafe_allow_html=True)
st.markdown('<p class="header-subtitle">Google Maps Lead Scraper · Producer-Consumer Dashboard</p>', unsafe_allow_html=True)
st.markdown("---")

# ── Inicializar DB (antes del sidebar para poder consultar búsquedas recientes) ──

queue = get_queue()
run_async(queue.initialize())

recent_queries = run_async(queue.get_recent_queries())

# ── Sidebar: Controles ──────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Configuración")

    # Búsquedas recientes como dropdown
    if recent_queries:
        selected_recent = st.selectbox(
            "🕐 Búsquedas recientes",
            options=["— Nueva búsqueda —"] + recent_queries,
            index=0,
        )
        use_recent = selected_recent != "— Nueva búsqueda —"
    else:
        use_recent = False

    # Campos de búsqueda nueva
    if not use_recent:
        col_svc, col_city = st.columns([3, 2])
        with col_svc:
            search_service = st.text_input(
                "🏢 Servicio / Negocio",
                value="Dentistas",
                placeholder="Ej: Abogados, Restaurantes...",
            )
        with col_city:
            search_city = st.text_input(
                "📍 Ciudad",
                value="Medellín",
                placeholder="Ej: Bogotá, Cali...",
            )
        search_query = f"{search_service} en {search_city}" if search_service and search_city else ""
    else:
        search_query = selected_recent

    # Mostrar la query compuesta
    if search_query:
        st.caption(f"🔎 Búsqueda: **{search_query}**")

    strategy_name = st.selectbox(
        "🎯 Estrategia de acción",
        options=list(AVAILABLE_STRATEGIES.keys()),
        format_func=lambda x: f"{x} — {AVAILABLE_STRATEGIES[x].__doc__.strip().split(chr(10))[0] if AVAILABLE_STRATEGIES[x].__doc__ else x}",
    )

    st.markdown("---")
    st.markdown("## 🚀 Ejecución")

    col_a, col_b = st.columns(2)

    with col_a:
        btn_scrape = st.button("🔍 Buscar (Scrape)", use_container_width=True, type="primary")
    with col_b:
        btn_process = st.button("⚡ Procesar Leads", use_container_width=True)

    st.markdown("---")
    st.markdown("## 📊 Opciones")

    headless = st.checkbox("🖥️ Modo Headless", value=True)
    max_scrolls = st.slider("📜 Máx. Scrolls", min_value=5, max_value=50, value=20)

    st.markdown("---")
    st.markdown(
        "<div style='text-align:center; color: rgba(255,255,255,0.3); font-size: 0.75rem;'>"
        "scraping-job-ms v0.1.0</div>",
        unsafe_allow_html=True,
    )



# ── Funciones de acción ─────────────────────────────────

def action_scrape():
    """Ejecuta el Producer (scraper) siempre desde cero para esta búsqueda."""
    # Verificar si ya existen datos para esta búsqueda
    existing = run_async(queue.get_all_businesses(search_query))

    if existing:
        deleted = run_async(queue.delete_by_query(search_query))
        # st.info(f"🗑️ Limpiando {deleted} registros anteriores...")
    
    log_container = st.empty()
    logs: list[str] = []

    def on_progress(msg: str):
        logs.append(msg)
        log_container.code("\n".join(logs[-10:]), language="text")

    progress_bar = st.progress(0, text="Iniciando scraping...")

    try:
        scraper = GoogleMapsScraper(
            queue,
            headless=headless,
            max_scrolls=max_scrolls,
            on_progress=on_progress,
        )
        count = run_async(scraper.run(search_query))
        progress_bar.progress(100, text=f"✅ {count} negocios encontrados")
        st.success(f"Scraping completado: **{count}** negocios extraídos para *\"{search_query}\"*")
        st.rerun() # Recargar para mostrar datos nuevos
    except Exception as e:
        progress_bar.progress(0, text="❌ Error")
        st.error(f"Error durante el scraping: {e}")






def action_process():
    """Ejecuta el Consumer (procesador de leads)."""
    log_container = st.empty()
    logs: list[str] = []

    def on_progress(msg: str):
        logs.append(msg)
        log_container.code("\n".join(logs[-30:]), language="text")

    progress_bar = st.progress(0, text="Procesando leads...")

    try:
        strategy = get_strategy(strategy_name)
        processor = LeadProcessor(queue, strategy, on_progress=on_progress)
        results = run_async(processor.run())
        progress_bar.progress(100, text="✅ Procesamiento completo")
        st.success(
            f"Procesamiento completo: **{results['leads_qualified']}** leads cualificados, "
            f"**{results['has_website']}** con web, "
            f"**{results['errors']}** errores"
        )
    except Exception as e:
        progress_bar.progress(100, text="❌ Error")
        st.error(f"Error durante el procesamiento: {e}")


# ── Disparar acciones ───────────────────────────────────

if btn_scrape:
    with st.spinner("Ejecutando scraping..."):
        action_scrape()

if btn_process:
    with st.spinner("Procesando leads..."):
        action_process()


# ── KPIs ────────────────────────────────────────────────

st.markdown("## 📈 Métricas")

stats = run_async(queue.get_stats(search_query))

total = stats.get("TOTAL", 0)
leads = stats.get("LEAD_QUALIFIED", 0)
has_web = stats.get("HAS_WEBSITE", 0)
pending = stats.get("PENDING", 0)
errors = stats.get("ERROR", 0)
conversion = (leads / total * 100) if total > 0 else 0

kpi_cols = st.columns(5)

kpi_data = [
    ("🔎", total, "Total Encontrados"),
    ("🟢", leads, "Leads Cualificados"),
    ("🔵", has_web, "Con Sitio Web"),
    ("⏳", pending, "Pendientes"),
    ("📊", f"{conversion:.1f}%", "Tasa de Conversión"),
]

for col, (icon, value, label) in zip(kpi_cols, kpi_data):
    with col:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div style="font-size: 1.5rem;">{icon}</div>
                <p class="kpi-value">{value}</p>
                <p class="kpi-label">{label}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)


# ── Data Grid ───────────────────────────────────────────

# ── Data Grid ───────────────────────────────────────────

st.markdown("## 📋 Negocios Encontrados")

businesses = run_async(queue.get_all_businesses(search_query))

if businesses:
    df = pd.DataFrame([b.to_dict() for b in businesses])
    
    # ── Controles Principales (Fila única) ──
    # Orden: Buscador -> Filtro Estado -> Ordenar
    c_search, c_filter, c_sort, c_order = st.columns([2, 1.5, 1.5, 1])

    with c_search:
        search_name = st.text_input("Buscar por nombre", value="", placeholder="Escribe para filtrar...")
        
    with c_filter:
        status_filter = st.multiselect(
            "Estado",
            options=[s.value for s in BusinessStatus],
            default=[],
            placeholder="Todos"
        )
        
    with c_sort:
        sort_col = st.selectbox(
            "Ordenar por",
            options=["name", "rating", "reviews_count", "status", "category"],
            format_func=lambda x: {
                "name": "Nombre",
                "rating": "Rating",
                "reviews_count": "Reseñas",
                "status": "Estado",
                "category": "Categoría"
            }.get(x, x),
            index=1
        )
        
    with c_order:
        st.write("") # Spacer para alinear verticalmente con inputs
        st.write("")
        sort_asc = st.radio(
            "Dirección",
            options=["Asc", "Desc"],
            index=1,
            horizontal=True,
            label_visibility="collapsed"
        )

    # ── Aplicar Lógica ──
    df_filtered = df.copy()
    
    # 1. Filtros
    if status_filter:
        df_filtered = df_filtered[df_filtered["status"].isin(status_filter)]
    if search_name:
        df_filtered = df_filtered[
            df_filtered["name"].str.contains(search_name, case=False, na=False)
        ]
        
    # 2. Orden
    if sort_col:
        ascending = sort_asc == "Asc"
        if sort_col in ["rating", "reviews_count"]:
             df_filtered[sort_col] = pd.to_numeric(df_filtered[sort_col], errors='coerce')
        df_filtered = df_filtered.sort_values(by=sort_col, ascending=ascending)

    st.markdown("---") 
    
    # ── Sección de Campaña (Separada para claridad) ──
    with st.container():
        # Verificar si la API está configurada
        wa_client = WhatsAppCloudAPI()
        api_configured = wa_client.is_configured

        if not api_configured:
            st.warning(
                "⚠️ **WhatsApp API no configurada.** "
                "Agrega `WA_API_TOKEN` y `WA_PHONE_NUMBER_ID` en tu archivo `.env` "
                "para habilitar campañas masivas. "
                "[Obtener credenciales →](https://developers.facebook.com)"
            )

        c_mode, c_msg_input = st.columns([1, 3])

        with c_mode:
            wa_mode = st.radio(
                "Modo de envío",
                options=["Template", "Texto libre"],
                index=0,
                help="Template: usa un template aprobado por Meta (puede iniciar conversaciones). "
                     "Texto libre: solo funciona si el destinatario ya te escribió."
            )
            use_template_mode = wa_mode == "Template"

        with c_msg_input:
            if use_template_mode:
                wa_template_name = st.text_input(
                    "📋 Nombre del Template",
                    value=settings.WA_TEMPLATE_NAME,
                    help="Nombre exacto del template aprobado en Meta Business Manager."
                )
                wa_template_lang = st.text_input(
                    "🌐 Idioma del Template",
                    value=settings.WA_TEMPLATE_LANG,
                )
                wa_template = f"[Template: {wa_template_name}]"
            else:
                wa_template = st.text_area(
                    "💬 Mensaje para Campaña WhatsApp",
                    value="Hola {nombre}, vi que no tienes sitio web y puedo ayudarte a crear uno increíble para tu negocio.",
                    height=100,
                    help="Usa {nombre} para insertar el nombre del negocio automáticamente."
                )
                wa_template_name = settings.WA_TEMPLATE_NAME
                wa_template_lang = settings.WA_TEMPLATE_LANG

        # ── Botón de Campaña Masiva ──
        campaign_logs = run_async(queue.get_message_logs())
        sent_ids = {bid for bid, status in campaign_logs.items() if status == "SENT"}
        pending_leads_df = df_filtered[~df_filtered["id"].isin(sent_ids)]
        count_pending = len(pending_leads_df)

        if count_pending > 0 and api_configured:
            if st.button(f"🚀 Iniciar Campaña ({count_pending} pendientes)", type="primary", use_container_width=True):
                target_ids = set(pending_leads_df["id"].values)
                target_businesses = [b for b in businesses if b.id in target_ids]

                campaign_prog = st.status("🚀 Ejecutando campaña...", expanded=True)
                log_box = st.empty()
                logs = []

                def on_campaign_progress(msg):
                    logs.append(msg)
                    log_box.code("\n".join(logs[-10:]), language="text")

                async def persist_log(bid, status, tmpl):
                    await queue.log_message(bid, status, tmpl)

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
                    stats = run_async(run_campaign_flow())
                    campaign_prog.update(label="✅ Campaña finalizada!", state="complete", expanded=False)
                    st.success(f"Resultados: {stats.sent} enviados, {stats.failed} fallidos, {stats.skipped} saltados.")
                    st.rerun()
                except Exception as e:
                    campaign_prog.update(label="❌ Error en campaña", state="error")
                    st.error(f"Error crítico: {e}")
        elif count_pending == 0:
            st.info("✅ Todos contactados")

    st.markdown(f"**Viendo {len(df_filtered)} de {len(df)} registros**")


    # ── Preparar Display ──
    # Añadimos estado de campaña al DF
    def get_campaign_status(row):
        return campaign_logs.get(row["id"], "—")
    
    df_filtered["campaign_status"] = df_filtered.apply(get_campaign_status, axis=1)

    # Columnas a mostrar
    display_cols = ["name", "phone", "email", "campaign_status", "address", "website", "status", "category", "rating"]
    display_cols = [c for c in display_cols if c in df_filtered.columns]
    
    # Trabajamos sobre una copia para formatear
    df_display = df_filtered[display_cols].copy()

    # Renombrar columnas al español
    col_rename = {
        "name": "Nombre",
        "phone": "Teléfono",
        "email": "Email",
        "campaign_status": "Estado Campaña",
        "address": "Dirección",
        "website": "Sitio Web",
        "status": "Estado",
        "category": "Categoría",
        "rating": "Rating",
    }
    df_display = df_display.rename(columns=col_rename)

    # Colorear estados
    def style_status(val):
        status_map = {
            "PENDING": "status-pending",
            "PROCESSING": "status-processing",
            "LEAD_QUALIFIED": "status-qualified",
            "HAS_WEBSITE": "status-has-website",
            "ERROR": "status-error",
        }
        css_class = status_map.get(val, "status-pending")
        return f'<span class="status-badge {css_class}">{val}</span>'

    def style_campaign(val):
        if val == "SENT":
            return '<span class="status-badge status-qualified">✅ Enviado</span>'
        elif val == "FAILED" or "FAILED" in str(val):
            return '<span class="status-badge status-error">❌ Fallido</span>'
        elif val == "SKIPPED":
            return '<span class="status-badge status-pending">⏭️ Saltado</span>'
        else:
            return '<span style="color:rgba(255,255,255,0.3);">—</span>'

    if "Estado" in df_display.columns:
        df_display["Estado"] = df_display["Estado"].apply(style_status)
    
    if "Estado Campaña" in df_display.columns:
        df_display["Estado Campaña"] = df_display["Estado Campaña"].apply(style_campaign)

    # Reemplazar NaN
    df_display = df_display.fillna("—")

    # ── Agregar Columna de Acción (WhatsApp Individual) ──
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
        encoded_msg = quote(msg_text)

        url = f"https://wa.me/{phone_clean}?text={encoded_msg}"

        return f'<a href="{url}" target="_blank" class="wa-btn"><span>📲</span> Enviar</a>'

    df_display["Acción"] = df_filtered.apply(create_wa_link, axis=1)

    # ── Renderizar Tabla con Container Responsivo ──
    st.markdown(
        f"""
        <div class="table-container">
            {df_display.to_html(escape=False, index=False, classes="dataframe")}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Botón de Exportación (Ahora abajo de la tabla filtrada o arriba si prefieres) ──
    # El usuario pidió "interactuar con la ya renderizada", ponerlo cerca es lo mejor.
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
