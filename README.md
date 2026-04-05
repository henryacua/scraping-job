# 🕷️ scraping-job-ms

**Sistema modular de scraping de Google Maps** para detectar negocios sin sitio web, generar leads cualificados y ejecutar campañas de contacto vía WhatsApp.

## Arquitectura

```
Producer (Scraper)  →  DB Queue  →  Consumer (Processor)  →  Action Pipeline
     ↑                    ↑                                        ↓
  Playwright         SQLite / PG                          FilterPhone / WhatsApp
  (Render)           (Supabase)

Dashboard (Streamlit Cloud)  →  HTTP  →  Worker API (Render)
```

- **Producer-Consumer** desacoplado con SQLite (local) o PostgreSQL/Supabase (producción)
- **Strategy Pattern** para pipeline de filtros extensible
- **Async** de punta a punta (Playwright + SQLModel + aiohttp)
- **FastAPI** como worker API en Render (lanza jobs, reporta estado)
- **Streamlit** como dashboard visual en Streamlit Cloud
- **Alembic** para migraciones de base de datos

## Estructura del Proyecto

```
scraping-job-ms/
│
├── backend/                        # Paquete principal (código activo)
│   ├── alembic.ini                 # Configuración de Alembic
│   └── app/
│       ├── main.py                 # FastAPI entry point (Render)
│       ├── models.py               # SQLModel tables + API schemas
│       ├── crud.py                 # Repositorio: operaciones de DB
│       ├── core/
│       │   ├── config.py           # Pydantic Settings (variables de entorno)
│       │   └── db.py               # Async engine + session factory
│       ├── api/
│       │   ├── deps.py             # Auth: validación de API key
│       │   └── routes/
│       │       ├── scraping.py     # POST /scrape, GET /jobs/{id}
│       │       └── leads.py        # POST /process
│       ├── services/
│       │   ├── scraper.py          # Producer: Google Maps con Playwright
│       │   ├── processor.py        # Consumer: pipeline de filtros
│       │   ├── strategies.py       # Acciones concretas (filtros)
│       │   ├── campaign.py         # WhatsApp Cloud API
│       │   └── utils.py            # Logger, helpers
│       └── alembic/
│           └── versions/           # Migraciones versionadas
│
├── src/                            # Código legacy (referencia)
├── config/                         # Config legacy (referencia)
│
├── dashboard.py                    # Dashboard Streamlit (Streamlit Cloud)
├── main.py                         # CLI orquestador
├── Dockerfile                      # Render: Streamlit dashboard.py
├── Dockerfile.worker               # Render: FastAPI + Places SDK + Playwright (un solo worker)
├── requirements.txt                # Deps Streamlit Cloud (sin Playwright)
├── requirements-render.txt         # Deps imagen playwright (Render)
├── requirements-render-places.txt  # Deps imagen ligera Places API (Render)
└── .env.example                    # Variables de entorno de referencia
```

## Deploy

El proyecto usa **tres servicios gratuitos** coordinados:

| Servicio | Qué corre | URL configurada en |
|----------|-----------|-------------------|
| **Render (Docker)** | `Dockerfile` → Streamlit; `Dockerfile.worker` → API (`source` = `places_api` o `playwright`) | `RENDER_API_URL` (si `DASHBOARD_MODE=remote`) |
| **Streamlit Cloud** | `dashboard.py` (sin Docker en su flujo nativo) | — |
| **Supabase** | PostgreSQL persistente | `DATABASE_URL` |

### Render (Docker)

| Archivo | Contenido |
|---------|-----------|
| `Dockerfile` | **Streamlit** — `streamlit run dashboard.py` (`requirements.txt`). |
| `Dockerfile.worker` | **FastAPI**: SDK Places + Playwright. El modo lo elige `source` en `POST /scrape`. |

## Setup local

### Requisitos

- Python 3.12+
- Chromium (instalado por Playwright)

### Instalación

```bash
git clone https://github.com/tu-usuario/scraping-job-ms.git
cd scraping-job-ms

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt    # Dashboard (sin Playwright)
pip install -r requirements-dev.txt

playwright install chromium        # Solo si necesitas scraping local

cp .env.example .env
```

> Para el **worker completo** (FastAPI + Playwright):
> ```bash
> pip install -r requirements-render.txt
> playwright install chromium --with-deps
> ```

### Variables de entorno (`.env`)

```bash
# DB: SQLite local o PostgreSQL (Supabase)
DATABASE_URL=sqlite+aiosqlite:///./queue.db
# DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/postgres

# Worker Render
RENDER_API_URL=https://scraping-job-ms.onrender.com
API_KEY=tu-clave-secreta

# WhatsApp Cloud API (opcional)
WA_API_TOKEN=
WA_PHONE_NUMBER_ID=
```

## Uso

> Activa el entorno virtual antes: `source .venv/bin/activate`

### 🖥️ Dashboard (Streamlit)

```bash
streamlit run dashboard.py
```

El dashboard permite:
- Configurar búsquedas y seleccionar estrategias de filtrado
- Lanzar scraping y procesamiento (local o remoto vía Render)
- Visualizar KPIs: total encontrados, leads cualificados, tasa de conversión
- Explorar, filtrar y exportar datos en tabla interactiva
- Ejecutar campañas masivas de WhatsApp

### 🚀 Worker API (Render / local)

```bash
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
```

Endpoints disponibles:

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/scrape` | Lanza job de scraping en background |
| `POST` | `/process` | Procesa leads pendientes |
| `GET` | `/jobs/{job_id}` | Estado de un job |

### ⌨️ CLI

```bash
# Pipeline completo: scraping + procesamiento
python main.py --query "Dentistas en Medellín"

# Solo scraping
python main.py --scrape-only --query "Abogados en Bogotá"

# Solo procesamiento
python main.py --process-only

# Browser visible (útil para debug)
python main.py --no-headless --query "Veterinarios en Barranquilla"

# Controlar máximo de scrolls
python main.py --max-scroll-attempts 10 --query "Restaurantes en Cali"
```

### Flags CLI

| Flag | Descripción |
|------|-------------|
| `--query, -q` | Término de búsqueda en Google Maps |
| `--scrape-only` | Solo ejecutar el scraper (Producer) |
| `--process-only` | Solo procesar leads pendientes (Consumer) |
| `--actions, -a` | Acciones: `FilterInvalidPhone`, `FilterNoWhatsApp` |
| `--no-headless` | Mostrar el browser durante el scraping |
| `--max-scroll-attempts` | Solo Playwright: intentos de scroll en el feed lateral (`--max-scrolls` sigue siendo alias) |

### Migraciones (Alembic)

```bash
# Aplicar migraciones a la DB configurada en DATABASE_URL
alembic -c backend/alembic.ini upgrade head

# Crear nueva migración tras cambios en models.py
alembic -c backend/alembic.ini revision --autogenerate -m "descripcion"
```

## Estrategias de filtrado disponibles

| Estrategia | Descripción |
|-----------|-------------|
| `FilterInvalidPhone` | Filtra negocios sin teléfono celular colombiano válido |
| `FilterNoWhatsApp` | Verifica si el número tiene WhatsApp activo (requiere WA API) |

Para agregar una nueva estrategia, crea una clase en `backend/app/services/strategies.py`:

```python
class MiNuevaAccion(Action):
    @property
    def name(self) -> str:
        return "Mi Acción"

    async def execute(self, business: Business) -> tuple[bool, str | None]:
        # True = pasa el filtro, False = filtrar con razón
        return True, None

# Registrar
AVAILABLE_STRATEGIES["MiNuevaAccion"] = MiNuevaAccion
```

## Tests

```bash
python -m pytest tests/ -v
```

## ⚠️ Notas

- **Google Maps** puede cambiar sus selectores CSS. Si el scraper falla, revisa los selectores en `backend/app/services/scraper.py`.
- **Rate limiting**: ajusta `SCROLL_PAUSE_SECONDS` y `MAX_SCROLL_ATTEMPTS` en `.env` para evitar bloqueos.
- **Solo para fines educativos**. Verifica los ToS de Google Maps antes de usar en producción.
