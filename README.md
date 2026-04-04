# 🕷️ scraping-job-ms

**Sistema modular de scraping de Google Maps** para detectar negocios sin sitio web, generar leads cualificados y ejecutar acciones de contacto.

## Arquitectura

```
Producer (Scraper)  →  SQLite Queue  →  Consumer (Processor)  →  Strategy (Action)
     ↑                     ↑                                          ↓
  Playwright           queue.db                                 CSV / Log / Email
```

- **Producer-Consumer** desacoplado con SQLite como cola de tareas
- **Strategy Pattern** para acciones extensibles sobre leads
- **Async** de punta a punta (Playwright + aiohttp + aiosqlite)
- **Dashboard Streamlit** como centro de mando visual

## Setup

### Requisitos previos
- Python 3.10+

### Instalación

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/scraping-job-ms.git
cd scraping-job-ms

# Crear entorno virtual e instalar dependencias
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Para desarrollo (incluye pytest)
pip install -r requirements-dev.txt

# Instalar browsers de Playwright
playwright install chromium

# Copiar variables de entorno
cp .env.example .env
```

## Uso

> **Nota:** Asegúrate de activar el entorno virtual antes de ejecutar: `source .venv/bin/activate`

### 🖥️ Dashboard (GUI)

```bash
streamlit run dashboard.py
```

El dashboard permite:
- Ingresar búsquedas y seleccionar estrategias
- Ejecutar scraping y procesamiento con logs en tiempo real
- Visualizar KPIs (total, leads, tasa de conversión)
- Explorar y exportar datos en tabla interactiva

### ⌨️ CLI

```bash
# Pipeline completo
python main.py --query "Dentistas en Medellín"

# Solo scraping
python main.py --scrape-only --query "Abogados en Bogotá"

# Solo procesamiento de leads pendientes
python main.py --process-only

# Cambiar estrategia
python main.py --strategy SaveToCSV --query "Restaurantes en Cali"

# Browser visible (no headless)
python main.py --no-headless --query "Veterinarios en Barranquilla"
```

### Flags disponibles

| Flag | Descripción |
|------|-------------|
| `--query, -q` | Término de búsqueda |
| `--scrape-only` | Solo ejecutar el scraper |
| `--process-only` | Solo procesar leads pendientes |
| `--strategy, -s` | Estrategia: `OfferWebDev`, `SaveToCSV`, `ConsoleLog` |
| `--no-headless` | Mostrar el browser |
| `--max-scrolls` | Límite de scrolls en Maps |

## Tests

```bash
python -m pytest tests/ -v
```

## Estructura del Proyecto

```
scraping-job-ms/
├── pyproject.toml          # Dependencias (Poetry)
├── main.py                 # Orquestador CLI
├── dashboard.py            # Dashboard Streamlit
├── config/
│   └── settings.py         # Configuración centralizada
├── src/
│   ├── models.py           # Business, BusinessStatus
│   ├── queue_manager.py    # Cola SQLite async
│   ├── scraper.py          # Producer (Playwright)
│   ├── processor.py        # Consumer (aiohttp)
│   ├── strategies.py       # Strategy pattern
│   └── utils.py            # Logger, helpers
├── output/                 # CSVs generados
└── tests/                  # Tests unitarios
```

## Estrategias Disponibles

| Estrategia | Descripción |
|-----------|-------------|
| `OfferWebDev` | Log + CSV con oferta de servicio web |
| `SaveToCSV` | Solo guarda leads en CSV |
| `ConsoleLog` | Imprime a consola (debug) |

Para agregar una nueva estrategia:

```python
# En src/strategies.py
class SendEmailStrategy(ActionStrategy):
    @property
    def name(self) -> str:
        return "Enviar Email"

    async def execute(self, business: Business) -> None:
        # Tu lógica de envío de email aquí
        ...

# Registrar en AVAILABLE_STRATEGIES
AVAILABLE_STRATEGIES["SendEmail"] = SendEmailStrategy
```

## ⚠️ Notas Importantes

- **Google Maps** puede cambiar sus selectores CSS. Si el scraper falla, revisa los selectores en `src/scraper.py`.
- **Rate limiting**: Google puede bloquear scraping agresivo. Ajusta `SCROLL_PAUSE_SECONDS` y `MAX_SCROLL_ATTEMPTS` en `.env`.
- **Solo para fines educativos**. Verifica los ToS de Google Maps antes de usar en producción.
