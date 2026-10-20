# Dashboards y Capa de Presentación

Módulo de visualización interactiva del proyecto **Pipeline Analítica** (Arquitectura Medallion), desarrollado por **Sergio** en la rama `feature/dashboard`.

## Historias de usuario cubiertas

- **US-14** — Dashboard Interactivo de Negocio (Retail KPIs y Filtros) en Streamlit.
- **US-15** — Interfaz Integrada para el Chatbot Conversacional y Monitor de Calidad.

## Cómo ejecutar

```bash
# 1. Entorno virtual y dependencias (incluye streamlit y plotly)
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

# 2. Variables de entorno
cp .env.example .env
# Ajusta BACKEND_API_URL y DASHBOARDS_GOLD_PATH si es necesario

# 3. Levantar el dashboard (puerto 8501)
streamlit run dashboards/main.py
```

> Si además vas a probar el Asistente IA, levanta también el backend FastAPI de Dylan
> (rama `feature/chatbot-ia`) en el puerto 8000 para que el endpoint
> `/api/v1/chatbot/ask` responda.

## Estructura

```
dashboards/
├── __init__.py
├── main.py                 # Punto de entrada de Streamlit (pestañas)
├── data_loader.py          # Carga Gold con degradación graceful (Postgres → MinIO → local)
├── chatbot_client.py       # Cliente HTTP para /api/v1/chatbot/ask
├── kpi_calculator.py       # Cálculo de KPIs sobre los modelos Gold
├── components/
│   ├── __init__.py
│   ├── filters.py          # Sidebar de filtros (fechas, países, categorías)
│   └── kpi_cards.py        # Tarjetas de KPIs reutilizables
└── views/
    ├── __init__.py
    ├── sales_overview.py        # Pestaña: Visión General de Ventas
    ├── customer_analysis.py     # Pestaña: Análisis de Clientes
    ├── product_performance.py   # Pestaña: Rendimiento de Productos
    ├── chatbot_assistant.py     # Pestaña: Asistente IA (US-15)
    └── pipeline_health.py       # Pestaña: Salud del Pipeline (US-15)
```

## Estrategia de datos (degradación graceful)

El `GoldDataLoader` intenta, en orden:

1. **PostgreSQL analítico** (`analytics_db`, esquema `analytics`) — recomendado en
   producción.
2. **DuckDB sobre Parquet en MinIO** (`s3://gold/analytics/<modelo>/*.parquet`).
3. **DuckDB/Parquet local** desde `DASHBOARDS_GOLD_PATH` (mock de Luis Gómez).

Esto permite maquetar desde el día 1 con los Parquet mock y cambiar a
producción sin tocar la UI.

## Dependencias externas

| Dependencia | Proveedor | Estado en `develop` |
| :--- | :--- | :--- |
| `analytics.daily_sales_kpis` (PostgreSQL) | Esteban Padilla / Luis Gómez | Disponible cuando corre el pipeline local |
| `data/mock/gold/*.parquet` | Luis Gómez | Pendiente (carpeta vacía) |
| `analytics.customer_360` y `analytics.product_performance` | Esteban Padilla (US-07) | Pendiente |
| Endpoint `/api/v1/chatbot/ask` (FastAPI) | Dylan Gamero (US-13) | En rama `feature/chatbot-ia` |
| Tabla/Parquet de Quarantine | Sebastián Ortiz (US-08) | Pendiente |

## Pruebas de aceptación asociadas

- **P06**: `SUM(total_revenue)` en Gold cuadra con los registros auditados (junto con Esteban).
- **P07**: KPIs del dashboard coinciden con consulta SQL directa sobre `daily_sales`.
- **P08 & P09**: Chatbot responde con cifras reales respaldadas por SQL, sin alucinar.
