"""Punto de entrada de la aplicación Streamlit (US-14 y US-15).

Ejecutar con::

    streamlit run dashboards/main.py

La aplicación consume los modelos analíticos de la capa Gold a través de
``dashboards.data_loader.GoldDataLoader`` (con degradación graceful entre
PostgreSQL, MinIO y Parquet local) y orquesta cinco pestañas:

    1. 📈 Visión General de Ventas
    2. 👥 Análisis de Clientes
    3. 📦 Rendimiento de Productos
    4. 🤖 Asistente IA          (US-15)
    5. 🩺 Salud del Pipeline    (US-15)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Asegurar que la raíz del proyecto esté en ``sys.path`` para que los imports
# absolutos ``from dashboards.xxx import yyy`` funcionen al ejecutar
# ``streamlit run dashboards/main.py``. Streamlit agrega al ``sys.path`` el
# directorio del script (``dashboards/``), no la raíz del proyecto, lo que
# rompería los imports absolutos sin este fix.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st

from dashboards.data_loader import get_loader
from dashboards.components.filters import render_sidebar_filters
from dashboards.views import (
    render_chatbot_assistant,
    render_customer_analysis,
    render_pipeline_health,
    render_product_performance,
    render_sales_overview,
)


def _source_label(loader) -> str | None:
    if loader.source is None:
        return None
    return f"{loader.source.backend} :: {loader.source.detail}"


def main() -> None:
    """Configura la página, carga los datos y renderiza las pestañas."""
    st.set_page_config(
        page_title="Pipeline Analítica — Dashboards",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("📊 Plataforma Extensible de Datos — Pipeline Analítica")
    st.caption("Dashboards interactivos sobre la capa Gold (Arquitectura Medallion)")

    # --- Carga de datos (cacheada por sesión de Streamlit) -----------------
    @st.cache_data(ttl=60, show_spinner=False)
    def _load_all():
        loader = get_loader()
        return {
            "daily_sales": loader.load_daily_sales(),
            "customer_360": loader.load_customer_360(),
            "product_performance": loader.load_product_performance(),
            "executions": loader.load_pipeline_executions(),
            "task_metrics": loader.load_task_metrics(),
            "quarantine": loader.load_quarantine_records(),
            "source": _source_label(loader),
        }

    data = _load_all()
    source_label = data["source"]
    if source_label is None:
        st.error(
            "⚠️ No se encontraron datos en ninguna fuente disponible (PostgreSQL, "
            "MinIO ni Parquet local). Verifica que: (1) los servicios estén "
            "levantados con `docker compose up -d`, o (2) los Parquet mock estén "
            "en `data/mock/gold/` (Luis Gómez)."
        )
    else:
        st.success(f"Datos cargados desde: `{source_label}`")

    # --- Sidebar de filtros (afecta a las pestañas analíticas) -------------
    filters = render_sidebar_filters(data["daily_sales"])

    # --- Pestañas de navegación -------------------------------------------
    tabs = st.tabs(
        [
            "📈 Visión General de Ventas",
            "👥 Análisis de Clientes",
            "📦 Rendimiento de Productos",
            "🤖 Asistente IA",
            "🩺 Salud del Pipeline",
        ]
    )

    with tabs[0]:
        render_sales_overview(data["daily_sales"], filters, source_label)
    with tabs[1]:
        render_customer_analysis(data["customer_360"], filters, source_label)
    with tabs[2]:
        render_product_performance(data["product_performance"], filters, source_label)
    with tabs[3]:
        render_chatbot_assistant()
    with tabs[4]:
        render_pipeline_health(
            data["executions"],
            data["task_metrics"],
            data["quarantine"],
            source_label,
        )


if __name__ == "__main__":
    main()
