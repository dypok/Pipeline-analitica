"""Punto de entrada del dashboard Streamlit (US-14 y US-15).

Ejecutar desde la raíz del proyecto::

    streamlit run dashboards/main.py

Rediseño "Oscuro Premium" con:
    - **Navegación lateral** real (``st.navigation`` / ``st.Page``) en lugar de
      pestañas: cada vista es una "página" con su propio icono.
    - Tema oscuro (``.streamlit/config.toml``) + CSS propio (``styles``).
    - Filtros con **presets rápidos** de fecha y **control de refresco** en el
      sidebar, compartidos por todas las páginas analíticas.
    - Carga de datos cacheada (``st.cache_data``) con degradación graceful
      entre PostgreSQL, MinIO y Parquet local (ver ``data_loader``).

Los datos y filtros se guardan en ``st.session_state`` para que las funciones
de página (que se ejecutan en aislamiento) accedan al estado compartido.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Raíz del proyecto en ``sys.path`` para imports absolutos al ejecutar
# ``streamlit run dashboards/main.py``.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st

from dashboards.components.filters import render_sidebar_filters
from dashboards.components.service_status import render_service_indicators
from dashboards.components.status import mark_loaded, render_refresh_control
from dashboards.data_loader import get_loader
from dashboards.styles import apply_custom_css
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


@st.cache_data(ttl=60, show_spinner="Cargando capa Gold…")
def _load_all() -> dict:
    """Carga (cacheada) los modelos Gold + estado operacional del pipeline."""
    loader = get_loader()
    data = {
        "daily_sales": loader.load_daily_sales(),
        "customer_360": loader.load_customer_360(),
        "product_performance": loader.load_product_performance(),
        "executions": loader.load_pipeline_executions(),
        "task_metrics": loader.load_task_metrics(),
        "quarantine": loader.load_quarantine_records(),
        "source": _source_label(loader),
    }
    return data


def _page_sales() -> None:
    d = st.session_state["data"]
    render_sales_overview(d["daily_sales"], st.session_state["filters"], d["source"])


def _page_customers() -> None:
    d = st.session_state["data"]
    render_customer_analysis(d["customer_360"], st.session_state["filters"], d["source"])


def _page_products() -> None:
    d = st.session_state["data"]
    render_product_performance(d["product_performance"], st.session_state["filters"], d["source"])


def _page_chatbot() -> None:
    d = st.session_state["data"]
    render_chatbot_assistant()


def _page_health() -> None:
    d = st.session_state["data"]
    render_pipeline_health(d["executions"], d["task_metrics"], d["quarantine"], d["source"])


def main() -> None:
    st.set_page_config(
        page_title="DataOps Monitor",
        page_icon="◈",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_custom_css()

    # --- Carga de datos (cacheada) y estado compartido ----------------------
    data = _load_all()
    st.session_state["data"] = data
    mark_loaded()

    # --- Sidebar: logo + navegación + servicios + filtros ------------------
    # Logo DataOps Monitor con icono geométrico (anillo cian con brillo).
    st.sidebar.markdown(
        """
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px;">
            <div style="width:40px;height:40px;border-radius:50%;
                        background:radial-gradient(circle at 50% 50%, rgba(34,211,238,0.25), rgba(3,6,14,0.9));
                        border:2px solid #22D3EE;
                        display:flex;align-items:center;justify-content:center;
                        font-size:17px;color:#00E5FF;font-weight:800;
                        box-shadow:0 0 18px rgba(34,211,238,0.5);">◉</div>
            <div>
                <div style="font-size:1.15rem;font-weight:800;color:#E8F6FF;line-height:1.05;letter-spacing:-0.02em;">
                    DataOps
                </div>
                <div style="font-size:0.68rem;color:#22D3EE;letter-spacing:0.24em;font-weight:600;">
                    MONITOR
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.sidebar.markdown('<div class="menu-title">Navegación</div>', unsafe_allow_html=True)

    source = data["source"]
    st.sidebar.caption(
        f"Fuente: `{source}`" if source else "Sin datos Gold disponibles"
    )
    st.sidebar.markdown("")

    filters = render_sidebar_filters(
        data["daily_sales"], data["customer_360"], data["product_performance"]
    )
    st.session_state["filters"] = filters

    # Indicadores de servicios (PostgreSQL, MinIO, Spark, Airflow).
    render_service_indicators()

    render_refresh_control(_load_all)

    # --- Navegación lateral -------------------------------------------------
    nav = st.navigation(
        [
            st.Page(_page_sales, title="Ventas", icon=":material/insights:", default=True),
            st.Page(_page_customers, title="Clientes", icon=":material/groups:"),
            st.Page(_page_products, title="Productos", icon=":material/inventory_2:"),
            st.Page(_page_chatbot, title="Asistente IA", icon=":material/smart_toy:"),
            st.Page(_page_health, title="Salud Pipeline", icon=":material/monitor_heart:"),
        ],
        position="sidebar",
        expanded=True,
    )
    nav.run()


if __name__ == "__main__":
    main()
