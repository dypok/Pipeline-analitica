"""Barra lateral con filtros dinámicos (US-14).

Filtros:
    - Rango de fechas (selector de calendario) -> afecta 'Visión General de Ventas'.
    - Multiselect de países -> afecta 'Análisis de Clientes' (origen: ``customer_360.country``).
    - Multiselect de categorías -> afecta 'Visión General de Ventas' (vía
      ``daily_sales.top_category``) y 'Rendimiento de Productos' (vía
      ``product_performance.category``).

Los filtros son tolerantes: si las columnas no existen en el modelo Gold,
se omiten con un aviso en la UI sin romper el dashboard. Esto permite que el
dashboard funcione con cualquier esquema Gold (solo ``daily_sales_kpis`` o
también ``customer_360``/``product_performance`` publicados por Esteban).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

import pandas as pd
import streamlit as st


def _collect_categories(
    daily_sales: pd.DataFrame,
    product_performance: Optional[pd.DataFrame],
) -> list[str]:
    """Une las categorías de ``daily_sales.top_category`` y ``product_performance.category``."""
    cat_sets: list[set] = []
    if not daily_sales.empty and "top_category" in daily_sales.columns:
        cat_sets.append(set(daily_sales["top_category"].dropna().unique().tolist()))
    if product_performance is not None and not product_performance.empty and "category" in product_performance.columns:
        cat_sets.append(set(product_performance["category"].dropna().unique().tolist()))
    if not cat_sets:
        return []
    union = set().union(*cat_sets)
    return sorted(union)


def render_sidebar_filters(
    daily_sales: pd.DataFrame,
    customer_360: Optional[pd.DataFrame] = None,
    product_performance: Optional[pd.DataFrame] = None,
) -> dict:
    """Renderiza los filtros en el sidebar y retorna las selecciones.

    Returns:
        Diccionario con ``date_range``, ``countries`` y ``categories``.
        Cada entrada puede ser ``None`` si la dimensión no está disponible.
    """
    st.sidebar.title("🔎 Filtros del Dashboard")
    st.sidebar.markdown("Ajusta los criterios para recalcular todos los KPIs.")

    selection: dict = {}

    # --- Rango de fechas (afecta: Visión General de Ventas) ----------------
    if not daily_sales.empty and "kpi_date" in daily_sales.columns:
        min_date = daily_sales["kpi_date"].min().date()
        max_date = daily_sales["kpi_date"].max().date()
        default_start = max_date - timedelta(days=30)
        selection["date_range"] = st.sidebar.date_input(
            "Rango de fechas  *(Visión General)*",
            value=(max(min_date, default_start), max_date),
            min_value=min_date,
            max_value=max_date,
        )
    else:
        st.sidebar.info("Sin datos de fechas disponibles para filtrar.")
        selection["date_range"] = None

    # --- Filtro por países (afecta: Análisis de Clientes) ------------------
    # Las países provienen de ``customer_360.country`` (no de ``daily_sales``,
    # que no expone la dimensión geográfica en el contrato vinculante).
    countries_options: list[str] = []
    if customer_360 is not None and not customer_360.empty and "country" in customer_360.columns:
        countries_options = sorted(customer_360["country"].dropna().unique().tolist())

    if countries_options:
        selection["countries"] = st.sidebar.multiselect(
            "Países  *(Análisis de Clientes)*",
            options=countries_options,
            default=countries_options,
        )
    else:
        selection["countries"] = None
        st.sidebar.caption(
            "_Filtro de países deshabilitado: ``customer_360`` no disponible "
            "o sin columna ``country`` (pendiente US-07, Esteban Padilla)._"
        )

    # --- Filtro por categorías (afecta: Ventas + Productos) -----------------
    categories_options = _collect_categories(daily_sales, product_performance)
    if categories_options:
        selection["categories"] = st.sidebar.multiselect(
            "Categorías  *(Ventas + Productos)*",
            options=categories_options,
            default=categories_options,
        )
    else:
        selection["categories"] = None
        st.sidebar.caption(
            "_Filtro de categorías deshabilitado: ni ``daily_sales.top_category`` "
            "ni ``product_performance.category`` están disponibles._"
        )

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Los filtros se aplican por pestaña según las dimensiones que cada "
        "modelo Gold expone. ``daily_sales`` no incluye país por contrato "
        "(``docs/contratos_integracion.md`` §4.1)."
    )
    return selection
