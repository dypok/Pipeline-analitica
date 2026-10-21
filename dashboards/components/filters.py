"""Barra lateral con filtros dinámicos (US-14).

Filtros:
    - Rango de fechas (selector de calendario).
    - Multiselect de países (soporta tanto ``country`` como ``shipping_country``).
    - Multiselect de categorías (soporta tanto ``category`` como ``top_category``).

Los filtros son tolerantes: si las columnas no existen aún en el modelo Gold,
se omiten con un aviso en la UI sin romper el dashboard.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st


def render_sidebar_filters(
    daily_sales: pd.DataFrame,
    customer_360: Optional[pd.DataFrame] = None,
    product_performance: Optional[pd.DataFrame] = None,
) -> dict:
    """Renderiza los filtros en el sidebar y retorna las selecciones.

    Returns:
        Diccionario con ``date_range``, ``countries`` y ``categories``.
    """
    st.sidebar.title("🔎 Filtros del Dashboard")
    st.sidebar.markdown("Ajusta los criterios para recalcular todos los KPIs.")

    selection: dict = {}

    # --- Rango de fechas ---------------------------------------------------
    date_col = (
        "kpi_date"
        if "kpi_date" in daily_sales.columns
        else ("order_date" if "order_date" in daily_sales.columns else None)
    )
    if not daily_sales.empty and date_col:
        dates = pd.to_datetime(daily_sales[date_col])
        min_date = dates.min().date()
        max_date = dates.max().date()
        default_start = max_date - timedelta(days=30)
        selection["date_range"] = st.sidebar.date_input(
            "Rango de fechas",
            value=(max(min_date, default_start), max_date),
            min_value=min_date,
            max_value=max_date,
        )
    else:
        st.sidebar.info("Sin datos de fechas disponibles para filtrar.")
        selection["date_range"] = None

    # --- Filtro por países -------------------------------------------------
    country_col = (
        "country"
        if "country" in daily_sales.columns
        else ("shipping_country" if "shipping_country" in daily_sales.columns else None)
    )
    if country_col:
        countries = sorted(daily_sales[country_col].dropna().unique().tolist())
        selection["countries"] = st.sidebar.multiselect(
            "Países",
            options=countries,
            default=countries,
        )
    else:
        selection["countries"] = None

    # --- Filtro por categorías ---------------------------------------------
    cat_col = (
        "category"
        if "category" in daily_sales.columns
        else ("top_category" if "top_category" in daily_sales.columns else None)
    )
    if cat_col:
        categories = sorted(daily_sales[cat_col].dropna().unique().tolist())
        selection["categories"] = st.sidebar.multiselect(
            "Categorías de producto",
            options=categories,
            default=categories,
        )
    else:
        selection["categories"] = None

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Filtros sincronizados con la capa Gold del Pipeline Analítica."
    )
    return selection
