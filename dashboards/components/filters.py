"""Barra lateral con filtros dinámicos (US-14).

Filtros:
    - Rango de fechas (selector de calendario).
    - Multiselect de países (cuando el modelo Gold exponga ``country``).
    - Multiselect de categorías (cuando el modelo Gold exponga ``category``).

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
    if not daily_sales.empty and "kpi_date" in daily_sales.columns:
        min_date = daily_sales["kpi_date"].min().date()
        max_date = daily_sales["kpi_date"].max().date()
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

    # --- Filtro por países (pendiente dimensión en Gold) -------------------
    if "country" in daily_sales.columns:
        countries = sorted(daily_sales["country"].dropna().unique().tolist())
        selection["countries"] = st.sidebar.multiselect(
            "Países",
            options=countries,
            default=countries,
        )
    else:
        selection["countries"] = None
        # TODO: habilitar este filtro cuando el modelo Gold incluya ``shipping_country``
        # (ver US-07 y ``docs/contratos_integracion.md`` §3.1, columna de Silver).

    # --- Filtro por categorías (pendiente dimensión en Gold) ----------------
    if "category" in daily_sales.columns:
        categories = sorted(daily_sales["category"].dropna().unique().tolist())
        selection["categories"] = st.sidebar.multiselect(
            "Categorías de producto",
            options=categories,
            default=categories,
        )
    elif "top_category" in daily_sales.columns:
        categories = sorted(daily_sales["top_category"].dropna().unique().tolist())
        selection["categories"] = st.sidebar.multiselect(
            "Categorías (top por día)",
            options=categories,
            default=categories,
        )
    else:
        selection["categories"] = None

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Filtros adicionales (país/categoría) se habilitarán en cuanto el "
        "modelo Gold exponga esas dimensiones (US-07, Esteban Padilla)."
    )
    return selection
