"""Barra lateral con filtros dinámicos premium (US-14).

Filtros:
    - Rango de fechas con **presets rápidos** (7d, 30d, 90d, Año) -> 'Ventas'.
    - Multiselect de países -> 'Análisis de Clientes' (``customer_360.country``).
    - Multiselect de categorías -> 'Ventas' + 'Productos' (unión de fuentes).

Los filtros son tolerantes al esquema (aceptan ``kpi_date``/``order_date``,
``top_category``/``category``) y se omiten con aviso si la columna no existe.
El dict retornado conserva las claves históricas (``date_range``,
``countries``, ``categories``) y añade ``preset`` para diagnóstico en la UI.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

# Selector de periodo rápido (especificación de diseño D):
#   etiqueta -> días (None = año en curso).
# El mock actual es de granularidad DIARIA, por lo que "1h" y "24h" se
# resuelven al último día disponible. Con datos reales horarios en Gold,
# estos periodos cortos mostrarían el tramo correspondiente.
_PERIODS = [
    ("1 h", 1),
    ("24 h", 1),
    ("7 días", 7),
    ("30 días", 30),
    ("Año", None),
]
# Compatibilidad con el resto del código que usa el nombre histórico.
_PRESETS = _PERIODS


def _resolve_preset(preset_label: str, max_date: date) -> tuple[date, date]:
    """Convierte un periodo en un rango (inicio, fin) concreto."""
    days = dict((lbl, d) for lbl, d in _PERIODS)[preset_label]
    if days is None:  # Año en curso
        start = date(max_date.year, 1, 1)
        return start, max_date
    return max_date - timedelta(days=days - 1), max_date


def _collect_categories(
    daily_sales: pd.DataFrame,
    product_performance: Optional[pd.DataFrame],
) -> list[str]:
    """Une las categorías de ``daily_sales`` y ``product_performance``.

    Tolerante al esquema: acepta ``top_category`` (data mart §4.1) o
    ``category`` (Gold US-07) en ``daily_sales``.
    """
    cat_sets: list[set] = []
    if not daily_sales.empty:
        col = "top_category" if "top_category" in daily_sales.columns else (
            "category" if "category" in daily_sales.columns else None
        )
        if col:
            cat_sets.append(set(daily_sales[col].dropna().unique().tolist()))
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
        Diccionario con ``date_range``, ``countries``, ``categories`` y
        ``preset``. Cada dimensión puede ser ``None`` si no está disponible.
    """
    st.sidebar.markdown("### Filtros")

    selection: dict = {"preset": None}

    # --- Rango de fechas con presets rápidos (afecta: Ventas) --------------
    date_col = (
        "kpi_date" if "kpi_date" in daily_sales.columns
        else ("order_date" if "order_date" in daily_sales.columns else None)
    )
    if not daily_sales.empty and date_col:
        dates = pd.to_datetime(daily_sales[date_col])
        min_date = dates.min().date()
        max_date = dates.max().date()
        default_start = max(min_date, max_date - timedelta(days=30))

        # Presets rápidos como botones en una fila.
        st.sidebar.caption("Periodo rápido")
        preset_cols = st.sidebar.columns(len(_PERIODS))
        chosen_preset = st.session_state.get("date_preset", "30 días")
        for c, (label, _days) in zip(preset_cols, _PERIODS):
            if c.button(label, key=f"preset_{label}", use_container_width=True):
                chosen_preset = label
                st.session_state["date_preset"] = label
        selection["preset"] = chosen_preset

        # Resuelve el rango por defecto desde el preset activo.
        preset_start, preset_end = _resolve_preset(chosen_preset, max_date)
        start_default = max(min_date, preset_start)

        selection["date_range"] = st.sidebar.date_input(
            "Rango de fechas  *(Ventas)*",
            value=(start_default, min(max_date, preset_end)),
            min_value=min_date,
            max_value=max_date,
            label_visibility="collapsed",
        )
    else:
        st.sidebar.info("Sin datos de fechas disponibles.")
        selection["date_range"] = None

    st.sidebar.markdown("---")

    # --- Filtro por países (afecta: Análisis de Clientes) ------------------
    countries_options: list[str] = []
    if customer_360 is not None and not customer_360.empty and "country" in customer_360.columns:
        countries_options = sorted(customer_360["country"].dropna().unique().tolist())

    if countries_options:
        selection["countries"] = st.sidebar.multiselect(
            "Países  *(Clientes)*",
            options=countries_options,
            default=countries_options,
        )
    else:
        selection["countries"] = None
        st.sidebar.caption(
            "_Países: ``customer_360`` no disponible o sin ``country`` (US-07)._"
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
            "_Categorías: ni ``daily_sales`` ni ``product_performance`` las exponen._"
        )

    return selection
