"""Pestaña 'Visión General de Ventas' (US-14).

Muestra:
    - Tarjetas con KPIs principales (Total Ingresos, Total Pedidos,
      Ticket Promedio, Tasa de Devolución, Clientes Activos).
    - Gráfico de líneas interactivo de tendencia temporal (Plotly).
    - Gráfico de barras por categoría.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.components.kpi_cards import render_kpi_cards
from dashboards.kpi_calculator import (
    compute_global_kpis,
    compute_revenue_trend,
    compute_top_categories,
    filter_by_selections,
)


def render_sales_overview(
    daily_sales: pd.DataFrame,
    filters: dict,
    source_label: str | None = None,
) -> None:
    """Renderiza la pestaña 'Visión General de Ventas'."""
    st.header("📈 Visión General de Ventas")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    filtered = filter_by_selections(
        daily_sales,
        date_range=filters.get("date_range"),
        countries=filters.get("countries"),
        categories=filters.get("categories"),
    )

    if filtered.empty:
        st.warning(
            "No hay datos de ventas disponibles para los filtros actuales. "
            "Verifica que la capa Gold esté publicada (Esteban, US-07) o que "
            "los Parquet mock estén en `data/mock/gold/` (Luis)."
        )
        return

    # --- KPIs --------------------------------------------------------------
    kpis = compute_global_kpis(filtered)
    render_kpi_cards(kpis)

    st.markdown("---")

    # --- Tendencia temporal -----------------------------------------------
    trend = compute_revenue_trend(filtered)
    if not trend.empty:
        fig_trend = px.line(
            trend,
            x="kpi_date",
            y="total_revenue",
            title="Tendencia de Ingresos Diarios",
            labels={"kpi_date": "Fecha", "total_revenue": "Ingresos (USD)"},
            template="plotly_white",
        )
        fig_trend.update_traces(mode="lines+markers")
        st.plotly_chart(fig_trend, width="stretch")

    # --- Barras por categoría ---------------------------------------------
    cat_df = compute_top_categories(filtered, top_n=10)
    if not cat_df.empty:
        fig_bar = px.bar(
            cat_df,
            x="completed_orders",
            y="top_category",
            orientation="h",
            title="Top 10 Categorías por Órdenes Completadas",
            labels={"completed_orders": "Órdenes completadas", "top_category": "Categoría"},
            template="plotly_white",
        )
        fig_bar.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_bar, width="stretch")
    else:
        st.info(
            "Las categorías se mostrarán en cuanto el modelo Gold incluya la "
            "dimensión `category` (US-07). Hoy solo disponemos de `top_category`."
        )
