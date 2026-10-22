"""Página 'Ventas' — tema Data Observability (cian neón).

Contenido:
    - Tarjetas KPI con icono + valor + **pill de variación** (periodo vs anterior).
    - "Sales Overview": **áreas rellenas** mensuales (año actual vs anterior) en
      degradado cian, estilo observabilidad.
    - "Órdenes por tiempo": heatmap día de la semana × mes (escala cian).
    - Descarga del filtrado.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboards.components.download import render_download_button
from dashboards.components.kpi_cards import render_kpi_cards
from dashboards.kpi_calculator import (
    compute_global_kpis,
    filter_by_selections,
)
from dashboards.styles import PLOTLY_CONFIG, PLOTLY_TEMPLATE, area_fill

# Escala cian para el heatmap (10 pasos: casi negro -> cian brillante).
HEAT_SCALE = [
    "#071019", "#0A2233", "#0D3447", "#10455C", "#135670",
    "#17708C", "#1B92A8", "#1FB6C4", "#22D3EE", "#7FF0FF",
]

_DIAS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _apply_template(fig):
    return fig.update_layout(template=PLOTLY_TEMPLATE)


def _date_col(df: pd.DataFrame) -> str:
    return "kpi_date" if "kpi_date" in df.columns else "order_date"


def _orders_col(df: pd.DataFrame) -> str:
    return "total_orders" if "total_orders" in df.columns else "orders_count"


def _compute_deltas(base: pd.DataFrame, filters: dict) -> dict:
    """Variación porcentual del rango activo contra el periodo inmediato anterior."""
    date_col = _date_col(base)
    dr = filters.get("date_range")
    if not dr or len(dr) != 2 or date_col not in base.columns:
        return {}
    start, end = pd.to_datetime(dr[0]), pd.to_datetime(dr[1])
    span = (end - start).days
    prev_end = start - pd.Timedelta(days=1)
    prev_start = prev_end - pd.Timedelta(days=span)

    cur = filter_by_selections(base, date_range=(start.date(), end.date()),
                               countries=filters.get("countries"),
                               categories=filters.get("categories"))
    prev = filter_by_selections(base, date_range=(prev_start.date(), prev_end.date()),
                                countries=filters.get("countries"),
                                categories=filters.get("categories"))
    if cur.empty or prev.empty:
        return {}

    k_cur, k_prev = compute_global_kpis(cur), compute_global_kpis(prev)

    def pct(a, b):
        return None if not b else (a - b) / b * 100

    out = {}
    mapping = {
        "total_revenue": pct(k_cur["total_revenue"], k_prev["total_revenue"]),
        "total_orders": pct(k_cur["total_orders"], k_prev["total_orders"]),
        "average_ticket": pct(k_cur["average_ticket"], k_prev["average_ticket"]),
        "distinct_customers": pct(k_cur["distinct_customers"], k_prev["distinct_customers"]),
    }
    for key, p in mapping.items():
        if p is None:
            continue
        direction = "ok" if p >= 0 else "down"
        out[key] = (f"{abs(p):.1f}%", direction)
    return out


def render_sales_overview(
    daily_sales: pd.DataFrame,
    filters: dict,
    source_label: str | None = None,
) -> None:
    """Renderiza la página 'Ventas'."""
    st.header("Visión General de Ventas")
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
            "No hay datos de ventas para los filtros actuales. Verifica que la "
            "capa Gold esté publicada (Esteban, US-07) o que los Parquet mock "
            "estén en `data/mock/gold/`."
        )
        return

    # --- KPIs con pill de variación -----------------------------------------
    kpis = compute_global_kpis(filtered)
    deltas = _compute_deltas(daily_sales, filters)
    render_kpi_cards(
        kpis,
        columns=["total_revenue", "total_orders", "average_ticket", "distinct_customers"],
        deltas=deltas,
    )

    # --- Barra de acciones (descarga) ---------------------------------------
    st.markdown("")
    a1, a2 = st.columns([4, 1])
    with a2:
        render_download_button(filtered, "Descargar CSV", "daily_sales", key="dl_sales")

    # --- Sales Overview: barras agrupadas por mes (año actual vs anterior) ---
    dc = _date_col(filtered)
    oc = _orders_col(filtered)
    tmp = filtered.copy()
    tmp[dc] = pd.to_datetime(tmp[dc])
    tmp["year"] = tmp[dc].dt.year
    tmp["month"] = tmp[dc].dt.month

    monthly = (
        tmp.groupby(["year", "month"], as_index=False)[oc].sum()
    )
    if len(monthly) > 0:
        years = sorted(monthly["year"].unique())
        current_year = years[-1]
        compare_year = years[-2] if len(years) > 1 else None

        cur = monthly[monthly["year"] == current_year].set_index("month")[oc]
        plot_df = pd.DataFrame({"month": range(1, 13)})
        plot_df["Mes"] = plot_df["month"].map(
            {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
             7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
        )
        plot_df[str(current_year)] = plot_df["month"].map(cur).fillna(0)
        if compare_year is not None:
            prev = monthly[monthly["year"] == compare_year].set_index("month")[oc]
            plot_df[str(compare_year)] = plot_df["month"].map(prev).fillna(0)

        cols = [c for c in plot_df.columns if c not in ("month", "Mes")]
        # Áreas rellenas con degradado cian (estilo observabilidad).
        fig = go.Figure()
        for i, col in enumerate(cols):
            fig.add_trace(
                go.Scatter(
                    x=plot_df["Mes"], y=plot_df[col],
                    mode="lines",
                    name=col,
                    line=dict(color="#00E5FF" if i == 0 else "#67E8F9", width=2.5),
                    fill="tozeroy",
                    fillcolor=area_fill("#00E5FF", 0.22 if i == 0 else 0.10),
                    hovertemplate=f"{col}: %{{y:,.0f}} órdenes<extra></extra>",
                )
            )
        fig.update_layout(
            title="Sales Overview — Órdenes por mes",
            legend_title_text="",
            xaxis=dict(categoryorder="array", categoryarray=plot_df["Mes"].tolist()),
        )
        _apply_template(fig)
        st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG, key="sales_overview")

    # --- Órdenes por tiempo (heatmap día × mes) ------------------------------
    tmp["weekday"] = tmp[dc].dt.weekday
    heat = (
        tmp.groupby(["weekday", "month"], as_index=False)[oc].sum()
    )
    if not heat.empty:
        # Matriz weekday(0-6) × month(1-12)
        matrix = (
            heat.pivot(index="weekday", columns="month", values=oc)
            .reindex(index=range(7), columns=range(1, 13))
            .fillna(0)
        )
        fig_h = go.Figure(
            go.Heatmap(
                z=matrix.values,
                x=[ "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                y=_DIAS,
                colorscale=HEAT_SCALE,
                colorbar=dict(title="Órdenes"),
                hovertemplate="%{y} · %{x}<br>%{z:,.0f} órdenes<extra></extra>",
            )
        )
        fig_h.update_layout(
            title="Órdenes por tiempo — día de la semana × mes",
            yaxis=dict(autorange="reversed"),
        )
        _apply_template(fig_h)
        st.plotly_chart(fig_h, width="stretch", config=PLOTLY_CONFIG, key="orders_heatmap")
