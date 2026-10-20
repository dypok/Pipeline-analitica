"""Tarjetas de KPIs reutilizables (US-14).

Renderiza una fila de métricas con formato y color según el signo/tendencia.
"""

from __future__ import annotations

from typing import Iterable

import streamlit as st


def _format_currency(value: float) -> str:
    return f"${value:,.2f}"


def _format_int(value: int) -> str:
    return f"{value:,}"


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def render_kpi_cards(kpis: dict, columns: Iterable[str] | None = None) -> None:
    """Dibuja una fila de tarjetas con los KPIs principales.

    Args:
        kpis: Diccionario con las métricas calculadas por ``kpi_calculator``.
        columns: Orden opcional de las métricas a mostrar.
    """
    formatters = {
        "total_revenue": _format_currency,
        "total_orders": _format_int,
        "average_ticket": _format_currency,
        "return_rate": _format_percent,
        "distinct_customers": _format_int,
    }
    labels = {
        "total_revenue": "💰 Ingresos Totales",
        "total_orders": "🛒 Órdenes Totales",
        "average_ticket": "🎫 Ticket Promedio",
        "return_rate": "↩️ Tasa de Devolución",
        "distinct_customers": "👥 Clientes Activos",
    }

    keys = list(columns) if columns else ["total_revenue", "total_orders", "average_ticket", "return_rate", "distinct_customers"]
    cols = st.columns(len(keys))
    for col, key in zip(cols, keys):
        value = kpis.get(key, 0)
        formatter = formatters.get(key, lambda v: str(v))
        label = labels.get(key, key.replace("_", " ").title())
        with col:
            st.metric(label=label, value=formatter(value))
