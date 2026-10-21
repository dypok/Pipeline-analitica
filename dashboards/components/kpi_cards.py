"""Tarjetas de KPIs — tema Data Observability (cian neón).

Mantiene la firma pública ``render_kpi_cards(kpis, columns, deltas)`` para no
romper las vistas. Soporta descripción bajo el valor y direcciones semánticas
("ok", "down", "warn", "run", "flat").
"""

from __future__ import annotations

from typing import Iterable, Optional

from dashboards.styles import kpi_card_html

# clave -> (etiqueta, formato, descripción)
_META = {
    "total_revenue":      ("Ingresos totales",   "currency", "Órdenes completadas"),
    "total_orders":       ("Órdenes totales",    "int",      "Pedidos registrados"),
    "average_ticket":     ("Ticket promedio",    "currency", "Ingresos / completadas"),
    "return_rate":        ("Tasa de devolución", "percent",  "Canceladas / totales"),
    "distinct_customers": ("Clientes activos",   "int",      "IDs únicos con compra"),
    "total_products":     ("Productos",          "int",      "SKUs analizados"),
    "total_units":        ("Unidades vendidas",  "int",      "Suma de unidades"),
    "total_gross":        ("Ingresos brutos",    "currency", "Antes de devoluciones"),
    "total_customers":    ("Clientes totales",   "int",      "Registros de clientes"),
    "avg_ltv":            ("LTV promedio",       "currency", "Valor de vida promedio"),
    "avg_orders":         ("Órdenes/cliente",    "float",    "Frecuencia de compra"),
    "quarantine_total":   ("En Quarantine",      "int",      "Registros rechazados"),
}


def _fmt(value, kind: str) -> str:
    if value is None:
        return "—"
    if kind == "currency":
        return f"${value:,.0f}" if abs(value) >= 1000 else f"${value:,.2f}"
    if kind == "int":
        return f"{int(value):,}"
    if kind == "float":
        return f"{value:,.2f}"
    if kind == "percent":
        return f"{value * 100:.1f}%"
    return str(value)


def render_kpi_cards(
    kpis: dict,
    columns: Iterable[str] | None = None,
    deltas: Optional[dict] = None,
    descriptions: Optional[dict] = None,
) -> None:
    """Dibuja una fila de tarjetas KPI.

    Args:
        kpis: Diccionario de métricas (de ``kpi_calculator``).
        columns: Orden opcional de las claves a mostrar.
        deltas: Mapa ``{clave: (texto_delta, direccion)}``.
        descriptions: Mapa opcional ``{clave: texto}`` de descripción.
    """
    import streamlit as st

    keys = list(columns) if columns else [
        "total_revenue", "total_orders", "average_ticket", "return_rate", "distinct_customers",
    ]
    deltas = deltas or {}
    descriptions = descriptions or {}
    cols = st.columns(len(keys))

    for col, key in zip(cols, keys):
        label, kind, default_desc = _META.get(
            key, (key.replace("_", " ").title(), "raw", "")
        )
        value = kpis.get(key, 0)
        delta = deltas.get(key)
        delta_text, direction = (delta if delta else (None, "flat"))
        html = kpi_card_html(
            label=label,
            value=_fmt(value, kind),
            delta=delta_text,
            direction=direction,
            description=descriptions.get(key, default_desc),
        )
        with col:
            st.markdown(html, unsafe_allow_html=True)
