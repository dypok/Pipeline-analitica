"""Cálculo de KPIs de negocio a partir de los modelos Gold.

Estos cálculos se usan cuando el dashboard necesita derivar métricas a partir
de los modelos Gold crudos (p. ej. agregaciones por país o categoría que no
están precalculadas en ``analytics.daily_sales_kpis``).

Métricas oficiales (``docs/contratos_integracion.md`` §4.2):
    - ``total_revenue``: SUM(total_amount) donde status = 'COMPLETED'.
    - ``average_ticket``: total_revenue / completed_orders (0 si no hay completadas).
    - ``distinct_customers``: COUNT(DISTINCT customer_id).
    - ``return_rate``: devoluciones / ventas (modelo product_performance).
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def compute_global_kpis(daily_sales: pd.DataFrame) -> dict:
    """Calcula los KPIs agregados para la pestaña 'Visión General de Ventas'.

    Args:
        daily_sales: DataFrame con el modelo ``daily_sales_kpis`` ya cargado
            (columnas: kpi_date, total_orders, completed_orders, cancelled_orders,
            total_revenue, average_ticket, distinct_customers, top_category).

    Returns:
        Diccionario con los KPIs para las tarjetas de la UI.
    """
    if daily_sales.empty:
        return {
            "total_revenue": 0.0,
            "total_orders": 0,
            "average_ticket": 0.0,
            "return_rate": 0.0,
            "distinct_customers": 0,
        }

    total_revenue = float(daily_sales["total_revenue"].sum())
    total_orders = int(daily_sales["total_orders"].sum())
    completed_orders = int(daily_sales["completed_orders"].sum())
    cancelled_orders = int(daily_sales["cancelled_orders"].sum())
    distinct_customers = int(daily_sales["distinct_customers"].sum())

    average_ticket = (total_revenue / completed_orders) if completed_orders > 0 else 0.0
    return_rate = (cancelled_orders / total_orders) if total_orders > 0 else 0.0

    return {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "average_ticket": round(average_ticket, 2),
        "return_rate": round(return_rate, 4),
        "distinct_customers": distinct_customers,
    }


def compute_revenue_trend(daily_sales: pd.DataFrame) -> pd.DataFrame:
    """Serie temporal de ingresos por día (para el gráfico de líneas de Plotly)."""
    if daily_sales.empty:
        return pd.DataFrame(columns=["kpi_date", "total_revenue"])
    out = daily_sales[["kpi_date", "total_revenue"]].copy()
    out = out.sort_values("kpi_date")
    return out


def compute_top_categories(daily_sales: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Ranking de categorías por órdenes completadas (gráfico de barras).

    TODO: cuando el modelo Gold exponga la dimensión categoría, sustituir este
    cómputo por una agregación directa sobre ``daily_sales`` particionada.
    """
    if daily_sales.empty or "top_category" not in daily_sales.columns:
        return pd.DataFrame(columns=["top_category", "completed_orders"])
    out = (
        daily_sales.groupby("top_category", as_index=False)["completed_orders"]
        .sum()
        .sort_values("completed_orders", ascending=False)
        .head(top_n)
    )
    return out


def compute_customer_segments(customer_360: pd.DataFrame) -> pd.DataFrame:
    """Segmenta clientes por Lifetime Value (LTV) en cuartiles.

    TODO(US-07): alinearse con el modelo ``customer_360`` cuando Esteban lo
    publique. Esquema esperado:
        customer_id, lifetime_value, total_orders, favorite_category, ...
    """
    if customer_360.empty or "lifetime_value" not in customer_360.columns:
        return pd.DataFrame(columns=["segment", "customer_id", "lifetime_value"])
    out = customer_360[["customer_id", "lifetime_value"]].copy()
    out["segment"] = pd.qcut(
        out["lifetime_value"].fillna(0),
        q=4,
        labels=["Bronce", "Plata", "Oro", "Premium"],
        duplicates="drop",
    )
    return out


def compute_quarantine_summary(quarantine: pd.DataFrame) -> dict:
    """Resume los registros en Quarantine por motivo de rechazo (US-15)."""
    if quarantine.empty:
        return {"total": 0, "by_reason": {}, "by_entity": {}}
    by_reason = quarantine["quarantine_reason"].value_counts().to_dict()
    by_entity = quarantine.get("entity_type", pd.Series()).value_counts().to_dict()
    return {
        "total": len(quarantine),
        "by_reason": by_reason,
        "by_entity": by_entity,
    }


def filter_by_selections(
    df: pd.DataFrame,
    date_range: Optional[tuple] = None,
    countries: Optional[list] = None,
    categories: Optional[list] = None,
    date_col: str = "kpi_date",
) -> pd.DataFrame:
    """Aplica los filtros del sidebar a un DataFrame del modelo Gold.

    Reglas de filtrado (tolerantes a la ausencia de columnas):
        - ``date_range``  -> filtra por ``date_col`` (por defecto ``kpi_date``).
        - ``countries``   -> filtra por ``country`` si la columna existe.
        - ``categories``  -> filtra por ``category`` si la columna existe;
          si no, filtra por ``top_category`` (caso ``daily_sales_kpis``).

    Si un filtro es ``None`` (dimensión no disponible) o lista vacía, no se
    aplica y se devuelven todas las filas. Esto evita que el dashboard quede
    en blanco cuando el usuario deselecciona todas las opciones.
    """
    out = df.copy()
    if date_range and date_col in out.columns and len(date_range) == 2:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        out = out[(out[date_col] >= start) & (out[date_col] <= end)]
    # Filtro por país (customer_360.country)
    if countries and "country" in out.columns:
        out = out[out["country"].isin(countries)]
    # Filtro por categoría: prefiera ``category`` (product_performance);
    # si no existe, use ``top_category`` (daily_sales_kpis).
    if categories:
        if "category" in out.columns:
            out = out[out["category"].isin(categories)]
        elif "top_category" in out.columns:
            out = out[out["top_category"].isin(categories)]
    return out
