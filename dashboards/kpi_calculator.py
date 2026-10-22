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
            (columnas: kpi_date / order_date, total_orders / orders_count,
            completed_orders, cancelled_orders / returns_count,
            total_revenue, average_ticket / avg_ticket, distinct_customers, top_category / category).

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

    total_revenue = float(daily_sales["total_revenue"].sum()) if "total_revenue" in daily_sales.columns else 0.0

    orders_col = (
        "total_orders"
        if "total_orders" in daily_sales.columns
        else ("orders_count" if "orders_count" in daily_sales.columns else None)
    )
    total_orders = int(daily_sales[orders_col].sum()) if orders_col else 0

    completed_orders = (
        int(daily_sales["completed_orders"].sum())
        if "completed_orders" in daily_sales.columns
        else total_orders
    )

    cancelled_orders = (
        int(daily_sales["cancelled_orders"].sum())
        if "cancelled_orders" in daily_sales.columns
        else (int(daily_sales["returns_count"].sum()) if "returns_count" in daily_sales.columns else 0)
    )

    distinct_customers = (
        int(daily_sales["distinct_customers"].sum())
        if "distinct_customers" in daily_sales.columns
        else total_orders
    )

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
    date_col = (
        "kpi_date"
        if "kpi_date" in daily_sales.columns
        else ("order_date" if "order_date" in daily_sales.columns else None)
    )
    if daily_sales.empty or not date_col or "total_revenue" not in daily_sales.columns:
        return pd.DataFrame(columns=["kpi_date", "total_revenue"])
    out = daily_sales[[date_col, "total_revenue"]].copy()
    if date_col != "kpi_date":
        out = out.rename(columns={date_col: "kpi_date"})
    out["kpi_date"] = pd.to_datetime(out["kpi_date"])
    out = out.groupby("kpi_date", as_index=False)["total_revenue"].sum()
    out = out.sort_values("kpi_date")
    return out


def compute_top_categories(daily_sales: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Ranking de categorías por órdenes completadas (gráfico de barras)."""
    cat_col = (
        "top_category"
        if "top_category" in daily_sales.columns
        else ("category" if "category" in daily_sales.columns else None)
    )
    orders_col = (
        "completed_orders"
        if "completed_orders" in daily_sales.columns
        else (
            "orders_count"
            if "orders_count" in daily_sales.columns
            else ("total_orders" if "total_orders" in daily_sales.columns else None)
        )
    )
    if daily_sales.empty or not cat_col or not orders_col:
        return pd.DataFrame(columns=["top_category", "completed_orders"])
    out = (
        daily_sales.groupby(cat_col, as_index=False)[orders_col]
        .sum()
        .rename(columns={cat_col: "top_category", orders_col: "completed_orders"})
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
    """Aplica los filtros del sidebar a un DataFrame del modelo Gold."""
    if df.empty:
        return df

    out = df.copy()
    actual_date_col = (
        date_col
        if date_col in out.columns
        else ("order_date" if "order_date" in out.columns else None)
    )
    if date_range and actual_date_col and len(date_range) == 2:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        dates = pd.to_datetime(out[actual_date_col])
        out = out[(dates >= start) & (dates <= end)]

    country_col = (
        "country"
        if "country" in out.columns
        else ("shipping_country" if "shipping_country" in out.columns else None)
    )
    if countries and country_col:
        out = out[out[country_col].isin(countries)]

    cat_col = (
        "category"
        if "category" in out.columns
        else ("top_category" if "top_category" in out.columns else None)
    )
    if categories and cat_col:
        out = out[out[cat_col].isin(categories)]

    return out
