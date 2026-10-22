"""Cálculo de KPIs de negocio a partir de los modelos Gold.

Estos cálculos se usan cuando el dashboard necesita derivar métricas a partir
de los modelos Gold crudos (p. ej. agregaciones por país o categoría que no
están precalculadas en ``analytics.daily_sales_kpis``).

Compatibilidad bidireccional de esquemas (fusión de enfoques):
    El mismo cálculo debe funcionar con **cualquiera** de los dos modelos que
    define el contrato, porque el dashboard puede degradar entre fuentes:
        - Data mart §4.1 (``analytics.daily_sales_kpis``): 1 fila/día.
              kpi_date, total_orders, completed_orders, cancelled_orders,
              total_revenue, average_ticket, distinct_customers, top_category
        - Gold US-07 (``gold/.../daily_sales``): fecha×país×categoría.
              order_date, shipping_country, category, total_revenue,
              orders_count, units_sold, avg_ticket, returns_count, refunded_amount
    Se resuelve con el helper ``_col`` que elige la primera columna disponible.

Métricas oficiales (``docs/contratos_integracion.md`` §4.2):
    - ``total_revenue``: SUM(total_amount) donde status = 'COMPLETED'.
    - ``average_ticket``: total_revenue / completed_orders (0 si no hay completadas).
    - ``distinct_customers``: COUNT(DISTINCT customer_id).
    - ``return_rate``: devoluciones / ventas (modelo product_performance).
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def _col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Devuelve el nombre de la primera columna de ``candidates`` presente en ``df``.

    Permite que un mismo cálculo funcione con el data mart §4.1 y con el Gold
    US-07 sin duplicar lógica. Si ninguna existe, devuelve ``None``.
    """
    for name in candidates:
        if name in df.columns:
            return name
    return None


def compute_global_kpis(daily_sales: pd.DataFrame) -> dict:
    """Calcula los KPIs agregados para la pestaña 'Visión General de Ventas'.

    Tolerante a ambos esquemas (data mart §4.1 y Gold US-07). Cuando falta una
    columna propia del data mart (p. ej. ``completed_orders``), se aproxima a
    partir de la información disponible en el Gold US-07 y se documenta el
    origen en el docstring de la vista.

    Args:
        daily_sales: DataFrame con el modelo Gold ya cargado y normalizado.

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

    orders_col = _col(daily_sales, "total_orders", "orders_count")
    total_orders = int(daily_sales[orders_col].sum()) if orders_col else 0

    # completed_orders solo existe en el data mart; en el Gold US-07 todas las
    # órdenes registradas se asumen completadas.
    completed_col = _col(daily_sales, "completed_orders")
    completed_orders = int(daily_sales[completed_col].sum()) if completed_col else total_orders

    # cancelled_orders solo existe en el data mart; en el Gold US-07 se usa
    # returns_count como mejor aproximación disponible.
    cancelled_col = _col(daily_sales, "cancelled_orders", "returns_count")
    cancelled_orders = int(daily_sales[cancelled_col].sum()) if cancelled_col else 0

    distinct_col = _col(daily_sales, "distinct_customers")
    distinct_customers = int(daily_sales[distinct_col].sum()) if distinct_col else total_orders

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
    """Serie temporal de ingressos por día (gráfico de líneas de Plotly).

    Tolerante a ``kpi_date`` (data mart) u ``order_date`` (Gold US-07).
    """
    date_col = _col(daily_sales, "kpi_date", "order_date")
    if daily_sales.empty or date_col is None or "total_revenue" not in daily_sales.columns:
        return pd.DataFrame(columns=["kpi_date", "total_revenue"])
    out = daily_sales[[date_col, "total_revenue"]].copy()
    out = out.rename(columns={date_col: "kpi_date"})
    out["kpi_date"] = pd.to_datetime(out["kpi_date"])
    # Agrupa por día: en el grano US-07 hay varias filas (país×categoría) por
    # fecha y la serie temporal necesita un punto por día.
    out = out.groupby("kpi_date", as_index=False)["total_revenue"].sum()
    return out.sort_values("kpi_date")


def compute_top_categories(daily_sales: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Ranking de categorías por órdenes (gráfico de barras).

    Tolerante a ``top_category`` (data mart) o ``category`` (Gold US-07), y a
    ``completed_orders`` / ``orders_count`` / ``total_orders`` para el cómputo.
    """
    cat_col = _col(daily_sales, "top_category", "category")
    orders_col = _col(daily_sales, "completed_orders", "orders_count", "total_orders")
    if daily_sales.empty or cat_col is None or orders_col is None:
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
    """Aplica los filtros del sidebar a un DataFrame del modelo Gold.

    Reglas de filtrado (tolerantes a ambos esquemas):
        - ``date_range``  -> por ``date_col`` o ``order_date``.
        - ``countries``   -> por ``country`` o ``shipping_country`` si existe.
        - ``categories``  -> por ``category`` o ``top_category`` si existe.

    Si un filtro es ``None`` (dimensión no disponible) o lista vacía, no se
    aplica y se devuelven todas las filas. Esto evita que el dashboard quede
    en blanco cuando el usuario deselecciona todas las opciones.
    """
    if df.empty:
        return df

    out = df.copy()

    actual_date_col = _col(out, date_col, "kpi_date", "order_date")
    if date_range and actual_date_col and len(date_range) == 2:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        dates = pd.to_datetime(out[actual_date_col])
        out = out[(dates >= start) & (dates <= end)]

    country_col = _col(out, "country", "shipping_country")
    if countries and country_col:
        out = out[out[country_col].isin(countries)]

    cat_col = _col(out, "category", "top_category")
    if categories and cat_col:
        out = out[out[cat_col].isin(categories)]

    return out
