"""Pruebas unitarias para los componentes y cálculo de KPIs de los Dashboards."""

from __future__ import annotations

import pandas as pd
import pytest

from dashboards.data_loader import GoldDataLoader
from dashboards.kpi_calculator import (
    compute_global_kpis,
    compute_revenue_trend,
    compute_top_categories,
    filter_by_selections,
)


def test_compute_global_kpis_with_parquet_schema():
    """Valida el cálculo de KPIs con el schema de Parquet Gold (orders_count, etc.)."""
    df = pd.DataFrame([
        {
            "order_date": "2026-04-20",
            "shipping_country": "CO",
            "category": "Electronics",
            "total_revenue": 1000.0,
            "orders_count": 5,
            "units_sold": 10,
            "avg_ticket": 200.0,
            "returns_count": 1,
            "refunded_amount": 50.0,
        },
        {
            "order_date": "2026-04-21",
            "shipping_country": "MX",
            "category": "Fashion",
            "total_revenue": 500.0,
            "orders_count": 5,
            "units_sold": 5,
            "avg_ticket": 100.0,
            "returns_count": 0,
            "refunded_amount": 0.0,
        },
    ])
    kpis = compute_global_kpis(df)
    assert kpis["total_revenue"] == 1500.0
    assert kpis["total_orders"] == 10
    assert kpis["average_ticket"] == 150.0
    assert kpis["return_rate"] == 0.1  # 1/10


def test_compute_global_kpis_with_postgres_schema():
    """Valida el cálculo de KPIs con el schema de PostgreSQL (total_orders, completed_orders, etc.)."""
    df = pd.DataFrame([
        {
            "kpi_date": "2026-04-20",
            "total_orders": 10,
            "completed_orders": 8,
            "cancelled_orders": 2,
            "total_revenue": 2000.0,
            "average_ticket": 250.0,
            "distinct_customers": 8,
            "top_category": "Home",
        }
    ])
    kpis = compute_global_kpis(df)
    assert kpis["total_revenue"] == 2000.0
    assert kpis["total_orders"] == 10
    assert kpis["average_ticket"] == 250.0
    assert kpis["return_rate"] == 0.2  # 2/10
    assert kpis["distinct_customers"] == 8


def test_compute_revenue_trend_and_top_categories():
    """Verifica agrupación y cálculo de tendencia y categorías."""
    df = pd.DataFrame([
        {"order_date": "2026-04-20", "category": "Electronics", "orders_count": 3, "total_revenue": 300.0},
        {"order_date": "2026-04-20", "category": "Fashion", "orders_count": 2, "total_revenue": 100.0},
        {"order_date": "2026-04-21", "category": "Electronics", "orders_count": 5, "total_revenue": 500.0},
    ])
    trend = compute_revenue_trend(df)
    assert len(trend) == 2  # Dos fechas distintas
    assert trend.loc[trend["kpi_date"] == pd.to_datetime("2026-04-20"), "total_revenue"].iloc[0] == 400.0

    cats = compute_top_categories(df)
    assert len(cats) == 2
    assert cats.iloc[0]["top_category"] == "Electronics"
    assert cats.iloc[0]["completed_orders"] == 8


def test_gold_data_loader_graceful_local_parquet():
    """Comprueba que GoldDataLoader normaliza correctamente el parquet local."""
    loader = GoldDataLoader()
    daily_sales = loader.load_daily_sales()
    assert not daily_sales.empty
    # Comprobar presencia de columnas normalizadas
    assert "total_orders" in daily_sales.columns
    assert "kpi_date" in daily_sales.columns
    assert "average_ticket" in daily_sales.columns
