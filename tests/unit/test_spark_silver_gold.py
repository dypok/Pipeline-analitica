"""Pruebas unitarias dedicadas para los módulos de Esteban Padilla (US-06 y US-07)."""

import os
import tempfile
import pandas as pd
import pytest

from src.silver.silver_transformer import SilverTransformer
from src.gold.gold_aggregator import GoldAggregator


@pytest.fixture
def sample_orders_raw():
    return [
        {"order_id": "ORD-101", "customer_id": "C-01", "order_date": "2026-04-10", "total_amount": 250.00, "status": "COMPLETED", "category": "Electronics", "quantity": 1, "unit_price": 250.00, "shipping_country": "CO"},
        {"order_id": "ORD-102", "customer_id": "C-02", "order_date": "2026-04-10", "total_amount": 120.00, "status": "COMPLETED", "category": "Home", "quantity": 2, "unit_price": 60.00, "shipping_country": "MX"},
        {"order_id": "ORD-103", "customer_id": "C-01", "order_date": "2026-04-11", "total_amount": 50.00, "status": "COMPLETED", "category": "Electronics", "quantity": 1, "unit_price": 50.00, "shipping_country": "CO"},
        {"order_id": "ORD-104", "customer_id": "C-03", "order_date": "2026-04-11", "total_amount": -10.00, "status": "COMPLETED", "category": "Sports", "quantity": 1, "unit_price": -10.00, "shipping_country": "PE"},
    ]


def test_silver_transformer_clean_orders(sample_orders_raw):
    transformer = SilverTransformer()
    df_clean, quarantine, report = transformer.transform_orders(sample_orders_raw)

    assert len(df_clean) == 3
    assert len(quarantine) == 1
    assert quarantine[0]["quarantine_reason"] is not None
    assert "Monto inválido" in quarantine[0]["quarantine_reason"]

    assert set(df_clean["order_id"]) == {"ORD-101", "ORD-102", "ORD-103"}
    assert all(df_clean["total_amount"] > 0)


def test_gold_aggregator_models(sample_orders_raw):
    transformer = SilverTransformer()
    df_clean, _, _ = transformer.transform_orders(sample_orders_raw)

    aggregator = GoldAggregator()
    df_daily = aggregator.build_daily_sales(df_clean)
    df_c360 = aggregator.build_customer_360(df_clean)
    df_perf = aggregator.build_product_performance(df_clean)

    # Verificaciones daily_sales
    assert len(df_daily) >= 2
    assert "total_revenue" in df_daily.columns
    assert "avg_ticket" in df_daily.columns

    # Verificaciones customer_360
    assert len(df_c360) == 2  # C-01 y C-02
    c01 = df_c360[df_c360["customer_id"] == "C-01"].iloc[0]
    assert c01["total_orders"] == 2
    assert c01["lifetime_value"] == 300.00

    # Verificaciones product_performance
    assert not df_perf.empty
    assert "margin" in df_perf.columns
    assert "performance_tier" in df_perf.columns
