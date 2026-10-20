"""Pruebas de transformaciones Bronze -> Silver y Silver -> Gold (US-06 y US-07).
Responsable: Esteban Padilla.
Valida deduplicación, tipado, reglas de calidad, segregación a Quarantine,
modelos analíticos Gold (daily_sales, customer_360, product_performance),
cuadre matemático de ingresos (diferencia 0.00) e idempotencia.
"""

import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.quality.validator import DataQualityValidator
from src.silver.silver_transformer import SilverTransformer
from src.gold.gold_aggregator import GoldAggregator


def test_silver_deduplication_and_typing():
    """Valida que la lógica de Silver elimine duplicados y aplique tipado numérico."""
    records = [
        {"order_id": "ORD-1", "customer_id": "C1", "order_date": "2026-02-01", "quantity": "2", "unit_price": "10.5", "total_amount": "21.0", "status": "COMPLETED"},
        # Duplicado de ORD-1 con monto actualizado
        {"order_id": "ORD-1", "customer_id": "C1", "order_date": "2026-02-01", "quantity": "3", "unit_price": "10.5", "total_amount": "31.5", "status": "COMPLETED"},
        {"order_id": "ORD-2", "customer_id": "C2", "order_date": "2026-02-02", "quantity": "1", "unit_price": "50.0", "total_amount": "50.0", "status": "COMPLETED"},
    ]

    transformer = SilverTransformer()
    df_clean, quarantine, report = transformer.transform_orders(records)

    assert len(df_clean) == 2
    assert len(quarantine) == 0

    # Verificar que el registro conservado de ORD-1 sea el último (total_amount = 31.5)
    row_1 = df_clean[df_clean["order_id"] == "ORD-1"].iloc[0]
    assert float(row_1["total_amount"]) == 31.5

    # Tipado y particionado
    assert df_clean["year"].tolist() == [2026, 2026]
    assert df_clean["month"].tolist() == [2, 2]
    assert df_clean["quantity"].dtype.name == "int32"
    assert df_clean["total_amount"].dtype.name == "float64"


def test_silver_quarantine_routing():
    """Valida que registros con montos negativos o campos nulos se enruten a Quarantine."""
    records = [
        {"order_id": "ORD-10", "customer_id": "C1", "order_date": "2026-02-15", "total_amount": 100.0, "status": "COMPLETED"},
        {"order_id": "ORD-11", "customer_id": "C2", "order_date": "2026-02-15", "total_amount": -50.0, "status": "COMPLETED"},
        {"order_id": None, "customer_id": "C3", "order_date": "2026-02-15", "total_amount": 20.0, "status": "COMPLETED"},
        {"order_id": "ORD-12", "customer_id": "C4", "order_date": "2026-02-15", "total_amount": 80.0, "status": "INVALID_STATUS"},
    ]

    transformer = SilverTransformer()
    df_clean, quarantine, report = transformer.transform_orders(records)

    assert len(df_clean) == 1
    assert len(quarantine) == 3
    assert df_clean.iloc[0]["order_id"] == "ORD-10"

    # Verificar metadatos de Quarantine
    reasons = [q["quarantine_reason"] for q in quarantine]
    assert any("Monto inválido" in r for r in reasons)
    assert any("Campo obligatorio" in r for r in reasons)
    assert any("Estado desconocido" in r for r in reasons)


def test_silver_customer_and_product_transformations():
    """Valida la transformación y limpieza de datasets de clientes y productos."""
    transformer = SilverTransformer()

    customers = [
        {"customer_id": "C1", "first_name": "  Carlos ", "last_name": "Gomez", "email": "Carlos.Gomez@Example.com", "country": "co"},
        {"customer_id": "C1", "first_name": "Carlos", "last_name": "Gomez", "email": "carlos.gomez@example.com", "country": "CO"},
        {"customer_id": "C2", "first_name": "Ana", "last_name": "Perez", "email": "invalid-email-address", "country": "MX"},
    ]
    df_cust, cust_q, _ = transformer.transform_customers(customers)
    assert len(df_cust) == 1
    assert len(cust_q) == 1
    assert df_cust.iloc[0]["customer_id"] == "C1"
    assert df_cust.iloc[0]["country"] == "CO"
    assert df_cust.iloc[0]["email"] == "carlos.gomez@example.com"

    products = [
        {"product_id": "P1", "product_name": " Laptop ", "category": "electronics", "unit_price": "1200.5", "cost_price": "800.0"},
        {"product_id": "P2", "product_name": "Mouse", "category": "electronics", "unit_price": "-10.0", "cost_price": "5.0"},
    ]
    df_prod, prod_q, _ = transformer.transform_products(products)
    assert len(df_prod) == 1
    assert len(prod_q) == 1
    assert df_prod.iloc[0]["product_id"] == "P1"
    assert df_prod.iloc[0]["category"] == "Electronics"
    assert df_prod.iloc[0]["unit_price"] == 1200.5


def test_gold_daily_sales_calculation():
    """Valida el modelo Gold daily_sales con grano (order_date, shipping_country, category)."""
    orders_data = [
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-02-10", "shipping_country": "CO", "category": "Electronics", "quantity": 1, "total_amount": 100.0, "status": "COMPLETED"},
        {"order_id": "O2", "customer_id": "C2", "order_date": "2026-02-10", "shipping_country": "CO", "category": "Electronics", "quantity": 2, "total_amount": 200.0, "status": "COMPLETED"},
        {"order_id": "O3", "customer_id": "C1", "order_date": "2026-02-10", "shipping_country": "CO", "category": "Fashion", "quantity": 1, "total_amount": 50.0, "status": "CANCELLED"},
        {"order_id": "O4", "customer_id": "C3", "order_date": "2026-02-11", "shipping_country": "MX", "category": "Sports", "quantity": 3, "total_amount": 90.0, "status": "COMPLETED"},
    ]
    df_orders = pd.DataFrame(orders_data)

    aggregator = GoldAggregator()
    df_gold = aggregator.build_daily_sales(df_orders)

    assert len(df_gold) == 2  # CO Electronics en 2026-02-10, MX Sports en 2026-02-11

    co_elec = df_gold[(df_gold["order_date"] == "2026-02-10") & (df_gold["category"] == "Electronics")].iloc[0]
    assert co_elec["total_revenue"] == 300.0
    assert co_elec["orders_count"] == 2
    assert co_elec["units_sold"] == 3
    assert co_elec["avg_ticket"] == 150.0

    mx_sports = df_gold[(df_gold["order_date"] == "2026-02-11") & (df_gold["category"] == "Sports")].iloc[0]
    assert mx_sports["total_revenue"] == 90.0
    assert mx_sports["orders_count"] == 1
    assert mx_sports["units_sold"] == 3
    assert mx_sports["avg_ticket"] == 90.0


def test_gold_customer_360_with_rfm_and_pii_masking():
    """Valida el modelo customer_360 con RFM y enmascaramiento seguro PII con SHA-256."""
    orders_data = [
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-02-01", "quantity": 2, "total_amount": 100.0, "status": "COMPLETED", "category": "Electronics"},
        {"order_id": "O2", "customer_id": "C1", "order_date": "2026-02-10", "quantity": 3, "total_amount": 150.0, "status": "COMPLETED", "category": "Electronics"},
        {"order_id": "O3", "customer_id": "C2", "order_date": "2026-02-05", "quantity": 1, "total_amount": 40.0, "status": "COMPLETED", "category": "Books"},
    ]
    customers_data = [
        {"customer_id": "C1", "country": "CO", "city": "Bogota", "email": "carlos@example.com", "phone": "+57 300 1234567"},
        {"customer_id": "C2", "country": "MX", "city": "CDMX", "email": "maria@example.com", "phone": "+52 555 7654321"},
    ]

    aggregator = GoldAggregator()
    df_c360 = aggregator.build_customer_360(pd.DataFrame(orders_data), pd.DataFrame(customers_data))

    assert len(df_c360) == 2
    c1 = df_c360[df_c360["customer_id"] == "C1"].iloc[0]
    assert c1["total_orders"] == 2
    assert c1["lifetime_value"] == 250.0
    assert c1["units_bought"] == 5
    assert c1["avg_order_value"] == 125.0
    assert c1["favorite_category"] == "Electronics"
    assert "rfm_segment" in c1

    # Verificación de Minimización PII: No debe haber emails ni teléfonos en texto plano
    assert "email" not in df_c360.columns
    assert "phone" not in df_c360.columns
    assert "email_hash" in df_c360.columns
    assert len(c1["email_hash"]) == 64  # SHA-256 hash length
    assert c1["phone_masked"].endswith("4567")


def test_gold_product_performance_calculation():
    """Valida el modelo product_performance con márgenes, retornos y performance_tier."""
    orders_data = [
        {"order_id": "O1", "product_id": "P1", "quantity": 5, "total_amount": 500.0, "status": "COMPLETED", "category": "Electronics"},
        {"order_id": "O2", "product_id": "P1", "quantity": 5, "total_amount": 500.0, "status": "COMPLETED", "category": "Electronics"},
        {"order_id": "O3", "product_id": "P2", "quantity": 2, "total_amount": 40.0, "status": "COMPLETED", "category": "Stationery"},
    ]
    products_data = [
        {"product_id": "P1", "product_name": "Headphones", "cost_price": 40.0},
        {"product_id": "P2", "product_name": "Notebook", "cost_price": 25.0},
    ]
    returns_data = [
        {"return_id": "R1", "order_id": "O1", "product_id": "P1", "refund_amount": 100.0},
    ]

    aggregator = GoldAggregator()
    df_perf = aggregator.build_product_performance(
        pd.DataFrame(orders_data),
        pd.DataFrame(products_data),
        pd.DataFrame(returns_data),
    )

    assert len(df_perf) == 2
    p1 = df_perf[df_perf["product_id"] == "P1"].iloc[0]
    assert p1["units_sold"] == 10
    assert p1["gross_revenue"] == 1000.0
    assert p1["total_cost"] == 400.0  # 10 * 40.0
    assert p1["margin"] == 600.0      # 1000 - 400
    assert p1["returns_count"] == 1
    assert p1["return_rate"] == 0.1   # 1 / 10

    p2 = df_perf[df_perf["product_id"] == "P2"].iloc[0]
    assert p2["margin"] == -10.0      # 40.0 - (2 * 25.0) = -10.0 (Underperforming)
    assert p2["performance_tier"] == "Underperforming"


def test_mathematical_consistency_silver_vs_gold():
    """Prueba T07.1 / P06: Cuadre matemático exacto entre ingresos Silver y Gold (diferencia 0.00)."""
    orders_data = [
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-03-01", "shipping_country": "CO", "category": "Electronics", "quantity": 2, "total_amount": 149.99, "status": "COMPLETED"},
        {"order_id": "O2", "customer_id": "C2", "order_date": "2026-03-01", "shipping_country": "MX", "category": "Fashion", "quantity": 1, "total_amount": 49.50, "status": "COMPLETED"},
        {"order_id": "O3", "customer_id": "C3", "order_date": "2026-03-02", "shipping_country": "AR", "category": "Home", "quantity": 4, "total_amount": 200.00, "status": "COMPLETED"},
        {"order_id": "O4", "customer_id": "C1", "order_date": "2026-03-02", "shipping_country": "CO", "category": "Electronics", "quantity": 1, "total_amount": 80.00, "status": "CANCELLED"},
        {"order_id": "O5", "customer_id": "C4", "order_date": "2026-03-02", "shipping_country": "PE", "category": "Toys", "quantity": 2, "total_amount": 60.00, "status": "PENDING"},
    ]
    df_silver = pd.DataFrame(orders_data)

    aggregator = GoldAggregator()
    df_gold_daily = aggregator.build_daily_sales(df_silver)

    # Suma de órdenes completadas en Silver
    silver_completed_revenue = round(float(df_silver[df_silver["status"] == "COMPLETED"]["total_amount"].sum()), 2)
    gold_total_revenue = round(float(df_gold_daily["total_revenue"].sum()), 2)

    diff = abs(silver_completed_revenue - gold_total_revenue)
    assert diff == 0.00, f"Discrepancia en ingresos: Silver={silver_completed_revenue}, Gold={gold_total_revenue}"


def test_gold_idempotence():
    """Prueba T07.2 / P05: Idempotencia en la generación de modelos Gold."""
    orders_data = [
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-03-01", "shipping_country": "CO", "category": "Electronics", "quantity": 2, "total_amount": 100.0, "status": "COMPLETED"},
        {"order_id": "O2", "customer_id": "C2", "order_date": "2026-03-01", "shipping_country": "CO", "category": "Electronics", "quantity": 1, "total_amount": 50.0, "status": "COMPLETED"},
    ]
    df_orders = pd.DataFrame(orders_data)

    aggregator = GoldAggregator()

    # Ejecución 1
    run_1 = aggregator.build_daily_sales(df_orders)
    # Ejecución 2 (reproceso con los mismos datos)
    run_2 = aggregator.build_daily_sales(df_orders)

    pd.testing.assert_frame_equal(
        run_1.drop(columns=["_gold_calculated_at"]),
        run_2.drop(columns=["_gold_calculated_at"]),
    )
    assert len(run_1) == len(run_2)
    assert run_1["total_revenue"].sum() == run_2["total_revenue"].sum()
