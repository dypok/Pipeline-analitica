"""
tests/unit/test_mock_data.py
----------------------------
Pruebas unitarias para el generador de Mock Data (US-03).
Responsable: Jefri Calderin.

Valida:
- T03.1: Generación completa de archivos en <10s y existencia de todas las fuentes.
- T03.2: Coherencia e integridad referencial entre clientes, productos, órdenes e ítems.
- Inyección controlada de casos anómalos para Quarantine.
- Consistencia de datasets analíticos precocinados en Gold.
"""

import csv
import json
import subprocess
import sys
import time
from pathlib import Path
import pandas as pd
import pytest


def test_t03_1_mock_data_generation_execution(tmp_path):
    """
    Prueba T03.1: Ejecuta el script generate_mock_data.py con parámetros reducidos y verifica
    que culmine en menos de 10 segundos generando todos los archivos esperados.
    """
    raw_dir = tmp_path / "raw"
    gold_dir = tmp_path / "gold"
    api_dir = tmp_path / "api"

    script_path = Path("scripts/generate_mock_data.py").resolve()
    cmd = [
        sys.executable,
        str(script_path),
        "--records", "200",
        "--customers", "50",
        "--products", "20",
        "--output-dir", str(raw_dir),
        "--gold-dir", str(gold_dir),
        "--api-dir", str(api_dir),
        "--seed", "99"
    ]

    start_time = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - start_time

    assert result.returncode == 0, f"Error ejecutando generate_mock_data.py: {result.stderr}"
    assert elapsed < 10.0, f"La generación tardó demasiado: {elapsed:.2f} segundos"

    # Verificar existencia de archivos generados en raw
    assert (raw_dir / "customers.json").exists()
    assert (raw_dir / "products.csv").exists()
    assert (raw_dir / "products.json").exists()
    assert (raw_dir / "orders.csv").exists()
    assert (raw_dir / "order_items.csv").exists()
    assert (raw_dir / "returns.csv").exists()
    assert (raw_dir / "orders_dirty.csv").exists()
    assert (raw_dir / "corrupt" / "dirty_data.csv").exists()
    assert (raw_dir / "api_stream.json").exists()

    # Verificar archivos en gold
    assert (gold_dir / "daily_sales.parquet").exists()
    assert (gold_dir / "customer_360.parquet").exists()
    assert (gold_dir / "product_performance.parquet").exists()

    # Esquema US-07 (grano order_date, shipping_country, category) del Gold
    # precocinado que genera generate_mock_data.py.
    df_sales = pd.read_parquet(gold_dir / "daily_sales.parquet")
    expected_us07_cols = {"order_date", "shipping_country", "category", "total_revenue", "orders_count", "units_sold"}
    assert expected_us07_cols.issubset(set(df_sales.columns))
    assert len(df_sales) > 0


def test_t03_2_referential_integrity():
    """
    Prueba T03.2: Verifica la integridad referencial en el dataset estándar generado:
    - Cada customer_id en orders existe en customers.json.
    - Cada product_id en order_items existe en products.csv.
    - Cada order_id en order_items existe en orders.csv.
    """
    base_dir = Path("data/mock/raw")
    if not (base_dir / "customers.json").exists():
        pytest.skip("Dataset de mock data no inicializado aún en data/mock/raw")

    with open(base_dir / "customers.json", "r", encoding="utf-8") as f:
        customers = json.load(f)
    customer_ids = {c["customer_id"] for c in customers}

    df_orders = pd.read_csv(base_dir / "orders.csv")
    df_items = pd.read_csv(base_dir / "order_items.csv")
    df_products = pd.read_csv(base_dir / "products.csv")

    product_ids = set(df_products["product_id"])
    order_ids = set(df_orders["order_id"])

    # 1. Órdenes válidas apuntan a clientes existentes
    for cust_id in df_orders["customer_id"]:
        assert cust_id in customer_ids, f"customer_id {cust_id} en orders no existe en customers.json"

    # 2. Ítems apuntan a órdenes existentes
    for ord_id in df_items["order_id"]:
        assert ord_id in order_ids, f"order_id {ord_id} en order_items no existe en orders.csv"

    # 3. Ítems apuntan a productos existentes
    for prod_id in df_items["product_id"]:
        assert prod_id in product_ids, f"product_id {prod_id} en order_items no existe en products.csv"


def test_dirty_data_anomalies_present():
    """
    Verifica que el dataset de anomalías (Quarantine) contenga los errores de negocio esperados:
    - Fechas imposibles / futuras.
    - Clave foránea inexistente (999999).
    - Precios negativos o cantidad 0.
    - Claves nulas.
    """
    dirty_path = Path("data/mock/raw/orders_dirty.csv")
    if not dirty_path.exists():
        pytest.skip("Archivo orders_dirty.csv no disponible")

    df_dirty = pd.read_csv(dirty_path)
    assert len(df_dirty) > 0

    expected_errors = {
        "ERR_INVALID_DATE",
        "ERR_FUTURE_DATE",
        "ERR_DATE_PARSE",
        "ERR_FK_NOT_FOUND",
        "ERR_NEGATIVE_PRICE",
        "ERR_ZERO_QUANTITY",
        "ERR_NULL_KEY"
    }

    present_errors = set(df_dirty["expected_quarantine_error"])
    assert expected_errors.intersection(present_errors), "El archivo dirty no contiene errores de Quarantine esperados"


def test_gold_parquet_schemas():
    """
    Verifica que los archivos Parquet en data/mock/gold/ sean legibles y contengan
    las columnas y granos analíticos correctos definidos en la especificación Medallion.

    Separa explícitamente los dos modelos que coexisten en Gold:
    - ``daily_sales.parquet``: Gold US-07 (grano fecha×país×categoría, §7.2),
      consumido por el chatbot de Dylan (``v_daily_sales``).
    - ``daily_sales_kpis.parquet``: data mart §4.1 (grano 1 fila/día),
      consumido por el dashboard de Sergio (``analytics.daily_sales_kpis``).
    """
    gold_dir = Path("data/mock/gold")
    if not (gold_dir / "daily_sales.parquet").exists():
        pytest.skip("Archivos Parquet Gold no encontrados")

    # 1. daily_sales (Gold US-07, §7.2): grano fecha × país × categoría.
    #    Consumido por el backend de Dylan vía DuckDB (v_daily_sales).
    df_sales = pd.read_parquet(gold_dir / "daily_sales.parquet")
    expected_sales_us07_cols = {
        "order_date", "shipping_country", "category", "total_revenue",
        "orders_count", "units_sold", "avg_ticket", "returns_count", "refunded_amount",
    }
    assert expected_sales_us07_cols.issubset(set(df_sales.columns))
    assert len(df_sales) > 0

    # 2. daily_sales_kpis (data mart §4.1): grano 1 fila/día, consumido por el
    #    dashboard. Es el modelo ``analytics.daily_sales_kpis`` del contrato.
    df_kpis = pd.read_parquet(gold_dir / "daily_sales_kpis.parquet")
    expected_kpi_cols = {
        "kpi_date", "total_orders", "completed_orders", "cancelled_orders",
        "total_revenue", "average_ticket", "distinct_customers", "top_category",
    }
    assert expected_kpi_cols.issubset(set(df_kpis.columns))
    assert len(df_kpis) > 0

    # 3. customer_360 (US-07 + country para el choropleth)
    df_cust = pd.read_parquet(gold_dir / "customer_360.parquet")
    expected_cust_cols = {
        "customer_id", "first_purchase_date", "last_purchase_date", "total_orders",
        "lifetime_value", "units_bought", "avg_order_value", "favorite_category", "country",
    }
    assert expected_cust_cols.issubset(set(df_cust.columns))
    assert len(df_cust) > 0

    # 4. product_performance (US-07 + product_name/category para el dashboard)
    df_prod = pd.read_parquet(gold_dir / "product_performance.parquet")
    expected_prod_cols = {
        "product_id", "product_name", "category", "units_sold", "gross_revenue",
        "returns_count", "return_rate", "margin",
    }
    assert expected_prod_cols.issubset(set(df_prod.columns))
    assert len(df_prod) > 0
