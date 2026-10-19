"""Pruebas de transformaciones Bronze -> Silver y Silver -> Gold."""

import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.quality.validator import DataQualityValidator


def test_silver_deduplication_and_typing():
    """Valida que la lógica de Silver elimine duplicados y aplique tipado numérico."""
    records = [
        {"order_id": "ORD-1", "customer_id": "C1", "order_date": "2026-02-01", "quantity": "2", "unit_price": "10.5", "total_amount": "21.0", "status": "COMPLETED"},
        # Duplicado de ORD-1 con monto actualizado
        {"order_id": "ORD-1", "customer_id": "C1", "order_date": "2026-02-01", "quantity": "3", "unit_price": "10.5", "total_amount": "31.5", "status": "COMPLETED"},
        {"order_id": "ORD-2", "customer_id": "C2", "order_date": "2026-02-02", "quantity": "1", "unit_price": "50.0", "total_amount": "50.0", "status": "COMPLETED"},
    ]

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["order_id"], keep="last")
    assert len(df) == 2

    # Verificar que el registro conservado de ORD-1 sea el último (total_amount = 31.5)
    row_1 = df[df["order_id"] == "ORD-1"].iloc[0]
    assert float(row_1["total_amount"]) == 31.5

    # Tipado y particionado
    df["total_amount"] = pd.to_numeric(df["total_amount"]).astype("float64")
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["year"] = pd.to_datetime(df["order_date"]).dt.year
    df["month"] = pd.to_datetime(df["order_date"]).dt.month

    assert df["year"].tolist() == [2026, 2026]
    assert df["month"].tolist() == [2, 2]


def test_gold_kpi_calculation():
    """Valida los cálculos agregados de la capa Gold."""
    data = {
        "order_id": ["O1", "O2", "O3", "O4"],
        "customer_id": ["C1", "C2", "C1", "C3"],
        "order_date": ["2026-02-10", "2026-02-10", "2026-02-10", "2026-02-11"],
        "category": ["Electronics", "Electronics", "Fashion", "Sports"],
        "total_amount": [100.0, 200.0, 50.0, 80.0],
        "status": ["COMPLETED", "COMPLETED", "CANCELLED", "COMPLETED"],
    }
    df = pd.DataFrame(data)

    # Agrupar por fecha
    group_10 = df[df["order_date"] == "2026-02-10"]
    completed_10 = group_10[group_10["status"] == "COMPLETED"]

    total_orders = len(group_10)
    completed_orders = len(completed_10)
    cancelled_orders = len(group_10[group_10["status"] == "CANCELLED"])
    total_revenue = float(completed_10["total_amount"].sum())
    avg_ticket = round(total_revenue / completed_orders, 2)
    distinct_customers = group_10["customer_id"].nunique()

    assert total_orders == 3
    assert completed_orders == 2
    assert cancelled_orders == 1
    assert total_revenue == 300.0
    assert avg_ticket == 150.0
    assert distinct_customers == 2
