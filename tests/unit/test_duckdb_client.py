import pytest
from app.backend.query_engine.duckdb_client import DuckDBClient


@pytest.fixture
def client():
    # Usa los datos mock de data/mock/gold generados
    return DuckDBClient("data/mock/gold")


def test_duckdb_is_healthy(client):
    assert client.is_healthy() is True


def test_duckdb_registered_views(client):
    views = client.registered_views
    assert "v_daily_sales" in views
    assert "v_customer_360" in views
    assert "v_product_performance" in views
    assert "v_pipeline_execution_logs" in views


def test_duckdb_execute_query(client):
    sql = "SELECT shipping_country, SUM(total_revenue) AS rev FROM v_daily_sales GROUP BY shipping_country"
    res = client.execute_query(sql)
    assert res["success"] is True
    assert len(res["data"]) > 0
    assert "shipping_country" in res["columns"]
    assert "rev" in res["columns"]
    assert res["execution_time_ms"] >= 0


def test_duckdb_get_schemas(client):
    schemas = client.get_schemas()
    assert "v_daily_sales" in schemas
    col_names = [c["name"] for c in schemas["v_daily_sales"]]
    assert "order_date" in col_names
    assert "total_revenue" in col_names


def test_duckdb_daily_sales_summary(client):
    summary = client.get_daily_sales_summary(country="Colombia")
    kpis = summary["kpis"]
    assert "total_revenue" in kpis
    assert kpis["total_revenue"] > 0
    assert "total_orders" in kpis
    assert len(summary["trend"]) > 0
