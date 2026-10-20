from fastapi.testclient import TestClient
from app.backend.main import app

client = TestClient(app)


def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["version"] == "2.0.0"
    assert "docs" in data


def test_health_endpoint():
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["duckdb_connected"] is True
    assert "v_daily_sales" in data["registered_views"]


def test_catalog_schemas_endpoint():
    response = client.get("/api/v1/catalog/schemas")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "v_daily_sales" in data["schemas"]


def test_daily_sales_metrics_endpoint():
    response = client.get("/api/v1/metrics/daily-sales?country=Colombia")
    assert response.status_code == 200
    data = response.json()
    assert "kpis" in data
    assert "total_revenue" in data["kpis"]
    assert "trend" in data


def test_top_products_endpoint():
    response = client.get("/api/v1/metrics/top-products?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) <= 5


def test_execute_query_valid():
    payload = {
        "sql": "SELECT shipping_country, SUM(orders_count) AS orders FROM v_daily_sales GROUP BY shipping_country",
        "max_rows": 10,
    }
    response = client.post("/api/v1/query/execute", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) > 0


def test_execute_query_forbidden_drop_fails():
    payload = {"sql": "DROP TABLE v_daily_sales"}
    response = client.post("/api/v1/query/execute", json=payload)
    assert response.status_code == 400
    assert "Consulta no permitida" in response.json()["detail"]


def test_chatbot_ask_endpoint():
    payload = {"question": "¿Qué país generó más ingresos?"}
    response = client.post("/api/v1/chatbot/ask", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["sql"] is not None
    assert len(data["data"]) > 0
    assert data["chart_suggestion"] in ("bar", "line", "table", "metric")
