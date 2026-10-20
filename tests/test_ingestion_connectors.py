"""
tests/test_ingestion_connectors.py
----------------------------------
Pruebas unitarias para conectores CSV, JSON y REST API de src/ingestion.
Responsable: Jefri Calderin (US-04, US-05).
"""

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock
import pytest
import requests

from src.ingestion import (
    CSVConnector,
    JSONConnector,
    RestApiConnector,
    IngestionResult
)


@pytest.fixture
def mock_s3_client():
    client = MagicMock()
    client.upload_bytes.return_value = "s3://landing-mock-bucket/test_object.csv"
    return client


def test_csv_connector_extract_and_ingest(tmp_path, mock_s3_client):
    """Verifica extracción y carga a Landing de un archivo CSV válido."""
    csv_file = tmp_path / "orders_test.csv"
    csv_file.write_text(
        "order_id,customer_id,total_amount,status\n"
        "ORD-001,CUST-001,150.50,COMPLETED\n"
        "ORD-002,CUST-002,230.00,COMPLETED\n"
        "ORD-003,CUST-003,45.00,CANCELLED\n",
        encoding="utf-8"
    )

    connector = CSVConnector(
        file_path=str(csv_file),
        source_name="orders_csv",
        s3_client=mock_s3_client,
        expected_columns=["order_id", "customer_id", "total_amount", "status"]
    )

    records = connector.extract()
    assert len(records) == 3
    assert records[0]["order_id"] == "ORD-001"
    assert records[1]["customer_id"] == "CUST-002"

    result = connector.ingest(destination_bucket="landing-bucket", destination_prefix="orders")
    assert result.status == "SUCCESS"
    assert result.records_count == 3
    assert result.bytes_written > 0
    assert mock_s3_client.upload_bytes.called


def test_csv_connector_corrupt_file_handling(tmp_path, mock_s3_client):
    """Verifica captura segura de errores y reporte de status FAILED ante CSV corrupto."""
    corrupt_file = tmp_path / "corrupt.csv"
    corrupt_file.write_text(
        "col_a,col_b,col_c\n"
        "1,2\n"  # Faltante de columna
        "4,5,6\n",
        encoding="utf-8"
    )

    connector = CSVConnector(
        file_path=str(corrupt_file),
        s3_client=mock_s3_client
    )

    # extract() debe fallar informando la inconsistencia
    with pytest.raises(ValueError):
        connector.extract()

    # ingest() debe capturar la falla sin caerse y retornar FAILED
    result = connector.ingest(destination_bucket="landing-bucket")
    assert result.status == "FAILED"
    assert result.error_message is not None


def test_json_connector_standard_and_ndjson(tmp_path, mock_s3_client):
    """Verifica ingesta de JSON estándar y formato NDJSON (JSON Lines)."""
    # 1. JSON Array estándar
    json_file = tmp_path / "customers.json"
    customers = [
        {"customer_id": "C-1", "name": "Ana", "country": "CO"},
        {"customer_id": "C-2", "name": "Carlos", "country": "MX"}
    ]
    json_file.write_text(json.dumps(customers), encoding="utf-8")

    json_conn = JSONConnector(
        file_path=str(json_file),
        s3_client=mock_s3_client
    )
    recs = json_conn.extract()
    assert len(recs) == 2
    res = json_conn.ingest(destination_prefix="customers")
    assert res.status == "SUCCESS"
    assert res.records_count == 2

    # 2. NDJSON / JSON Lines
    ndjson_file = tmp_path / "events.ndjson"
    ndjson_file.write_text(
        '{"event": "login", "user": "u1"}\n'
        '{"event": "click", "user": "u2"}\n'
        '{"event": "purchase", "user": "u1"}\n',
        encoding="utf-8"
    )

    ndjson_conn = JSONConnector(
        file_path=str(ndjson_file),
        s3_client=mock_s3_client,
        json_lines=True
    )
    recs_nd = ndjson_conn.extract()
    assert len(recs_nd) == 3
    res_nd = ndjson_conn.ingest(destination_prefix="events")
    assert res_nd.status == "SUCCESS"
    assert res_nd.records_count == 3


def test_rest_api_connector_pagination_and_retry(monkeypatch, mock_s3_client):
    """
    Verifica que RestApiConnector soporte paginación, credenciales seguras
    y reintento con backoff ante error 429 transitorio.
    """
    monkeypatch.setenv("SALES_API_TOKEN", "bearer_token_12345")
    call_counts = {"page_1": 0, "page_2": 0}

    def mock_get(url, headers=None, params=None, timeout=None):
        assert headers.get("Authorization") == "Bearer bearer_token_12345"
        page = params.get("page", 1)

        resp = MagicMock()
        resp.headers = {}

        if page == 1:
            call_counts["page_1"] += 1
            resp.status_code = 200
            resp.json.return_value = {
                "page": 1,
                "total_pages": 2,
                "data": [{"id": 1, "product": "Laptop"}, {"id": 2, "product": "Mouse"}]
            }
            return resp

        elif page == 2:
            call_counts["page_2"] += 1
            if call_counts["page_2"] == 1:
                # Falla 429 transitoria
                resp.status_code = 429
                resp.headers = {"Retry-After": "0.01"}
                return resp
            else:
                # Éxito tras reintento
                resp.status_code = 200
                resp.json.return_value = {
                    "page": 2,
                    "total_pages": 2,
                    "data": [{"id": 3, "product": "Keyboard"}]
                }
                return resp

        resp.status_code = 404
        return resp

    mock_session = MagicMock()
    mock_session.get.side_effect = mock_get

    connector = RestApiConnector(
        base_url="https://api.retail.com",
        endpoint="v1/products",
        s3_client=mock_s3_client,
        auth_type="bearer",
        token_env_var="SALES_API_TOKEN",
        pagination_type="page_number",
        page_size=2,
        max_retries=2,
        backoff_factor=0.01,
        session=mock_session
    )

    records = connector.extract()
    assert len(records) == 3
    assert call_counts["page_1"] == 1
    assert call_counts["page_2"] == 2  # 1 fallido 429 + 1 exitoso

    result = connector.ingest(destination_prefix="products_api")
    assert result.status == "SUCCESS"
    assert result.records_count == 3
    assert mock_s3_client.upload_bytes.called
