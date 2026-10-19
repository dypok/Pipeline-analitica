"""
tests/unit/test_connectors.py
-----------------------------
Pruebas unitarias para conectores de ingesta (US-04 y US-05).
Responsable: Jefri Calderin.

Cubre:
- Prueba T04.1 (P01): CSVConnector procesa CSV válido, preserva SHA-256 en Landing y genera Parquet con auditoría en Bronze.
- Prueba T04.2 (P03): CSVConnector rechaza CSV corrupto/desfasado, status FAILED y no genera Bronze roto.
- JSONConnector: Ingesta de JSON estructurado y JSON Lines (NDJSON) con aplanado columnar en Bronze.
- Prueba T05.1 (P02): RestApiConnector maneja paginación de 3 páginas y reintenta con éxito ante error 429 transitorio.
- Seguridad de credenciales: Inyección desde variables de entorno.
"""

import hashlib
import json
import os
from pathlib import Path
from unittest.mock import MagicMock
import pandas as pd
import pytest
import requests

from ingestion import (
    BaseConnector,
    IngestionConfig,
    IngestionResult,
    CSVConnector,
    JSONConnector,
    RestApiConnector,
    StorageAdapter
)


@pytest.fixture
def temp_storage(tmp_path):
    return StorageAdapter(base_dir=str(tmp_path / "storage"))


def test_t04_1_csv_connector_valid(tmp_path, temp_storage):
    """
    Prueba T04.1 (P01):
    Carga un CSV de transacciones válido.
    Verifica:
    1. Objeto idéntico en Landing con coincidencia exacta de hash SHA-256.
    2. Existencia de tabla Parquet en Bronze legible con columnas de auditoría:
       - _ingestion_id
       - _ingested_at
       - _source_file
    3. Manifiesto emitido con status='SUCCESS' y conteo correcto de filas.
    """
    csv_file = tmp_path / "sales_valid.csv"
    content = (
        "order_id,customer_id,total_amount,order_status\n"
        "ORD-001,CUST-10,150.50,completed\n"
        "ORD-002,CUST-20,89.00,completed\n"
        "ORD-003,CUST-30,45.20,cancelled\n"
    )
    csv_file.write_text(content, encoding="utf-8")
    original_sha256 = hashlib.sha256(csv_file.read_bytes()).hexdigest()

    config = IngestionConfig(
        source_id="csv_sales_source",
        source_type="CSV",
        dataset_name="sales_transactions",
        storage_base_dir=str(temp_storage.base_dir)
    )

    connector = CSVConnector(
        file_path=str(csv_file),
        config=config
    )
    connector.storage = temp_storage

    result = connector.execute()

    assert result.status == "SUCCESS"
    assert result.record_count == 3
    assert result.sha256_hash == original_sha256
    assert result.landing_path is not None
    assert result.bronze_path is not None

    # Verificar que el archivo en Landing tenga exactamente el mismo contenido
    with open(result.landing_path, "rb") as f:
        landing_bytes = f.read()
    assert hashlib.sha256(landing_bytes).hexdigest() == original_sha256

    # Verificar que Bronze sea un Parquet válido con columnas de auditoría
    df_bronze = pd.read_parquet(result.bronze_path)
    assert len(df_bronze) == 3
    assert "_ingestion_id" in df_bronze.columns
    assert "_ingested_at" in df_bronze.columns
    assert "_source_file" in df_bronze.columns
    assert df_bronze["_source_file"].iloc[0] == "sales_valid.csv"


def test_t04_2_csv_connector_corrupt_fails_safely(tmp_path, temp_storage):
    """
    Prueba T04.2 (P03):
    Proporciona un CSV corrupto con columnas desfasadas.
    Verifica:
    1. El conector captura la excepción.
    2. Retorna status='FAILED'.
    3. Registra el mensaje de error.
    4. NO crea un archivo Bronze roto.
    """
    corrupt_csv = tmp_path / "corrupt_sales.csv"
    # Fila 2 tiene 3 columnas, pero encabezado tiene 4
    content = (
        "order_id,customer_id,total_amount,order_status\n"
        "ORD-001,CUST-10,150.50\n"
        "ORD-002,CUST-20,89.00,completed\n"
    )
    corrupt_csv.write_text(content, encoding="utf-8")

    config = IngestionConfig(
        source_id="csv_corrupt_source",
        source_type="CSV",
        dataset_name="sales_transactions",
        storage_base_dir=str(temp_storage.base_dir)
    )

    connector = CSVConnector(
        file_path=str(corrupt_csv),
        config=config
    )
    connector.storage = temp_storage

    result = connector.execute()

    assert result.status == "FAILED"
    assert result.error_message is not None
    assert result.bronze_path is None


def test_json_connector_array_and_ndjson(tmp_path, temp_storage):
    """
    Prueba JSONConnector para formato array estándar y formato JSON Lines (NDJSON).
    """
    # 1. JSON estándar
    json_file = tmp_path / "customers_test.json"
    customers = [
        {"customer_id": "C-1", "name": "Alice", "city": "Bogota"},
        {"customer_id": "C-2", "name": "Bob", "city": "Medellin"}
    ]
    json_file.write_text(json.dumps(customers), encoding="utf-8")

    config = IngestionConfig(
        source_id="json_crm",
        source_type="JSON",
        dataset_name="customers",
        storage_base_dir=str(temp_storage.base_dir)
    )

    connector = JSONConnector(
        file_path=str(json_file),
        config=config
    )
    connector.storage = temp_storage

    result = connector.execute()

    assert result.status == "SUCCESS"
    assert result.record_count == 2
    assert result.landing_path is not None
    assert result.bronze_path is not None

    df_bronze = pd.read_parquet(result.bronze_path)
    assert len(df_bronze) == 2
    assert "_ingestion_id" in df_bronze.columns
    assert "city" in df_bronze.columns

    # 2. JSON Lines (NDJSON)
    ndjson_file = tmp_path / "events.ndjson"
    ndjson_content = (
        '{"event_id": "E1", "type": "click"}\n'
        '{"event_id": "E2", "type": "view"}\n'
        '{"event_id": "E3", "type": "purchase"}\n'
    )
    ndjson_file.write_text(ndjson_content, encoding="utf-8")

    config_ndjson = IngestionConfig(
        source_id="events_stream",
        source_type="JSON",
        dataset_name="events",
        storage_base_dir=str(temp_storage.base_dir)
    )

    connector_ndjson = JSONConnector(
        file_path=str(ndjson_file),
        config=config_ndjson,
        json_lines=True
    )
    connector_ndjson.storage = temp_storage

    result_ndjson = connector_ndjson.execute()

    assert result_ndjson.status == "SUCCESS"
    assert result_ndjson.record_count == 3
    df_ndjson = pd.read_parquet(result_ndjson.bronze_path)
    assert len(df_ndjson) == 3


def test_t05_1_rest_api_connector_pagination_and_retry(monkeypatch, temp_storage):
    """
    Prueba T05.1 (P02):
    Simula una API REST con paginación de 3 páginas (100 registros cada una = 300 en total).
    En la página 2 simula una falla transitoria HTTP 429 (Too Many Requests).
    Verifica:
    1. El conector reintenta ante 429 y recupera la respuesta exitosa.
    2. Descarga las 3 páginas completas (300 registros).
    3. Almacena el JSON crudo en Landing.
    4. Genera un único Parquet en Bronze con record_count = 300.
    5. Manifiesto status='SUCCESS'.
    """
    call_counts = {"page_1": 0, "page_2": 0, "page_3": 0}

    def mock_get(url, headers=None, params=None, timeout=None):
        page = params.get("page", 1) if params else 1

        mock_resp = MagicMock()
        mock_resp.headers = {}

        if page == 1:
            call_counts["page_1"] += 1
            mock_resp.status_code = 200
            data = [{"order_id": f"ORD-P1-{i:03d}", "amount": 10.0 + i} for i in range(1, 101)]
            mock_resp.json.return_value = {
                "page": 1,
                "page_size": 100,
                "total_pages": 3,
                "total_records": 300,
                "data": data
            }
            return mock_resp

        elif page == 2:
            call_counts["page_2"] += 1
            if call_counts["page_2"] == 1:
                # Primer intento: falla transitoria 429
                mock_resp.status_code = 429
                mock_resp.headers = {"Retry-After": "0.1"}
                return mock_resp
            else:
                # Segundo intento tras reintento: éxito 200
                mock_resp.status_code = 200
                data = [{"order_id": f"ORD-P2-{i:03d}", "amount": 20.0 + i} for i in range(1, 101)]
                mock_resp.json.return_value = {
                    "page": 2,
                    "page_size": 100,
                    "total_pages": 3,
                    "total_records": 300,
                    "data": data
                }
                return mock_resp

        elif page == 3:
            call_counts["page_3"] += 1
            mock_resp.status_code = 200
            data = [{"order_id": f"ORD-P3-{i:03d}", "amount": 30.0 + i} for i in range(1, 101)]
            mock_resp.json.return_value = {
                "page": 3,
                "page_size": 100,
                "total_pages": 3,
                "total_records": 300,
                "data": data
            }
            return mock_resp

        mock_resp.status_code = 404
        return mock_resp

    mock_session = MagicMock()
    mock_session.get.side_effect = mock_get

    # Test con autenticación por variable de entorno
    monkeypatch.setenv("SALES_API_TOKEN", "secret-token-xyz")

    config = IngestionConfig(
        source_id="rest_api_external_sales",
        source_type="REST_API",
        dataset_name="api_orders",
        storage_base_dir=str(temp_storage.base_dir)
    )

    connector = RestApiConnector(
        base_url="https://api.external-sales.com",
        endpoint="v1/orders",
        config=config,
        auth_type="bearer",
        token_env_var="SALES_API_TOKEN",
        pagination_type="page_number",
        page_size=100,
        max_pages=5,
        max_retries=3,
        backoff_factor=0.05,
        custom_session=mock_session
    )
    connector.storage = temp_storage

    result = connector.execute()

    # Verificaciones de la prueba T05.1
    assert result.status == "SUCCESS"
    assert result.record_count == 300
    assert call_counts["page_1"] == 1
    assert call_counts["page_2"] == 2  # 1 intento fallido (429) + 1 reintento exitoso
    assert call_counts["page_3"] == 1

    # Verificar Landing
    assert result.landing_path is not None
    with open(result.landing_path, "r", encoding="utf-8") as f:
        landing_json = json.load(f)
    assert landing_json["pages_fetched"] == 3

    # Verificar Bronze
    assert result.bronze_path is not None
    df_bronze = pd.read_parquet(result.bronze_path)
    assert len(df_bronze) == 300
    assert "_ingestion_id" in df_bronze.columns
    assert "_ingested_at" in df_bronze.columns
    assert "_source_file" in df_bronze.columns
