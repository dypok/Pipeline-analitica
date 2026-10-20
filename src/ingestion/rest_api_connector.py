"""
src/ingestion/rest_api_connector.py
-----------------------------------
Conector reutilizable para consumo de APIs REST con paginación y resiliencia (US-05).
Responsable: Jefri Calderin.

Características:
- Estrategias de paginación:
    1. Basada en número de página ('page' y 'page_size').
    2. Basada en desplazamiento ('offset' y 'limit').
    3. Basada en cursor ('cursor' y 'next_token').
- Resiliencia con reintentos exponenciales, jitter y cabecera 'Retry-After' (errores 429, 500, 502, 503, 504).
- Autenticación segura mediante variables de entorno (Bearer token o API Key).
- Depósito inmutable en MinIO Landing con metadatos técnicos y hash SHA-256.
- Retorno de IngestionResult conforme al contrato BaseConnector.
"""

from datetime import datetime, timezone
import hashlib
import json
import os
import random
import time
from typing import Any, Dict, List, Optional
import uuid
import requests

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.ingestion.base_connector import BaseConnector, IngestionResult

logger = get_logger("RestApiConnector")


class RestApiConnector(BaseConnector):
    """
    Conector resiliente para extracción de datos desde APIs REST hacia MinIO Landing.
    """

    def __init__(
        self,
        base_url: str,
        endpoint: str = "",
        source_name: str = "rest_api_connector",
        config: Optional[Dict[str, Any]] = None,
        s3_client: Optional[S3Client] = None,
        auth_type: str = "none",  # "bearer", "api_key", "none"
        token_env_var: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        pagination_type: str = "page_number",  # "page_number", "offset_limit", "cursor"
        page_size: int = 100,
        max_pages: int = 50,
        records_json_path: str = "data",
        cursor_json_path: str = "next_token",
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        session: Optional[requests.Session] = None,
    ):
        super().__init__(source_name=source_name, config=config)
        self.base_url = (base_url or "").rstrip("/")
        self.endpoint = endpoint.lstrip("/")
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.auth_type = auth_type.lower()
        self.token_env_var = token_env_var
        self.api_key_header = api_key_header
        self.pagination_type = pagination_type
        self.page_size = page_size
        self.max_pages = max_pages
        self.records_json_path = records_json_path
        self.cursor_json_path = cursor_json_path
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = session or requests.Session()

    def _build_headers(self) -> Dict[str, str]:
        """Construye las cabeceras HTTP incluyendo tokens seguros desde el entorno."""
        headers = {
            "Accept": "application/json",
            "User-Agent": "MedallionIngestion/1.0",
        }
        if self.auth_type == "bearer" and self.token_env_var:
            token = os.getenv(self.token_env_var)
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                logger.warning(f"Variable de entorno {self.token_env_var} no configurada para Bearer auth")

        elif self.auth_type == "api_key" and self.token_env_var:
            api_key = os.getenv(self.token_env_var)
            if api_key:
                headers[self.api_key_header] = api_key
            else:
                logger.warning(f"Variable de entorno {self.token_env_var} no configurada para API Key")

        return headers

    def _request_with_retry(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Ejecuta petición GET con reintentos exponenciales y jitter ante 429 / 5xx."""
        headers = self._build_headers()
        attempt = 0

        while attempt <= self.max_retries:
            attempt += 1
            try:
                response = self.session.get(url, headers=headers, params=params, timeout=15)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            wait_time = self.backoff_factor * (2 ** (attempt - 1))
                    else:
                        wait_time = self.backoff_factor * (2 ** (attempt - 1)) + random.uniform(0.05, 0.2)

                    logger.warning(
                        f"HTTP 429 recibido. Reintentando intento {attempt}/{self.max_retries} en {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    continue

                if response.status_code in [500, 502, 503, 504]:
                    wait_time = self.backoff_factor * (2 ** (attempt - 1)) + random.uniform(0.05, 0.2)
                    logger.warning(
                        f"HTTP {response.status_code} transitorio. Reintentando {attempt}/{self.max_retries} en {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt > self.max_retries:
                    raise
                wait_time = self.backoff_factor * (2 ** (attempt - 1))
                logger.warning(f"Error de red '{e}'. Reintentando en {wait_time:.2f}s...")
                time.sleep(wait_time)

        raise RuntimeError(f"Agotados los {self.max_retries} reintentos en {url}")

    def _extract_records_from_payload(self, payload: Any) -> List[Dict[str, Any]]:
        """Extrae la lista de registros del payload JSON."""
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if self.records_json_path in payload and isinstance(payload[self.records_json_path], list):
                return payload[self.records_json_path]
            for key in ["data", "items", "records", "results"]:
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
            return [payload]
        return []

    def extract(self) -> List[Dict[str, Any]]:
        """
        Extrae todos los registros paginados desde el endpoint REST.
        """
        if not self.base_url:
            raise ValueError("No se ha configurado 'base_url' en RestApiConnector")

        full_url = f"{self.base_url}/{self.endpoint}" if self.endpoint else self.base_url
        all_records: List[Dict[str, Any]] = []

        current_page = 1
        current_offset = 0
        cursor: Optional[str] = None

        while current_page <= self.max_pages:
            params: Dict[str, Any] = {}
            if self.pagination_type == "page_number":
                params["page"] = current_page
                params["page_size"] = self.page_size
            elif self.pagination_type == "offset_limit":
                params["offset"] = current_offset
                params["limit"] = self.page_size
            elif self.pagination_type == "cursor":
                params["limit"] = self.page_size
                if cursor:
                    params["cursor"] = cursor

            logger.info(f"Petición GET a {full_url} con parámetros: {params}")
            payload = self._request_with_retry(full_url, params)

            records = self._extract_records_from_payload(payload)
            if not records:
                break

            all_records.extend(records)

            # Comprobar fin de paginación
            if self.pagination_type == "cursor":
                cursor = payload.get(self.cursor_json_path) if isinstance(payload, dict) else None
                if not cursor:
                    break
            elif self.pagination_type == "page_number":
                total_pages = payload.get("total_pages") if isinstance(payload, dict) else None
                if total_pages and current_page >= total_pages:
                    break
                if len(records) < self.page_size:
                    break
            elif self.pagination_type == "offset_limit":
                current_offset += len(records)
                total_records = payload.get("total_records") if isinstance(payload, dict) else None
                if total_records and current_offset >= total_records:
                    break
                if len(records) < self.page_size:
                    break

            current_page += 1

        return all_records

    def ingest(
        self,
        destination_bucket: Optional[str] = None,
        destination_prefix: str = "api_sources"
    ) -> IngestionResult:
        """
        Extrae los datos desde la API REST y los deposita en formato JSON crudo
        en la capa Landing de MinIO con SHA-256 e inmutabilidad garantizada.
        """
        started_at = datetime.now(timezone.utc)
        bucket = destination_bucket or self.settings.get_bucket("landing")

        try:
            records = self.extract()
            payload = {
                "source_url": f"{self.base_url}/{self.endpoint}",
                "extracted_at": started_at.isoformat(),
                "records_count": len(records),
                "data": records,
            }
            raw_bytes = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
            sha256_hash = hashlib.sha256(raw_bytes).hexdigest()

            date_prefix = started_at.strftime("%Y/%m/%d")
            file_id = str(uuid.uuid4())[:8]
            clean_endpoint = self.endpoint.replace("/", "_") or "root"
            object_key = f"{destination_prefix}/{date_prefix}/{clean_endpoint}_{file_id}.json"

            uri = self.s3_client.upload_bytes(
                raw_bytes,
                bucket_name=bucket,
                object_name=object_key,
                content_type="application/json; charset=utf-8",
            )
            finished_at = datetime.now(timezone.utc)

            result = IngestionResult(
                source_name=self.source_name,
                destination_uri=uri,
                records_count=len(records),
                bytes_written=len(raw_bytes),
                status="SUCCESS",
                started_at=started_at,
                finished_at=finished_at,
                metadata={
                    "object_key": object_key,
                    "bucket": bucket,
                    "format": "json",
                    "sha256": sha256_hash,
                    "endpoint": self.endpoint,
                },
            )
            logger.info(f"Ingesta REST API exitosa: {len(records)} registros en {uri}")
            return result

        except Exception as e:
            finished_at = datetime.now(timezone.utc)
            logger.error(f"Fallo en ingesta REST API ({self.base_url}/{self.endpoint}): {e}")
            return IngestionResult(
                source_name=self.source_name,
                destination_uri=f"s3://{bucket}/{destination_prefix}",
                records_count=0,
                bytes_written=0,
                status="FAILED",
                started_at=started_at,
                finished_at=finished_at,
                error_message=str(e),
            )
