"""
ingestion/rest_api_connector.py
-------------------------------
Conector reutilizable para consumo de APIs REST con paginación y resiliencia (US-05).
Responsable: Jefri Calderin.

Características:
- Estrategias de paginación:
    1. Basada en número de página ('page' y 'page_size').
    2. Basada en desplazamiento ('offset' y 'limit').
    3. Basada en cursores / tokens ('next_token' o 'cursor').
- Resiliencia y tolerancia a fallos:
    - Reintentos exponenciales con backoff y jitter ante códigos 429, 500, 502, 503, 504.
    - Soporte explícito a la cabecera 'Retry-After' en errores 429 (Too Many Requests).
    - Rate limiting preventivo (delay configurable entre peticiones).
- Seguridad:
    - Carga de credenciales exclusivamente desde variables de entorno (Bearer token, API Key o Basic Auth).
- Almacenamiento Medallion:
    - Landing: guarda el payload JSON crudo consolidado de todas las páginas con su hash SHA-256.
    - Bronze: aplana todos los registros y escribe en Parquet con columnas técnicas de auditoría.
"""

import json
import os
import random
import time
from typing import Any, Callable, Optional, Union
from urllib.parse import urljoin
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from ingestion.base_connector import BaseConnector, IngestionConfig, IngestionResult


class RestApiConnector(BaseConnector):
    """
    Conector resiliente para extracción de datos desde endpoints REST HTTP/HTTPS.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        endpoint: str = "",
        config: Optional[IngestionConfig] = None,
        auth_type: str = "none",  # "bearer", "api_key", "basic", "none"
        token_env_var: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        pagination_type: str = "page_number",  # "page_number", "offset_limit", "cursor"
        page_size: int = 100,
        max_pages: int = 50,
        records_json_path: str = "data",
        cursor_json_path: str = "next_token",
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        rate_limit_delay: float = 0.0,
        custom_session: Optional[requests.Session] = None
    ):
        super().__init__(config=config)
        self.base_url = (base_url or "").rstrip("/")
        self.endpoint = endpoint.lstrip("/")
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
        self.rate_limit_delay = rate_limit_delay
        self.session = custom_session or requests.Session()

        self._collected_pages_data: list[dict] = []
        self._flattened_records: list[dict] = []

    def configure(self, config: dict[str, Any]) -> None:
        """Configura dinámicamente los parámetros de conexión."""
        if "base_url" in config:
            self.base_url = config["base_url"].rstrip("/")
        if "endpoint" in config:
            self.endpoint = config["endpoint"].lstrip("/")
        if "auth_type" in config:
            self.auth_type = config["auth_type"].lower()
        if "token_env_var" in config:
            self.token_env_var = config["token_env_var"]
        if "api_key_header" in config:
            self.api_key_header = config["api_key_header"]
        if "pagination_type" in config:
            self.pagination_type = config["pagination_type"]
        if "page_size" in config:
            self.page_size = config["page_size"]
        if "max_pages" in config:
            self.max_pages = config["max_pages"]
        if "records_json_path" in config:
            self.records_json_path = config["records_json_path"]
        if "cursor_json_path" in config:
            self.cursor_json_path = config["cursor_json_path"]
        if "max_retries" in config:
            self.max_retries = config["max_retries"]
        if "backoff_factor" in config:
            self.backoff_factor = config["backoff_factor"]
        if "rate_limit_delay" in config:
            self.rate_limit_delay = config["rate_limit_delay"]

    def _build_headers(self) -> dict[str, str]:
        """Construye las cabeceras HTTP incluyendo autenticación segura desde entorno."""
        headers = {
            "Accept": "application/json",
            "User-Agent": "MedallionIngestionConnector/1.0"
        }

        if self.auth_type == "bearer" and self.token_env_var:
            token = os.getenv(self.token_env_var)
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                self.logger.warning(f"Variable de entorno de token no encontrada: {self.token_env_var}")

        elif self.auth_type == "api_key" and self.token_env_var:
            api_key = os.getenv(self.token_env_var)
            if api_key:
                headers[self.api_key_header] = api_key
            else:
                self.logger.warning(f"Variable de entorno de API Key no encontrada: {self.token_env_var}")

        return headers

    def validate(self) -> bool:
        """Valida que la URL base esté configurada correctamente."""
        if not self.base_url:
            raise ValueError("No se ha configurado 'base_url' para el RestApiConnector")
        return True

    def _request_with_retry(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Ejecuta una petición GET con reintentos exponenciales ante 429, 500, 502, 503, 504.
        Respeta la cabecera Retry-After cuando está disponible.
        """
        headers = self._build_headers()
        attempt = 0

        while attempt <= self.max_retries:
            attempt += 1
            try:
                response = self.session.get(url, headers=headers, params=params, timeout=15)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    # Rate limit excedido
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            wait_time = self.backoff_factor * (2 ** (attempt - 1))
                    else:
                        wait_time = self.backoff_factor * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)

                    self.logger.warning(
                        f"HTTP 429 (Too Many Requests). Reintento {attempt}/{self.max_retries} en {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    continue

                if response.status_code in [500, 502, 503, 504]:
                    wait_time = self.backoff_factor * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
                    self.logger.warning(
                        f"HTTP {response.status_code} transitorio. Reintento {attempt}/{self.max_retries} en {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt > self.max_retries:
                    raise
                wait_time = self.backoff_factor * (2 ** (attempt - 1))
                self.logger.warning(
                    f"Error de red '{type(e).__name__}'. Reintento {attempt}/{self.max_retries} en {wait_time:.2f}s..."
                )
                time.sleep(wait_time)

        raise RuntimeError(f"Agotados los {self.max_retries} reintentos en {url}")

    def _extract_records(self, payload: Any) -> list[dict]:
        """Extrae la lista de registros del cuerpo de la respuesta."""
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if self.records_json_path in payload and isinstance(payload[self.records_json_path], list):
                return payload[self.records_json_path]
            for candidate in ["data", "items", "records", "results"]:
                if candidate in payload and isinstance(payload[candidate], list):
                    return payload[candidate]
            # Si no hay lista anidada pero es un diccionario, tratar como registro único
            return [payload]
        return []

    def fetch(self) -> dict[str, Any]:
        """
        Extrae recursiva o iterativamente todas las páginas del endpoint REST.
        Consolida los payloads de Landing y los registros planos para Bronze.
        """
        full_url = f"{self.base_url}/{self.endpoint}" if self.endpoint else self.base_url
        self._collected_pages_data = []
        self._flattened_records = []

        current_page = 1
        current_offset = 0
        cursor: Optional[str] = None

        while current_page <= self.max_pages:
            params: dict[str, Any] = {}

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

            self.logger.info(f"Consultando página {current_page} en {full_url} con params={params}")
            payload = self._request_with_retry(full_url, params)

            self._collected_pages_data.append(payload)
            page_records = self._extract_records(payload)
            self._flattened_records.extend(page_records)

            self.logger.info(f"Página {current_page} obtenida: {len(page_records)} registros recolectados")

            # Comprobar condición de fin de paginación
            if not page_records:
                break

            if self.pagination_type == "cursor":
                cursor = payload.get(self.cursor_json_path) if isinstance(payload, dict) else None
                if not cursor:
                    break
            elif self.pagination_type == "page_number":
                total_pages = payload.get("total_pages") if isinstance(payload, dict) else None
                if total_pages and current_page >= total_pages:
                    break
                if len(page_records) < self.page_size:
                    break
            elif self.pagination_type == "offset_limit":
                current_offset += len(page_records)
                total_records = payload.get("total_records") if isinstance(payload, dict) else None
                if total_records and current_offset >= total_records:
                    break
                if len(page_records) < self.page_size:
                    break

            current_page += 1

            if self.rate_limit_delay > 0:
                time.sleep(self.rate_limit_delay)

        return {
            "endpoint": full_url,
            "pages_fetched": len(self._collected_pages_data),
            "total_records_extracted": len(self._flattened_records),
            "pages": self._collected_pages_data
        }

    def save_landing(self, raw_data: dict[str, Any], source_filename: str) -> tuple[str, str]:
        """Guarda el payload consolidado en formato JSON en Landing."""
        source_id = self.config.source_id if self.config else "rest_api"
        relative_key = self.storage.build_landing_key(
            source_id=source_id,
            ingestion_id=self.ingestion_id,
            extension="json"
        )
        json_bytes = json.dumps(raw_data, indent=2, ensure_ascii=False).encode("utf-8")
        return self.storage.save_landing_bytes(relative_key, json_bytes)

    def save_bronze(self, data: dict[str, Any], source_filename: str) -> tuple[str, int]:
        """
        Aplana los registros recolectados de todas las páginas y los escribe en Parquet en Bronze.
        """
        records = self._flattened_records

        if not records:
            df = pd.DataFrame()
        else:
            df = pd.json_normalize(records)

        # Convertir objetos/listas a string JSON para que Parquet tenga tipos consistentes
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

        df = self.add_audit_columns(df, source_filename)
        dataset_name = self.config.dataset_name if self.config else "dataset"
        relative_key = self.storage.build_bronze_key(
            dataset_name=dataset_name,
            ingestion_id=self.ingestion_id
        )

        bronze_path = self.storage.save_bronze_parquet(relative_key, df)
        return bronze_path, len(df)

    def execute(self) -> IngestionResult:
        """Ejecuta la ingesta desde el endpoint REST configurado."""
        source_name = f"{self.endpoint or 'root'}_api.json"
        return self.run(source_filename=source_name)
