"""
src/ingestion/json_connector.py
-------------------------------
Conector especializado para archivos JSON y JSON Lines (NDJSON) hacia Landing en MinIO (US-04).
Responsable: Jefri Calderin.

Características:
- Soporte para JSON estructurado (array de objetos o clave de datos) y NDJSON.
- Validación de sintaxis jerárquica.
- Inmutabilidad con verificación SHA-256 en Landing.
- Compatible con el contrato BaseConnector de la plataforma.
"""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.ingestion.base_connector import BaseConnector, IngestionResult

logger = get_logger("JSONConnector")


class JSONConnector(BaseConnector):
    """
    Conector robusto para la ingesta de archivos JSON / NDJSON hacia MinIO Landing.
    """

    def __init__(
        self,
        file_path: Optional[str] = None,
        source_name: str = "json_connector",
        config: Optional[Dict[str, Any]] = None,
        s3_client: Optional[S3Client] = None,
        json_lines: bool = False,
        data_key: Optional[str] = None,
    ):
        super().__init__(source_name=source_name, config=config)
        self.file_path = file_path or (self.config.get("file_path") if self.config else None)
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.json_lines = json_lines or bool(self.config.get("json_lines", False) if self.config else False)
        self.data_key = data_key or (self.config.get("data_key") if self.config else None)

    def extract(self) -> List[Dict[str, Any]]:
        """
        Lee y valida los registros JSON o JSON Lines desde el archivo origen.
        """
        if not self.file_path:
            raise ValueError("No se ha configurado 'file_path' en JSONConnector")

        path = Path(self.file_path)
        if not path.exists():
            raise FileNotFoundError(f"Archivo JSON no encontrado: {self.file_path}")

        raw_bytes = path.read_bytes()
        if len(raw_bytes) == 0:
            return []

        text = raw_bytes.decode("utf-8", errors="replace").strip()
        records: List[Dict[str, Any]] = []

        if self.json_lines or ("\n" in text and not text.startswith("[")):
            # Formato NDJSON / JSON Lines
            for line_idx, line in enumerate(text.splitlines(), start=1):
                clean_line = line.strip()
                if not clean_line:
                    continue
                try:
                    obj = json.loads(clean_line)
                    if isinstance(obj, dict):
                        records.append(obj)
                    else:
                        raise ValueError(f"Línea {line_idx} no es un diccionario JSON.")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error de sintaxis NDJSON en línea {line_idx} de {self.file_path}: {e}")
        else:
            # Formato JSON estándar
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    if self.data_key and self.data_key in data:
                        records = data[self.data_key]
                    elif "data" in data and isinstance(data["data"], list):
                        records = data["data"]
                    else:
                        records = [data]
                else:
                    raise ValueError(f"Estructura JSON no reconocida en {self.file_path}")
            except json.JSONDecodeError as e:
                raise ValueError(f"Error de sintaxis JSON en {self.file_path}: {e}")

        return records

    def ingest(
        self,
        destination_bucket: Optional[str] = None,
        destination_prefix: str = "json_sources"
    ) -> IngestionResult:
        """
        Carga el archivo JSON inalterado en Landing en MinIO registrando su hash SHA-256.
        """
        started_at = datetime.now(timezone.utc)
        bucket = destination_bucket or self.settings.get_bucket("landing")

        if not self.file_path:
            return IngestionResult(
                source_name=self.source_name,
                destination_uri=f"s3://{bucket}/{destination_prefix}",
                records_count=0,
                bytes_written=0,
                status="FAILED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                error_message="No se ha configurado 'file_path'",
            )

        path = Path(self.file_path)
        if not path.exists():
            return IngestionResult(
                source_name=self.source_name,
                destination_uri=f"s3://{bucket}/{destination_prefix}",
                records_count=0,
                bytes_written=0,
                status="FAILED",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                error_message=f"Archivo no encontrado: {self.file_path}",
            )

        try:
            records = self.extract()
            raw_bytes = path.read_bytes()
            sha256_hash = hashlib.sha256(raw_bytes).hexdigest()

            date_prefix = started_at.strftime("%Y/%m/%d")
            file_id = str(uuid.uuid4())[:8]
            object_key = f"{destination_prefix}/{date_prefix}/{path.stem}_{file_id}.json"

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
                    "format": "json_lines" if self.json_lines else "json",
                    "sha256": sha256_hash,
                    "original_filename": path.name,
                },
            )
            logger.info(f"Ingesta JSON exitosa: {len(records)} registros ({len(raw_bytes)} bytes) -> {uri}")
            return result

        except Exception as e:
            finished_at = datetime.now(timezone.utc)
            logger.error(f"Fallo en ingesta JSON para {self.file_path}: {e}")
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
