"""
src/ingestion/csv_connector.py
------------------------------
Conector especializado para la ingesta de archivos CSV hacia la capa Landing de MinIO (US-04).
Responsable: Jefri Calderin.

Características:
- Lectura en bloques (chunks) o buffer para escalabilidad de memoria.
- Detección automática o parametrizada de delimitadores (, ; | \\t).
- Validación de encabezados y consistencia de filas.
- Preservación inmutable del archivo crudo en Landing con cálculo de hash SHA-256.
- Retorno de IngestionResult conforme al contrato BaseConnector.
"""

import csv
from datetime import datetime, timezone
import hashlib
import io
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.ingestion.base_connector import BaseConnector, IngestionResult

logger = get_logger("CSVConnector")


class CSVConnector(BaseConnector):
    """
    Conector robusto para extraer y depositar archivos CSV en MinIO Landing.
    """

    def __init__(
        self,
        file_path: Optional[str] = None,
        source_name: str = "csv_connector",
        config: Optional[Dict[str, Any]] = None,
        s3_client: Optional[S3Client] = None,
        delimiter: Optional[str] = None,
        expected_columns: Optional[List[str]] = None,
        chunk_size: int = 10000,
    ):
        super().__init__(source_name=source_name, config=config)
        self.file_path = file_path or (self.config.get("file_path") if self.config else None)
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.delimiter = delimiter or (self.config.get("delimiter") if self.config else None)
        self.expected_columns = expected_columns or (self.config.get("expected_columns") if self.config else None)
        self.chunk_size = chunk_size

    def _detect_delimiter(self, sample_bytes: bytes) -> str:
        """Detecta automáticamente el delimitador del archivo CSV."""
        sample_text = sample_bytes[:4096].decode("utf-8", errors="ignore")
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample_text, delimiters=[",", ";", "\t", "|"])
            return dialect.delimiter
        except Exception:
            return ","

    def extract(self) -> List[Dict[str, Any]]:
        """
        Lee el archivo CSV y retorna una lista de diccionarios representando los registros.
        Valida consistencia de columnas y delimitador.
        """
        if not self.file_path:
            raise ValueError("No se ha configurado 'file_path' en CSVConnector")

        path = Path(self.file_path)
        if not path.exists():
            raise FileNotFoundError(f"Archivo CSV no encontrado: {self.file_path}")

        raw_bytes = path.read_bytes()
        if len(raw_bytes) == 0:
            return []

        delim = self.delimiter or self._detect_delimiter(raw_bytes)
        self.delimiter = delim

        text = raw_bytes.decode("utf-8", errors="replace")
        stream = io.StringIO(text)
        reader = csv.DictReader(stream, delimiter=delim)

        headers = reader.fieldnames
        if not headers:
            raise ValueError(f"El archivo CSV {self.file_path} no contiene encabezados válidos.")

        if self.expected_columns:
            missing = [c for c in self.expected_columns if c not in headers]
            if missing:
                raise ValueError(f"Faltan columnas requeridas en el CSV {self.file_path}: {missing}")

        records: List[Dict[str, Any]] = []
        expected_count = len(headers)

        for row_idx, row in enumerate(reader, start=2):
            if None in row or any(v is None for v in row.values()) or len(row) != expected_count:
                raise ValueError(
                    f"Fila {row_idx} corrupta o con número desfasado de columnas en {self.file_path}."
                )
            records.append(dict(row))

        return records

    def ingest(
        self,
        destination_bucket: Optional[str] = None,
        destination_prefix: str = "csv_sources"
    ) -> IngestionResult:
        """
        Carga el contenido crudo original del archivo CSV en el bucket Landing de MinIO
        garantizando inmutabilidad y registrando su hash SHA-256.
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
            # Validar y extraer registros
            records = self.extract()
            raw_bytes = path.read_bytes()
            sha256_hash = hashlib.sha256(raw_bytes).hexdigest()

            date_prefix = started_at.strftime("%Y/%m/%d")
            file_id = str(uuid.uuid4())[:8]
            object_key = f"{destination_prefix}/{date_prefix}/{path.stem}_{file_id}.csv"

            uri = self.s3_client.upload_bytes(
                raw_bytes,
                bucket_name=bucket,
                object_name=object_key,
                content_type="text/csv; charset=utf-8",
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
                    "format": "csv",
                    "sha256": sha256_hash,
                    "delimiter": self.delimiter,
                    "original_filename": path.name,
                },
            )
            logger.info(f"Ingesta CSV exitosa: {len(records)} registros ({len(raw_bytes)} bytes) -> {uri}")
            return result

        except Exception as e:
            finished_at = datetime.now(timezone.utc)
            logger.error(f"Fallo en ingesta CSV para {self.file_path}: {e}")
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
