"""Ingestor para cargar archivos y datos crudos hacia la capa Landing de MinIO."""

from datetime import datetime
import json
from typing import Any, Dict, List, Optional
import uuid

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.ingestion.base_connector import BaseConnector, IngestionResult

logger = get_logger("LandingIngestor")


class LandingIngestor(BaseConnector):
    """Ingesta de datos crudos hacia el bucket Landing."""

    def __init__(self, source_name: str = "retail_simulator", s3_client: Optional[S3Client] = None):
        super().__init__(source_name)
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()

    def extract(self) -> List[Dict[str, Any]]:
        """Extrae registros por defecto (permite override por subclases o simulador)."""
        return []

    def ingest(self, destination_bucket: Optional[str] = None, destination_prefix: str = "orders") -> IngestionResult:
        """Implementación del contrato base BaseConnector."""
        records = self.extract()
        bucket = destination_bucket or self.settings.get_bucket("landing")
        return self.ingest_records(records, dataset_name=destination_prefix, file_format="json", target_bucket=bucket)

    def ingest_records(
        self,
        records: List[Dict[str, Any]],
        dataset_name: str,
        file_format: str = "json",
        target_bucket: Optional[str] = None,
    ) -> IngestionResult:
        """Deposita una lista de registros crudos en el bucket Landing."""
        started_at = datetime.utcnow()
        bucket = target_bucket or self.settings.get_bucket("landing")
        timestamp_prefix = started_at.strftime("%Y/%m/%d")
        file_id = str(uuid.uuid4())[:8]
        object_key = f"{dataset_name}/{timestamp_prefix}/{dataset_name}_{file_id}.{file_format}"

        try:
            if file_format == "json":
                payload = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")
                content_type = "application/json"
            else:
                raise ValueError(f"Formato no soportado: {file_format}")

            uri = self.s3_client.upload_bytes(payload, bucket, object_key, content_type)
            finished_at = datetime.utcnow()

            result = IngestionResult(
                source_name=self.source_name,
                destination_uri=uri,
                records_count=len(records),
                bytes_written=len(payload),
                status="SUCCESS",
                started_at=started_at,
                finished_at=finished_at,
                metadata={"object_key": object_key, "bucket": bucket, "format": file_format},
            )
            logger.info(f"Ingesta exitosa en Landing: {len(records)} registros -> {uri}")
            return result

        except Exception as e:
            finished_at = datetime.utcnow()
            logger.error(f"Fallo en ingesta a Landing: {e}")
            return IngestionResult(
                source_name=self.source_name,
                destination_uri=f"s3://{bucket}/{object_key}",
                records_count=0,
                bytes_written=0,
                status="FAILED",
                started_at=started_at,
                finished_at=finished_at,
                error_message=str(e),
            )
