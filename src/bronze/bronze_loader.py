"""Cargador de la capa Bronze: Agrega metadatos técnicos de ingesta e inmutabilidad."""

from datetime import datetime
import hashlib
import json
from typing import Any, Dict, List, Optional
import uuid

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client

logger = get_logger("BronzeLoader")


class BronzeLoader:
    """Transforma entradas crudas de Landing a Bronze enriquecidas con metadatos de auditoría."""

    def __init__(self, s3_client: Optional[S3Client] = None):
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()

    def process_landing_to_bronze(self, landing_object_key: str, dataset_name: str) -> Dict[str, Any]:
        """Lee un objeto de Landing, añade metadatos de linaje y lo guarda en Bronze."""
        landing_bucket = self.settings.get_bucket("landing")
        bronze_bucket = self.settings.get_bucket("bronze")

        logger.info(f"Procesando de Landing ({landing_bucket}/{landing_object_key}) a Bronze...")
        raw_data = self.s3_client.read_json(landing_bucket, landing_object_key)

        if not isinstance(raw_data, list):
            records = [raw_data]
        else:
            records = raw_data

        ingestion_id = str(uuid.uuid4())
        ingested_at = datetime.utcnow().isoformat()

        bronze_records: List[Dict[str, Any]] = []
        for rec in records:
            # Calcular hash SHA256 del registro para trazabilidad e idempotencia
            rec_str = json.dumps(rec, sort_keys=True)
            rec_hash = hashlib.sha256(rec_str.encode("utf-8")).hexdigest()

            enriched = dict(rec)
            enriched["_metadata"] = {
                "ingestion_id": ingestion_id,
                "ingested_at": ingested_at,
                "source_file": f"s3://{landing_bucket}/{landing_object_key}",
                "record_hash": rec_hash,
                "dataset_name": dataset_name,
            }
            bronze_records.append(enriched)

        # Destino en Bronze
        date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
        bronze_key = f"{dataset_name}/{date_prefix}/bronze_{dataset_name}_{ingestion_id[:8]}.json"

        bronze_uri = self.s3_client.upload_json(bronze_records, bronze_bucket, bronze_key)
        logger.info(f"Capa Bronze generada: {len(bronze_records)} registros en {bronze_uri}")

        return {
            "status": "SUCCESS",
            "bronze_uri": bronze_uri,
            "bronze_key": bronze_key,
            "records_count": len(bronze_records),
            "ingestion_id": ingestion_id,
        }
