"""Transformador de la capa Silver: Limpieza, tipado, deduplicación y almacenamiento Parquet."""

from datetime import datetime
import io
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.quality.validator import DataQualityValidator

logger = get_logger("SilverTransformer")


class SilverTransformer:
    """Procesa datos crudos de Bronze hacia Silver en formato Parquet limpio."""

    def __init__(self, s3_client: Optional[S3Client] = None, validator: Optional[DataQualityValidator] = None):
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.validator = validator or DataQualityValidator()

    def process_bronze_to_silver(
        self, bronze_object_key: str, dataset_name: str = "orders"
    ) -> Dict[str, Any]:
        """Lee datos de Bronze, segrega anomalías a Quarantine y guarda Parquet en Silver."""
        bronze_bucket = self.settings.get_bucket("bronze")
        silver_bucket = self.settings.get_bucket("silver")
        quarantine_bucket = self.settings.get_bucket("quarantine")

        logger.info(f"Iniciando transformación Bronze -> Silver para {bronze_bucket}/{bronze_object_key}")
        raw_bronze = self.s3_client.read_json(bronze_bucket, bronze_object_key)

        # 1. Separar registros y metadatos
        records: List[Dict[str, Any]] = []
        for item in raw_bronze:
            clean_item = {k: v for k, v in item.items() if not k.startswith("_")}
            records.append(clean_item)

        # 2. Validación de calidad y segregación a Quarantine
        valid_records, quarantine_records, report = self.validator.partition_records(
            records, entity_type="orders" if "order_id" in records[0] else "customers"
        )

        # 3. Guardar registros rechazados en Quarantine
        quarantine_uri = None
        if quarantine_records:
            date_prefix = datetime.utcnow().strftime("%Y/%m/%d")
            q_key = f"rejected/{dataset_name}/{date_prefix}/quarantine_{len(quarantine_records)}_items.json"
            quarantine_uri = self.s3_client.upload_json(quarantine_records, quarantine_bucket, q_key)
            logger.warning(
                f"Registros enviados a Quarantine: {len(quarantine_records)} items -> {quarantine_uri}"
            )

        if not valid_records:
            logger.error("No hay registros válidos para la capa Silver.")
            return {
                "status": "FAILED",
                "valid_records_count": 0,
                "quarantine_records_count": len(quarantine_records),
                "quarantine_uri": quarantine_uri,
            }

        # 4. Transformación, Tipado y Deduplicación con Pandas/PyArrow
        df = pd.DataFrame(valid_records)

        # Normalización de textos y strings
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str).str.strip()

        # Deduplicación por clave primaria (conservar último registro)
        if "order_id" in df.columns:
            initial_count = len(df)
            df = df.drop_duplicates(subset=["order_id"], keep="last")
            logger.info(f"Deduplicación por order_id: {initial_count} -> {len(df)} registros")

            # Tipado estricto
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype("int32")
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").astype("float64")
            df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").astype("float64")
            df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

            # Columnas de partición
            df["year"] = pd.to_datetime(df["order_date"]).dt.year.astype("int32")
            df["month"] = pd.to_datetime(df["order_date"]).dt.month.astype("int32")

        # Metadatos de auditoría Silver
        df["_silver_processed_at"] = datetime.utcnow().isoformat()

        # 5. Serialización a Parquet en memoria
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="SNAPPY")
        parquet_bytes = buffer.getvalue()

        # 6. Almacenamiento en MinIO Silver
        silver_key = f"retail/{dataset_name}/silver_{dataset_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
        silver_uri = self.s3_client.upload_bytes(
            parquet_bytes, silver_bucket, silver_key, content_type="application/octet-stream"
        )

        logger.info(f"Capa Silver generada exitosamente: {len(df)} filas en {silver_uri}")

        return {
            "status": "SUCCESS",
            "silver_uri": silver_uri,
            "silver_key": silver_key,
            "valid_records_count": len(df),
            "quarantine_records_count": len(quarantine_records),
            "quarantine_uri": quarantine_uri,
            "report": report,
        }
