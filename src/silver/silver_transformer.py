"""Transformador de la capa Silver: Limpieza, tipado, deduplicación y almacenamiento Parquet."""

from datetime import datetime, timezone
import io
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.quality.validator import DataQualityValidator, ValidationReport

logger = get_logger("SilverTransformer")


class SilverTransformer:
    """Procesa datos crudos de Bronze hacia Silver en formato Parquet limpio y validado."""

    def __init__(self, s3_client: Optional[S3Client] = None, validator: Optional[DataQualityValidator] = None):
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.validator = validator or DataQualityValidator()

    def transform_orders(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], ValidationReport]:
        """Limpia, valida, deduplica y tipa órdenes de compra."""
        valid_records, quarantine_records, report = self.validator.partition_records(records, entity_type="orders")
        if not valid_records:
            return pd.DataFrame(), quarantine_records, report

        df = pd.DataFrame(valid_records)

        # Normalización de strings
        for col in df.select_dtypes(include=["object", "str"]).columns:
            df[col] = df[col].astype(str).str.strip()

        # Deduplicación por clave de negocio: orden e item si existe, o solo orden
        dedup_keys = ["order_id", "item_id"] if "item_id" in df.columns else ["order_id"]
        initial_count = len(df)
        df = df.drop_duplicates(subset=dedup_keys, keep="last")
        logger.info(f"Deduplicación de órdenes ({dedup_keys}): {initial_count} -> {len(df)} registros")

        # Tipado numérico y fecha estricto
        if "quantity" not in df.columns:
            df["quantity"] = 1
        else:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype("int32")

        if "unit_price" not in df.columns:
            df["unit_price"] = 0.0
        else:
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0).astype("float64")

        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0).astype("float64")
        df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

        if "status" in df.columns:
            df["status"] = df["status"].str.upper()
        if "category" in df.columns:
            df["category"] = df["category"].str.title()
        if "shipping_country" in df.columns:
            df["shipping_country"] = df["shipping_country"].str.upper()

        # Particionado temporal
        df["year"] = pd.to_datetime(df["order_date"]).dt.year.astype("int32")
        df["month"] = pd.to_datetime(df["order_date"]).dt.month.astype("int32")
        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

        return df, quarantine_records, report

    def transform_customers(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], ValidationReport]:
        """Limpia, valida y normaliza clientes."""
        valid_records, quarantine_records, report = self.validator.partition_records(records, entity_type="customers")
        if not valid_records:
            return pd.DataFrame(), quarantine_records, report

        df = pd.DataFrame(valid_records)

        for col in df.select_dtypes(include=["object", "str"]).columns:
            df[col] = df[col].astype(str).str.strip()

        # Deduplicación por customer_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=["customer_id"], keep="last")
        logger.info(f"Deduplicación de clientes por customer_id: {initial_count} -> {len(df)} registros")

        if "email" in df.columns:
            df["email"] = df["email"].str.lower()
        if "country" in df.columns:
            df["country"] = df["country"].str.upper()
        if "registration_date" in df.columns:
            df["registration_date"] = pd.to_datetime(df["registration_date"]).dt.date

        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        return df, quarantine_records, report

    def transform_products(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], ValidationReport]:
        """Limpia, valida y normaliza productos de catálogo."""
        valid_records, quarantine_records, report = self.validator.partition_records(records, entity_type="products")
        if not valid_records:
            return pd.DataFrame(), quarantine_records, report

        df = pd.DataFrame(valid_records)

        for col in df.select_dtypes(include=["object", "str"]).columns:
            df[col] = df[col].astype(str).str.strip()

        initial_count = len(df)
        df = df.drop_duplicates(subset=["product_id"], keep="last")
        logger.info(f"Deduplicación de productos por product_id: {initial_count} -> {len(df)} registros")

        df["unit_price"] = pd.to_numeric(df.get("unit_price", 0.0), errors="coerce").fillna(0.0).astype("float64")
        if "cost_price" in df.columns:
            df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce").fillna(0.0).astype("float64")
        if "category" in df.columns:
            df["category"] = df["category"].str.title()

        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        return df, quarantine_records, report

    def transform_returns(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], ValidationReport]:
        """Limpia, valida y normaliza devoluciones."""
        valid_records, quarantine_records, report = self.validator.partition_records(records, entity_type="returns")
        if not valid_records:
            return pd.DataFrame(), quarantine_records, report

        df = pd.DataFrame(valid_records)

        for col in df.select_dtypes(include=["object", "str"]).columns:
            df[col] = df[col].astype(str).str.strip()

        initial_count = len(df)
        df = df.drop_duplicates(subset=["return_id"], keep="last")
        logger.info(f"Deduplicación de devoluciones por return_id: {initial_count} -> {len(df)} registros")

        df["refund_amount"] = pd.to_numeric(df.get("refund_amount", 0.0), errors="coerce").fillna(0.0).astype("float64")
        if "return_date" in df.columns:
            df["return_date"] = pd.to_datetime(df["return_date"]).dt.date
            df["year"] = pd.to_datetime(df["return_date"]).dt.year.astype("int32")
            df["month"] = pd.to_datetime(df["return_date"]).dt.month.astype("int32")

        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        return df, quarantine_records, report

    def process_bronze_to_silver(
        self, bronze_object_key: str, dataset_name: str = "orders"
    ) -> Dict[str, Any]:
        """Lee datos de Bronze, segrega anomalías a Quarantine y guarda Parquet en Silver."""
        bronze_bucket = self.settings.get_bucket("bronze")
        silver_bucket = self.settings.get_bucket("silver")
        quarantine_bucket = self.settings.get_bucket("quarantine")

        logger.info(f"Iniciando transformación Bronze -> Silver para {bronze_bucket}/{bronze_object_key}")
        raw_bronze = self.s3_client.read_json(bronze_bucket, bronze_object_key)

        # 1. Separar registros y metadatos técnicos de Bronze
        records: List[Dict[str, Any]] = []
        for item in raw_bronze:
            clean_item = {k: v for k, v in item.items() if not k.startswith("_")}
            records.append(clean_item)

        if not records:
            logger.warning("No hay registros en el objeto Bronze.")
            return {"status": "SKIPPED", "valid_records_count": 0, "quarantine_records_count": 0}

        # 2. Seleccionar transformador según el dataset
        lower_name = dataset_name.lower()
        if "customer" in lower_name:
            df, quarantine_records, report = self.transform_customers(records)
            entity_type = "customers"
        elif "product" in lower_name:
            df, quarantine_records, report = self.transform_products(records)
            entity_type = "products"
        elif "return" in lower_name:
            df, quarantine_records, report = self.transform_returns(records)
            entity_type = "returns"
        else:
            df, quarantine_records, report = self.transform_orders(records)
            entity_type = "orders"

        # 3. Guardar registros rechazados en Quarantine
        quarantine_uri = None
        if quarantine_records:
            date_prefix = datetime.now(timezone.utc).strftime("%Y/%m/%d")
            q_key = f"rejected/{dataset_name}/{date_prefix}/quarantine_{len(quarantine_records)}_items.json"
            quarantine_uri = self.s3_client.upload_json(quarantine_records, quarantine_bucket, q_key)
            logger.warning(
                f"Registros enviados a Quarantine: {len(quarantine_records)} items -> {quarantine_uri}"
            )

        if df.empty:
            logger.error("No hay registros válidos para la capa Silver.")
            return {
                "status": "FAILED",
                "valid_records_count": 0,
                "quarantine_records_count": len(quarantine_records),
                "quarantine_uri": quarantine_uri,
                "report": report,
            }

        # 4. Serialización a Parquet Snappy en memoria
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="SNAPPY")
        parquet_bytes = buffer.getvalue()

        # 5. Almacenamiento en MinIO Silver
        silver_key = f"retail/{dataset_name}/silver_{dataset_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
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
            "entity_type": entity_type,
        }
