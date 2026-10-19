"""Agregador de la capa Gold: Cálculo de KPIs de negocio, Parquet analítico y publicación en PostgreSQL."""

from datetime import datetime
import io
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import get_settings
from src.common.db_client import DatabaseClient
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client

logger = get_logger("GoldAggregator")


class GoldAggregator:
    """Calcula indicadores analíticos de la capa Gold y los publica en MinIO y PostgreSQL."""

    def __init__(self, s3_client: Optional[S3Client] = None, db_client: Optional[DatabaseClient] = None):
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()
        self.db_client = db_client or DatabaseClient()

    def process_silver_to_gold(self, silver_object_key: str) -> Dict[str, Any]:
        """Lee el dataset Parquet de Silver, agrega KPIs de ventas y persiste en Gold y Postgres."""
        silver_bucket = self.settings.get_bucket("silver")
        gold_bucket = self.settings.get_bucket("gold")

        logger.info(f"Procesando capa Gold a partir de Silver ({silver_bucket}/{silver_object_key})...")

        # 1. Leer Parquet desde MinIO
        parquet_bytes = self.s3_client.read_bytes(silver_bucket, silver_object_key)
        df_silver = pd.read_parquet(io.BytesIO(parquet_bytes))

        if df_silver.empty:
            logger.warning("Dataset Silver vacío. No se generaron métricas Gold.")
            return {"status": "SKIPPED", "kpis_count": 0}

        # 2. Cálculo de KPIs agregados por fecha (daily_sales)
        kpi_rows: List[Dict[str, Any]] = []

        grouped = df_silver.groupby("order_date")
        for order_date, group in grouped:
            total_orders = len(group)
            completed_group = group[group["status"] == "COMPLETED"]
            completed_orders = len(completed_group)
            cancelled_orders = len(group[group["status"] == "CANCELLED"])

            total_revenue = round(float(completed_group["total_amount"].sum()), 2)
            average_ticket = round(total_revenue / completed_orders, 2) if completed_orders > 0 else 0.00
            distinct_customers = int(group["customer_id"].nunique())

            # Categoría líder en ventas
            top_category = "General"
            if not group.empty and "category" in group.columns:
                cat_counts = group.groupby("category")["total_amount"].sum()
                if not cat_counts.empty:
                    top_category = str(cat_counts.idxmax())

            kpi_rows.append(
                {
                    "kpi_date": order_date,
                    "total_orders": total_orders,
                    "completed_orders": completed_orders,
                    "cancelled_orders": cancelled_orders,
                    "total_revenue": total_revenue,
                    "average_ticket": average_ticket,
                    "distinct_customers": distinct_customers,
                    "top_category": top_category,
                }
            )

        df_gold = pd.DataFrame(kpi_rows)
        df_gold["_gold_calculated_at"] = datetime.utcnow().isoformat()

        # 3. Guardar Parquet en MinIO Gold
        table = pa.Table.from_pandas(df_gold)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="SNAPPY")
        gold_bytes = buffer.getvalue()

        gold_key = f"analytics/daily_sales/gold_daily_sales_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
        gold_uri = self.s3_client.upload_bytes(gold_bytes, gold_bucket, gold_key)
        logger.info(f"Parquet Gold guardado en: {gold_uri}")

        # 4. Publicación en la base de datos analítica PostgreSQL
        db_published_count = 0
        try:
            if self.db_client.ping():
                db_published_count = self.db_client.upsert_daily_sales_kpis(kpi_rows)
                logger.info(f"Métricas publicadas en analytics.daily_sales_kpis ({db_published_count} registros)")
            else:
                logger.warning("PostgreSQL analítico no disponible en este momento. Se continuará con MinIO Gold.")
        except Exception as e:
            logger.warning(f"No se pudo sincronizar en PostgreSQL: {e}")

        return {
            "status": "SUCCESS",
            "gold_uri": gold_uri,
            "gold_key": gold_key,
            "kpis_count": len(kpi_rows),
            "db_published_count": db_published_count,
            "sample_kpi": kpi_rows[0] if kpi_rows else None,
        }
