"""Cliente de base de datos relacional para el Data Mart analítico en PostgreSQL."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.common.config import get_settings
from src.common.logging_utils import get_logger

logger = get_logger("DatabaseClient")


class DatabaseClient:
    """Cliente para operaciones en el esquema analytics de PostgreSQL."""

    def __init__(self, db_url: Optional[str] = None):
        settings = get_settings()
        self.db_url = db_url or settings.analytics_db_url
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.db_url, pool_pre_ping=True, future=True)
        return self._engine

    def ping(self) -> bool:
        """Verifica conectividad con la base de datos."""
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text("SELECT 1")).scalar()
                return res == 1
        except Exception as e:
            logger.warning(f"Error conectando a la base de datos analítica: {e}")
            return False

    def log_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        layer: str,
        status: str,
        records_processed: int = 0,
        records_quarantined: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Registra una ejecución de pipeline en analytics.pipeline_execution_logs."""
        sql = text("""
            INSERT INTO analytics.pipeline_execution_logs (
                execution_id, pipeline_name, layer, status,
                records_processed, records_quarantined, finished_at, error_message
            ) VALUES (
                :execution_id, :pipeline_name, :layer, :status,
                :records_processed, :records_quarantined, :finished_at, :error_message
            )
            ON CONFLICT (execution_id) DO UPDATE SET
                status = EXCLUDED.status,
                records_processed = EXCLUDED.records_processed,
                records_quarantined = EXCLUDED.records_quarantined,
                finished_at = EXCLUDED.finished_at,
                error_message = EXCLUDED.error_message;
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    sql,
                    {
                        "execution_id": execution_id,
                        "pipeline_name": pipeline_name,
                        "layer": layer,
                        "status": status,
                        "records_processed": records_processed,
                        "records_quarantined": records_quarantined,
                        "finished_at": datetime.utcnow() if status in ["SUCCESS", "FAILED"] else None,
                        "error_message": error_message,
                    },
                )
        except Exception as e:
            logger.warning(f"No se pudo guardar log de ejecución en DB: {e}")

    def upsert_daily_sales_kpis(self, records: List[Dict[str, Any]]) -> int:
        """Inserta o actualiza métricas de la capa Gold en analytics.daily_sales_kpis."""
        if not records:
            return 0

        sql = text("""
            INSERT INTO analytics.daily_sales_kpis (
                kpi_date, total_orders, completed_orders, cancelled_orders,
                total_revenue, average_ticket, distinct_customers, top_category, updated_at
            ) VALUES (
                :kpi_date, :total_orders, :completed_orders, :cancelled_orders,
                :total_revenue, :average_ticket, :distinct_customers, :top_category, CURRENT_TIMESTAMP
            )
            ON CONFLICT (kpi_date) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                completed_orders = EXCLUDED.completed_orders,
                cancelled_orders = EXCLUDED.cancelled_orders,
                total_revenue = EXCLUDED.total_revenue,
                average_ticket = EXCLUDED.average_ticket,
                distinct_customers = EXCLUDED.distinct_customers,
                top_category = EXCLUDED.top_category,
                updated_at = CURRENT_TIMESTAMP;
        """)

        with self.engine.begin() as conn:
            for rec in records:
                conn.execute(sql, rec)

        logger.info(f"Guardados {len(records)} registros en analytics.daily_sales_kpis")
        return len(records)
