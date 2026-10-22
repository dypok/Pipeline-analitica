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
        self._task_metrics_table_ready = False

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

    def _ensure_task_metrics_table(self) -> None:
        """Crea la tabla de control de métricas operativas si no existe."""
        if self._task_metrics_table_ready:
            return
        ddl = text("""
            CREATE TABLE IF NOT EXISTS analytics.pipeline_task_metrics (
                run_id VARCHAR(200) NOT NULL,
                task_id VARCHAR(100) NOT NULL,
                pipeline_name VARCHAR(100) NOT NULL,
                layer VARCHAR(20),
                status VARCHAR(20) NOT NULL,
                duration_seconds NUMERIC(12, 3) DEFAULT 0,
                records_processed INT DEFAULT 0,
                records_quarantined INT DEFAULT 0,
                recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, task_id)
            );
        """)
        with self.engine.begin() as conn:
            conn.execute(ddl)
        self._task_metrics_table_ready = True

    def log_task_metric(
        self,
        run_id: str,
        task_id: str,
        pipeline_name: str,
        layer: str,
        status: str,
        duration_seconds: float = 0.0,
        records_processed: int = 0,
        records_quarantined: int = 0,
    ) -> None:
        """Registra/actualiza la métrica operativa de una tarea (idempotente por run_id+task_id)."""
        self._ensure_task_metrics_table()
        sql = text("""
            INSERT INTO analytics.pipeline_task_metrics (
                run_id, task_id, pipeline_name, layer, status,
                duration_seconds, records_processed, records_quarantined, recorded_at
            ) VALUES (
                :run_id, :task_id, :pipeline_name, :layer, :status,
                :duration_seconds, :records_processed, :records_quarantined, CURRENT_TIMESTAMP
            )
            ON CONFLICT (run_id, task_id) DO UPDATE SET
                status = EXCLUDED.status,
                duration_seconds = EXCLUDED.duration_seconds,
                records_processed = EXCLUDED.records_processed,
                records_quarantined = EXCLUDED.records_quarantined,
                recorded_at = CURRENT_TIMESTAMP;
        """)
        with self.engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "run_id": run_id,
                    "task_id": task_id,
                    "pipeline_name": pipeline_name,
                    "layer": layer,
                    "status": status,
                    "duration_seconds": round(float(duration_seconds), 3),
                    "records_processed": records_processed,
                    "records_quarantined": records_quarantined,
                },
            )

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

    def upsert_customer_360_kpis(self, records: List[Dict[str, Any]]) -> int:
        """Inserta o actualiza métricas de la capa Gold en analytics.customer_360_kpis."""
        if not records:
            return 0

        sql = text("""
            INSERT INTO analytics.customer_360_kpis (
                customer_id, first_name, last_name, email, country,
                total_lifetime_orders, total_lifetime_spend, average_order_value,
                first_order_date, last_order_date, customer_tier, updated_at
            ) VALUES (
                :customer_id, :first_name, :last_name, :email, :country,
                :total_lifetime_orders, :total_lifetime_spend, :average_order_value,
                :first_order_date, :last_order_date, :customer_tier, CURRENT_TIMESTAMP
            )
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                total_lifetime_orders = EXCLUDED.total_lifetime_orders,
                total_lifetime_spend = EXCLUDED.total_lifetime_spend,
                average_order_value = EXCLUDED.average_order_value,
                first_order_date = EXCLUDED.first_order_date,
                last_order_date = EXCLUDED.last_order_date,
                customer_tier = EXCLUDED.customer_tier,
                updated_at = CURRENT_TIMESTAMP;
        """)

        with self.engine.begin() as conn:
            for rec in records:
                conn.execute(sql, rec)

        logger.info(f"Guardados {len(records)} registros en analytics.customer_360_kpis")
        return len(records)

    def upsert_product_performance(self, records: List[Dict[str, Any]]) -> int:
        """Inserta o actualiza métricas de la capa Gold en analytics.product_performance."""
        if not records:
            return 0

        sql = text("""
            INSERT INTO analytics.product_performance (
                product_id, product_name, category, units_sold, gross_revenue,
                returns_count, return_rate, margin, performance_tier, updated_at
            ) VALUES (
                :product_id, :product_name, :category, :units_sold, :gross_revenue,
                :returns_count, :return_rate, :margin, :performance_tier, CURRENT_TIMESTAMP
            )
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                units_sold = EXCLUDED.units_sold,
                gross_revenue = EXCLUDED.gross_revenue,
                returns_count = EXCLUDED.returns_count,
                return_rate = EXCLUDED.return_rate,
                margin = EXCLUDED.margin,
                performance_tier = EXCLUDED.performance_tier,
                updated_at = CURRENT_TIMESTAMP;
        """)

        with self.engine.begin() as conn:
            for rec in records:
                conn.execute(sql, rec)

        logger.info(f"Guardados {len(records)} registros en analytics.product_performance")
        return len(records)

    def insert_quarantine_records(self, records: List[Dict[str, Any]]) -> int:
        """Inserta registros anómalos rechazados en analytics.quarantine_records."""
        if not records:
            return 0

        sql = text("""
            INSERT INTO analytics.quarantine_records (
                record_id, rejected_at, entity_type, quarantine_reason, raw_record
            ) VALUES (
                :record_id, :rejected_at, :entity_type, :quarantine_reason, :raw_record
            )
            ON CONFLICT (record_id) DO NOTHING;
        """)

        with self.engine.begin() as conn:
            for rec in records:
                conn.execute(sql, rec)

        logger.info(f"Guardados {len(records)} registros en analytics.quarantine_records")
        return len(records)
