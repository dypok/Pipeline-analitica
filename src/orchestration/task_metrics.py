"""Registro de métricas operativas de ejecución en la tabla de control (US-11).

Exporta tiempos por tarea y volúmenes procesados a una tabla de control en
PostgreSQL para dar observabilidad a las corridas de Airflow.
"""

from typing import Optional

from src.common.db_client import DatabaseClient
from src.common.logging_utils import get_logger

logger = get_logger("TaskMetricsRecorder")


class TaskMetricsRecorder:
    """Encapsula el registro de métricas de ejecución por tarea y por corrida."""

    def __init__(self, db_client: Optional[DatabaseClient] = None) -> None:
        self.db_client = db_client or DatabaseClient()

    def record_task(
        self,
        run_id: str,
        task_id: str,
        pipeline_name: str,
        layer: str,
        status: str,
        duration_seconds: float,
        records_processed: int = 0,
        records_quarantined: int = 0,
    ) -> bool:
        """Registra la métrica de una tarea. Devuelve True si se persistió."""
        try:
            self.db_client.log_task_metric(
                run_id=run_id,
                task_id=task_id,
                pipeline_name=pipeline_name,
                layer=layer,
                status=status,
                duration_seconds=duration_seconds,
                records_processed=records_processed,
                records_quarantined=records_quarantined,
            )
            return True
        except Exception as e:
            logger.warning(f"No se pudo registrar la métrica de la tarea '{task_id}': {e}")
            return False

    def record_run(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        records_processed: int = 0,
        records_quarantined: int = 0,
        error_message: Optional[str] = None,
    ) -> bool:
        """Registra el cierre de una corrida completa en el log de ejecuciones."""
        try:
            self.db_client.log_execution(
                execution_id=f"EXEC-{run_id}",
                pipeline_name=pipeline_name,
                layer="ALL",
                status=status,
                records_processed=records_processed,
                records_quarantined=records_quarantined,
                error_message=error_message,
            )
            return True
        except Exception as e:
            logger.warning(f"No se pudo registrar la corrida '{run_id}': {e}")
            return False
