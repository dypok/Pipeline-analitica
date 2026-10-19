"""Monitor de SLA y construcción de alertas de incumplimiento (US-11).

Expone los objetivos de SLA por tarea del DAG maestro y normaliza los eventos
de ``sla_miss`` de Airflow a mensajes/estructuras listos para alertar y
registrar en la tabla de control operativa.
"""

from datetime import timedelta

# Tiempo máximo esperado (encolado + ejecución) por tarea del DAG maestros.
DEFAULT_TASK_SLAS = {
    "validate_environment": timedelta(minutes=5),
    "extract_and_land": timedelta(minutes=15),
    "process_bronze": timedelta(minutes=15),
    "process_silver_and_quarantine": timedelta(minutes=20),
    "process_gold": timedelta(minutes=15),
    "quality_gate": timedelta(minutes=10),
    "publish_metrics": timedelta(minutes=5),
}

FALLBACK_SLA = timedelta(minutes=30)


class SLAMonitor:
    """Resuelve los SLA por tarea y arma las alertas de incumplimiento."""

    def __init__(self, task_slas: dict | None = None) -> None:
        self.task_slas = dict(task_slas) if task_slas else dict(DEFAULT_TASK_SLAS)

    def sla_for(self, task_id: str, default: timedelta = FALLBACK_SLA) -> timedelta:
        """Devuelve el SLA configurado para una tarea (o el valor por defecto)."""
        return self.task_slas.get(task_id, default)

    @staticmethod
    def summarize(slas, blocking_tis) -> list[dict]:
        """Normaliza los ``SlaMiss`` y las tareas bloqueantes a una lista de dicts."""
        summary = [
            {
                "task_id": getattr(sla, "task_id", "?"),
                "dag_id": getattr(sla, "dag_id", "?"),
                "expected_at": str(getattr(sla, "timestamp", "")),
            }
            for sla in (slas or [])
        ]
        summary += [
            {
                "task_id": getattr(ti, "task_id", "?"),
                "run_id": getattr(ti, "run_id", "?"),
                "state": str(getattr(ti, "state", "?")),
            }
            for ti in (blocking_tis or [])
        ]
        return summary

    @staticmethod
    def build_alert_message(dag_id: str, run_id: str, slas, blocking_tis) -> str:
        """Construye el mensaje legible del incumplimiento de SLA."""
        expired = sorted({getattr(sla, "task_id", "?") for sla in (slas or [])})
        blockers = sorted({getattr(ti, "task_id", "?") for ti in (blocking_tis or [])})
        return (
            f"[SLA MISS] DAG='{dag_id}' run_id='{run_id}' no cumplió el SLA. "
            f"Tareas vencidas: {', '.join(expired) if expired else 'n/d'}. "
            f"Tareas bloqueantes: {', '.join(blockers) if blockers else 'n/d'}."
        )
