"""Pruebas unitarias de los helpers de orquestación (US-10 / US-11)."""

from datetime import timedelta
from types import SimpleNamespace

from src.orchestration.partition_manager import PartitionManager
from src.orchestration.quality_gate import GoldQualityGate
from src.orchestration.sla_monitor import DEFAULT_TASK_SLAS, SLAMonitor
from src.orchestration.task_metrics import TaskMetricsRecorder


class FakeS3Client:
    """Doble de prueba que simula un almacenamiento de objetos en memoria."""

    def __init__(self, keys=None):
        self.store = {("gold", k): b"x" for k in (keys or [])}
        self.deleted = []

    def list_objects(self, bucket_name, prefix=""):
        return [k for (b, k) in self.store if b == bucket_name and k.startswith(prefix)]

    def delete_object(self, bucket_name, object_name):
        self.store.pop((bucket_name, object_name), None)
        self.deleted.append(object_name)


class FakeDatabaseClient:
    """Doble de prueba que captura las métricas registradas."""

    def __init__(self):
        self.task_metrics = []
        self.executions = []

    def log_task_metric(self, **kwargs):
        self.task_metrics.append(kwargs)

    def log_execution(self, **kwargs):
        self.executions.append(kwargs)


def test_partition_manager_cleans_only_matching_prefix():
    fake_s3 = FakeS3Client(keys=["analytics/daily_sales/a.parquet", "analytics/daily_sales/b.parquet", "other/c.parquet"])
    manager = PartitionManager(s3_client=fake_s3)

    deleted = manager.clean_partition("gold", "analytics/daily_sales/")

    assert deleted == 2
    assert set(fake_s3.deleted) == {"analytics/daily_sales/a.parquet", "analytics/daily_sales/b.parquet"}
    assert ("gold", "other/c.parquet") in fake_s3.store


def test_partition_manager_is_noop_when_empty():
    manager = PartitionManager(s3_client=FakeS3Client())
    assert manager.clean_partition("gold", "analytics/daily_sales/") == 0


def test_quality_gate_passes_on_healthy_metrics():
    rows = [
        {"kpi_date": "2026-01-01", "total_revenue": 1500.0, "total_orders": 10, "completed_orders": 8, "cancelled_orders": 2},
        {"kpi_date": "2026-01-02", "total_revenue": 0.0, "total_orders": 0, "completed_orders": 0, "cancelled_orders": 0},
    ]
    result = GoldQualityGate().evaluate(rows)
    assert result.passed is True
    assert result.checked_metrics == 2


def test_quality_gate_flags_negative_revenue():
    rows = [{"kpi_date": "2026-01-01", "total_revenue": -10.0, "total_orders": 5, "completed_orders": 5, "cancelled_orders": 0}]
    result = GoldQualityGate().evaluate(rows)
    assert result.passed is False
    assert any("negativo" in a for a in result.anomalies)


def test_quality_gate_flags_inconsistent_orders():
    rows = [{"kpi_date": "2026-01-01", "total_revenue": 100.0, "total_orders": 3, "completed_orders": 3, "cancelled_orders": 2}]
    result = GoldQualityGate().evaluate(rows)
    assert result.passed is False


def test_quality_gate_fails_on_empty_dataset():
    result = GoldQualityGate().evaluate([])
    assert result.passed is False
    assert result.checked_metrics == 0


def test_task_metrics_recorder_persists_task_and_run():
    fake_db = FakeDatabaseClient()
    recorder = TaskMetricsRecorder(db_client=fake_db)

    assert recorder.record_task(
        run_id="manual__2026-01-01",
        task_id="process_gold",
        pipeline_name="dag_medallion_pipeline",
        layer="GOLD",
        status="SUCCESS",
        duration_seconds=1.234,
        records_processed=42,
    )
    assert recorder.record_run(
        run_id="manual__2026-01-01",
        pipeline_name="dag_medallion_pipeline",
        status="SUCCESS",
        records_processed=42,
        records_quarantined=3,
    )

    assert fake_db.task_metrics[0]["task_id"] == "process_gold"
    assert fake_db.task_metrics[0]["records_processed"] == 42
    assert fake_db.executions[0]["execution_id"] == "EXEC-manual__2026-01-01"
    assert fake_db.executions[0]["records_quarantined"] == 3


def test_task_metrics_recorder_swallows_errors():
    class BrokenDb:
        def log_task_metric(self, **kwargs):
            raise RuntimeError("db caída")

        def log_execution(self, **kwargs):
            raise RuntimeError("db caída")

    recorder = TaskMetricsRecorder(db_client=BrokenDb())
    assert recorder.record_task("r", "t", "p", "GOLD", "SUCCESS", 0.0) is False
    assert recorder.record_run("r", "p", "SUCCESS") is False


def test_sla_monitor_covers_every_dag_task():
    expected = {
        "validate_environment",
        "extract_and_land",
        "process_bronze",
        "process_silver_and_quarantine",
        "process_gold",
        "quality_gate",
        "publish_metrics",
    }
    assert set(DEFAULT_TASK_SLAS) == expected


def test_sla_monitor_sla_for_uses_override_and_fallback():
    monitor = SLAMonitor(task_slas={"process_gold": timedelta(minutes=7)})
    assert monitor.sla_for("process_gold") == timedelta(minutes=7)
    assert monitor.sla_for("desconocida") == timedelta(minutes=30)


def test_sla_monitor_build_alert_message_reports_expired_and_blocking():
    slas = [SimpleNamespace(task_id="process_bronze")]
    blocking = [SimpleNamespace(task_id="extract_and_land")]
    message = SLAMonitor.build_alert_message("dag_medallion_pipeline", "manual__1", slas, blocking)
    assert "[SLA MISS]" in message
    assert "process_bronze" in message
    assert "extract_and_land" in message


def test_sla_monitor_build_alert_message_handles_empty_inputs():
    message = SLAMonitor.build_alert_message("dag_medallion_pipeline", "manual__1", [], [])
    assert "Tareas vencidas: n/d" in message
    assert "Tareas bloqueantes: n/d" in message


def test_sla_monitor_summarize_normalizes_events():
    slas = [SimpleNamespace(task_id="quality_gate", dag_id="d", timestamp="2026-01-01T00:00:00")]
    blocking = [SimpleNamespace(task_id="process_gold", run_id="manual__1", state="running")]
    summary = SLAMonitor.summarize(slas, blocking)
    assert summary[0] == {"task_id": "quality_gate", "dag_id": "d", "expected_at": "2026-01-01T00:00:00"}
    assert summary[1] == {"task_id": "process_gold", "run_id": "manual__1", "state": "running"}
