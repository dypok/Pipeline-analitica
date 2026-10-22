"""DAG maestro de orquestación del Pipeline Medallion (US-10) con observabilidad,
reintentos e idempotencia (US-11).

Responsable: Héctor (Ingeniero de Operaciones / DataOps).
Flujo: validate_environment >> extract_and_land >> process_bronze
       >> process_silver_and_quarantine >> process_gold >> quality_gate >> publish_metrics
"""

from datetime import datetime, timedelta
import io
import os
import sys
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

# Asegurar que el código fuente esté disponible en el path de Python dentro del contenedor
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.bronze.bronze_loader import BronzeLoader
from src.common.config import get_settings
from src.common.db_client import DatabaseClient
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.gold.gold_aggregator import GoldAggregator
from src.ingestion.file_ingestor import LandingIngestor
from src.ingestion.sample_generator import MockDataGenerator
from ingestion.rest_api_connector import RestApiConnector
from ingestion.base_connector import ConnectorConfig
from src.orchestration.partition_manager import PartitionManager
from src.orchestration.quality_gate import GoldQualityGate
from src.orchestration.sla_monitor import SLAMonitor
from src.orchestration.task_metrics import TaskMetricsRecorder
from src.orchestration.lineage_emitter import LineageEmitter
from src.silver.silver_transformer import SilverTransformer

logger = get_logger("DAGMedallionPipeline")

SLA_MONITOR = SLAMonitor()

# ---------------------------------------------------------------------------
# Parámetros del DAG (US-10 / US-11)
# ---------------------------------------------------------------------------
PIPELINE_NAME = "dag_medallion_pipeline"
DATASET_NAME = os.getenv("INGEST_DATASET_NAME", "orders")
ALERT_EMAIL = os.getenv("AIRFLOW_ALERT_EMAIL", "").strip()
MOCK_ORDER_COUNT = int(os.getenv("MOCK_ORDER_COUNT", "200"))
MOCK_CUSTOMER_COUNT = int(os.getenv("MOCK_CUSTOMER_COUNT", "40"))

# Configuración SMTP para alertas (US-11)
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes")
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() in ("1", "true", "yes")
SMTP_FROM = os.getenv("SMTP_FROM", "airflow@medallion.local").strip()

# Configuración para ingesta REST (US-05)
USE_REST_API_INGEST = os.getenv("USE_REST_API_INGEST", "false").lower() in ("1", "true", "yes")
REST_API_BASE_URL = os.getenv("REST_API_BASE_URL", "").strip()
REST_API_ENDPOINT = os.getenv("REST_API_ENDPOINT", "").strip()
REST_API_SOURCE_ID = os.getenv("REST_API_SOURCE_ID", "rest_api_sales")
REST_API_DATASET_NAME = os.getenv("REST_API_DATASET_NAME", DATASET_NAME)
REST_API_PAGINATION_TYPE = os.getenv("REST_API_PAGINATION_TYPE", "page_number")
REST_API_PAGE_SIZE = int(os.getenv("REST_API_PAGE_SIZE", "100"))
REST_API_MAX_PAGES = int(os.getenv("REST_API_MAX_PAGES", "50"))


def _record_task_metric(context, task_id, layer, started_at, status="SUCCESS", records=0, quarantined=0):
    """Registra best-effort la métrica operativa de una tarea (US-11)."""
    duration = time.perf_counter() - started_at
    try:
        TaskMetricsRecorder().record_task(
            run_id=context.get("run_id", "manual"),
            task_id=task_id,
            pipeline_name=PIPELINE_NAME,
            layer=layer,
            status=status,
            duration_seconds=duration,
            records_processed=records,
            records_quarantined=quarantined,
        )
    except Exception as e:  # nunca debe tumbar la tarea por telemetría
        logger.warning(f"No se pudo registrar la métrica de '{task_id}': {e}")


def _send_alert_email(subject, body):
    """Envía una alerta por correo (SMTP configurable vía .env; fallback a Airflow)."""
    if not ALERT_EMAIL:
        return
    try:
        if SMTP_HOST:
            msg = MIMEMultipart()
            msg["From"] = SMTP_FROM or SMTP_USER or "airflow@medallion.local"
            msg["To"] = ALERT_EMAIL
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain", "utf-8"))
            if SMTP_USE_SSL:
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                    if SMTP_USER and SMTP_PASSWORD:
                        server.login(SMTP_USER, SMTP_PASSWORD)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                    if SMTP_USE_TLS:
                        server.starttls()
                    if SMTP_USER and SMTP_PASSWORD:
                        server.login(SMTP_USER, SMTP_PASSWORD)
                    server.send_message(msg)
            logger.info(f"Alerta enviada vía SMTP a {ALERT_EMAIL}: {subject}")
            return
    except Exception as e:
        logger.warning(f"Falló el envío de alerta vía SMTP: {e}")

    try:
        from airflow.utils.email import send_email

        send_email(to=[ALERT_EMAIL], subject=subject, html_content=body)
    except Exception as e:
        logger.warning(f"No se pudo enviar la alerta por correo: {e}")
def notify_failure(context):
    """Alerta por log/correo ante fallos de cualquier tarea (US-10, punto 4)."""
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "desconocida"
    run_id = context.get("run_id", "desconocido")
    exception = context.get("exception")
    message = f"[ALERTA] Fallo en la tarea '{task_id}' (run_id='{run_id}'): {exception}"
    logger.error(message)
    _send_alert_email(f"[Airflow][FAIL] {PIPELINE_NAME} :: {task_id}", message)
    _record_task_metric(context, task_id, "UNKNOWN", time.perf_counter(), status="FAILED")


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alerta cuando una tarea incumple su SLA (US-11)."""
    run_id = getattr(slas[0], "run_id", "desconocido") if slas else "desconocido"
    dag_id = getattr(dag, "dag_id", PIPELINE_NAME)
    message = SLAMonitor.build_alert_message(dag_id, run_id, slas, blocking_tis)
    logger.error(message)
    try:
        TaskMetricsRecorder().record_task(
            run_id=run_id,
            task_id="sla_miss",
            pipeline_name=PIPELINE_NAME,
            layer="ALL",
            status="SLA_MISS",
            duration_seconds=0.0,
        )
    except Exception as e:  # la telemetría nunca debe tumbar el callback
        logger.warning(f"No se pudo registrar la métrica de SLA: {e}")
    _send_alert_email(f"[Airflow][SLA MISS] {dag_id}", message)


# ---------------------------------------------------------------------------
# Tareas del flujo
# ---------------------------------------------------------------------------
def task_validate_environment(**context):
    """Comprueba conectividad a MinIO y disponibilidad de recursos/DB."""
    started = time.perf_counter()
    settings = get_settings()
    landing_bucket = settings.get_bucket("landing")

    try:
        S3Client().ensure_bucket_exists(landing_bucket)
    except Exception as e:
        raise AirflowException(f"MinIO no disponible en {settings.minio_endpoint}: {e}")

    db_available = DatabaseClient().ping()
    if not db_available:
        logger.warning("PostgreSQL analítico no disponible; se continuará con el almacenamiento de objetos.")

    logger.info(f"Entorno validado (MinIO='{settings.minio_endpoint}', analytics_db={db_available}).")
    _record_task_metric(context, "validate_environment", "ALL", started)
    return {"minio": True, "analytics_db": db_available}


def task_extract_and_land(**context):
    """Ejecuta la extracción y depósito de datos crudos en la capa Landing (Mock o REST API - US-05)."""
    started = time.perf_counter()
    run_id = context.get("run_id", "manual")
    execution_date = str(context.get("logical_date") or context.get("execution_date") or run_id)
    settings = get_settings()

    if USE_REST_API_INGEST and REST_API_BASE_URL:
        logger.info(
            f"Ingesta vía REST API habilitada: {REST_API_BASE_URL}/{REST_API_ENDPOINT} (paginación={REST_API_PAGINATION_TYPE})"
        )
        config = ConnectorConfig(
            source_id=REST_API_SOURCE_ID,
            dataset_name=REST_API_DATASET_NAME,
            base_url=REST_API_BASE_URL,
            endpoint=REST_API_ENDPOINT,
            pagination_type=REST_API_PAGINATION_TYPE,
            page_size=REST_API_PAGE_SIZE,
            max_pages=REST_API_MAX_PAGES,
        )
        connector = RestApiConnector(config=config)
        ingestion_result = connector.execute()
        if ingestion_result.status != "SUCCESS":
            raise AirflowException(f"Fallo en la ingesta REST API hacia Landing: {ingestion_result.error_message}")

        landing_key = ingestion_result.metadata.get("landing_key") or ingestion_result.metadata.get("object_key")
        if not landing_key and ingestion_result.metadata.get("landing_object_key"):
            landing_key = ingestion_result.metadata["landing_object_key"]
        if not landing_key:
            raise AirflowException("El conector REST API no devolvió la clave de Landing")

        extracted_count = ingestion_result.records_count or ingestion_result.metadata.get("total_records_extracted") or 0
    else:
        logger.info("Ingesta vía MockDataGenerator (USE_REST_API_INGEST=false)")
        generator = MockDataGenerator(seed=42)
        orders = generator.generate_orders(count=MOCK_ORDER_COUNT, customer_count=MOCK_CUSTOMER_COUNT)

        ingestor = LandingIngestor()
        result = ingestor.ingest_records(orders, dataset_name=DATASET_NAME, file_format="json")
        if result.status != "SUCCESS":
            raise AirflowException(f"Fallo en la ingesta hacia Landing: {result.error_message}")

        landing_key = result.metadata["object_key"]
        extracted_count = result.records_count

    # US-11: manifiesto de ejecución etiquetado con run_id / execution_date
    manifest = {
        "run_id": run_id,
        "execution_date": execution_date,
        "ingested_at": datetime.utcnow().isoformat(),
        "dataset": REST_API_DATASET_NAME if (USE_REST_API_INGEST and REST_API_BASE_URL) else DATASET_NAME,
        "object_key": landing_key,
        "records_count": extracted_count,
        "source": "rest_api" if (USE_REST_API_INGEST and REST_API_BASE_URL) else "mock",
    }
    S3Client().upload_json(
        manifest, settings.get_bucket("landing"), f"{manifest['dataset']}/_manifests/run_id={run_id}.json"
    )

    context["ti"].xcom_push(key="landing_key", value=landing_key)
    context["ti"].xcom_push(key="extracted_count", value=extracted_count)
    _record_task_metric(context, "extract_and_land", "LANDING", started, records=extracted_count)
    logger.info(f"[{run_id}] {extracted_count} registros depositados en Landing ({manifest['source']}): {landing_key}")
    return landing_key


def task_process_bronze(**context):
    """Promueve los datos de Landing a Bronze con metadatos de auditoría."""
    started = time.perf_counter()
    landing_key = context["ti"].xcom_pull(task_ids="extract_and_land", key="landing_key")

    result = BronzeLoader().process_landing_to_bronze(landing_key, dataset_name=DATASET_NAME)

    context["ti"].xcom_push(key="bronze_key", value=result["bronze_key"])
    _record_task_metric(context, "process_bronze", "BRONZE", started, records=result["records_count"])
    logger.info(f"Capa Bronze generada: {result['bronze_key']}")
    return result["bronze_key"]


def task_process_silver_and_quarantine(**context):
    """Ejecuta la transformación Silver y segrega anomalías a Quarantine."""
    started = time.perf_counter()
    bronze_key = context["ti"].xcom_pull(task_ids="process_bronze", key="bronze_key")

    result = SilverTransformer().process_bronze_to_silver(bronze_key, dataset_name=DATASET_NAME)
    if result["status"] != "SUCCESS":
        raise AirflowException("No se generaron registros válidos para la capa Silver.")

    report = result["report"]
    context["ti"].xcom_push(key="silver_key", value=result["silver_key"])
    context["ti"].xcom_push(key="quarantine_count", value=result["quarantine_records_count"])
    _record_task_metric(
        context,
        "process_silver_and_quarantine",
        "SILVER",
        started,
        records=result["valid_records_count"],
        quarantined=result["quarantine_records_count"],
    )

    if not report.passed_threshold:
        logger.warning(f"Tasa de cuarentena elevada: {report.quarantine_rate:.2%} (umbral superado).")
    logger.info(f"Capa Silver generada: {result['silver_key']} ({result['valid_records_count']} filas).")
    return result["silver_key"]


def task_process_gold(**context):
    """Construye las agregaciones Gold con limpieza previa de la partición (idempotencia)."""
    started = time.perf_counter()
    settings = get_settings()
    silver_key = context["ti"].xcom_pull(task_ids="process_silver_and_quarantine", key="silver_key")

    # US-11: limpieza de la partición Gold destino antes de reescribir (modo overwrite)
    gold_prefix = settings.storage_paths.get("gold_daily_sales", "analytics/daily_sales")
    PartitionManager().clean_partition("gold", gold_prefix)

    result = GoldAggregator().process_silver_to_gold(silver_key)
    if result["status"] != "SUCCESS":
        raise AirflowException("No se pudieron generar los KPIs de la capa Gold.")

    context["ti"].xcom_push(key="gold_key", value=result["gold_key"])
    context["ti"].xcom_push(key="kpis_count", value=result["kpis_count"])
    _record_task_metric(context, "process_gold", "GOLD", started, records=result["kpis_count"])
    logger.info(f"Capa Gold generada: {result['gold_key']} ({result['kpis_count']} KPIs).")
    return result["gold_key"]


def task_quality_gate(**context):
    """Valida que las métricas Gold no presenten anomalías numéricas."""
    started = time.perf_counter()
    settings = get_settings()
    gold_key = context["ti"].xcom_pull(task_ids="process_gold", key="gold_key")

    raw = S3Client().read_bytes(settings.get_bucket("gold"), gold_key)
    kpi_rows = pd.read_parquet(io.BytesIO(raw)).to_dict(orient="records")

    gate = GoldQualityGate().evaluate(kpi_rows)
    _record_task_metric(
        context,
        "quality_gate",
        "GOLD",
        started,
        status="SUCCESS" if gate.passed else "FAILED",
        records=gate.checked_metrics,
    )

    if not gate.passed:
        raise AirflowException(gate.summary())

    logger.info(gate.summary())
    return {"passed": gate.passed, "checked_metrics": gate.checked_metrics}


def task_publish_metrics(**context):
    """Registra el estado final de la corrida y publica las métricas operativas."""
    started = time.perf_counter()
    run_id = context.get("run_id", "manual")
    ti = context["ti"]

    extracted = ti.xcom_pull(task_ids="extract_and_land", key="extracted_count") or 0
    quarantined = ti.xcom_pull(task_ids="process_silver_and_quarantine", key="quarantine_count") or 0
    kpis = ti.xcom_pull(task_ids="process_gold", key="kpis_count") or 0

    TaskMetricsRecorder().record_run(
        run_id=run_id,
        pipeline_name=PIPELINE_NAME,
        status="SUCCESS",
        records_processed=extracted,
        records_quarantined=quarantined,
    )
    _record_task_metric(context, "publish_metrics", "ALL", started, records=kpis)

    logger.info(
        f"Corrida '{run_id}' publicada con éxito: {extracted} registros, "
        f"{quarantined} en cuarentena, {kpis} KPIs Gold."
    )
    return {"run_id": run_id, "records_processed": extracted, "records_quarantined": quarantined, "kpis": kpis}




def task_emit_lineage(**context):
    """Emite el linaje Medallion hacia OpenMetadata tras publicar métricas (US-09)."""
    started = time.perf_counter()
    run_id = context.get("run_id", "manual")
    execution_date = str(context.get("logical_date") or context.get("execution_date") or run_id)

    try:
        success = LineageEmitter().emit(run_id=run_id, execution_date=execution_date, pipeline_name=PIPELINE_NAME)
        status = "SUCCESS" if success else "SKIPPED"
        _record_task_metric(context, "emit_lineage", "ALL", started, status=status)
        logger.info(f"Linaje emitido (status={status}) para run_id='{run_id}'")
        return {"run_id": run_id, "lineage_emitted": success}
    except Exception as e:
        logger.warning(f"No se pudo emitir linaje (best-effort): {e}")
        _record_task_metric(context, "emit_lineage", "ALL", started, status="SKIPPED")
        return {"run_id": run_id, "lineage_emitted": False}

# ---------------------------------------------------------------------------
# Definición del DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "hector",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": [ALERT_EMAIL] if ALERT_EMAIL else [],
    "email_on_failure": bool(ALERT_EMAIL),
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id=PIPELINE_NAME,
    default_args=default_args,
    description="DAG maestro Medallion E2E: Landing -> Bronze -> Silver/Quarantine -> Gold (US-10/US-11)",
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_alert,
    tags=["medallion", "airflow", "orquestacion", "retail"],
) as dag:

    validate_environment = PythonOperator(
        task_id="validate_environment",
        python_callable=task_validate_environment,
        sla=SLA_MONITOR.sla_for("validate_environment"),
    )

    extract_and_land = PythonOperator(
        task_id="extract_and_land",
        python_callable=task_extract_and_land,
        sla=SLA_MONITOR.sla_for("extract_and_land"),
    )

    process_bronze = PythonOperator(
        task_id="process_bronze",
        python_callable=task_process_bronze,
        sla=SLA_MONITOR.sla_for("process_bronze"),
    )

    process_silver_and_quarantine = PythonOperator(
        task_id="process_silver_and_quarantine",
        python_callable=task_process_silver_and_quarantine,
        sla=SLA_MONITOR.sla_for("process_silver_and_quarantine"),
    )

    process_gold = PythonOperator(
        task_id="process_gold",
        python_callable=task_process_gold,
        sla=SLA_MONITOR.sla_for("process_gold"),
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=task_quality_gate,
        sla=SLA_MONITOR.sla_for("quality_gate"),
    )

    publish_metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=task_publish_metrics,
        sla=SLA_MONITOR.sla_for("publish_metrics"),
    )

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=task_emit_lineage,
        sla=SLA_MONITOR.sla_for("publish_metrics"),  # reutiliza SLA de publicación
    )

    # Dependencias estrictas y secuenciales (t1 >> t2 >> ... >> t7 >> t8)
    (
        validate_environment
        >> extract_and_land
        >> process_bronze
        >> process_silver_and_quarantine
        >> process_gold
        >> quality_gate
        >> publish_metrics
        >> emit_lineage
    )
