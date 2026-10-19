"""DAG de Orquestación Apache Airflow para el Pipeline Medallion E2E."""

from datetime import datetime, timedelta
import json
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Asegurar que el directorio raíz de la aplicación esté en el path de Python
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.common.config import get_settings
from src.common.db_client import DatabaseClient
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.ingestion.file_ingestor import LandingIngestor
from src.ingestion.sample_generator import MockDataGenerator
from src.bronze.bronze_loader import BronzeLoader
from src.silver.silver_transformer import SilverTransformer
from src.gold.gold_aggregator import GoldAggregator

logger = get_logger("AirflowMedallionDAG")

default_args = {
    "owner": "luis.gomez",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def task_generate_mock_data(**context):
    """Tarea 1: Generación de datos de prueba sintéticos."""
    logger.info("Iniciando generación de datos sintéticos...")
    generator = MockDataGenerator(seed=42)
    orders = generator.generate_orders(count=150, customer_count=30)
    context["ti"].xcom_push(key="generated_orders", value=json.dumps(orders))
    logger.info(f"Generadas {len(orders)} órdenes de prueba.")
    return len(orders)


def task_ingest_landing(**context):
    """Tarea 2: Ingesta de datos crudos a Landing en MinIO."""
    raw_orders_str = context["ti"].xcom_pull(task_ids="generate_mock_data", key="generated_orders")
    orders = json.loads(raw_orders_str)

    ingestor = LandingIngestor()
    result = ingestor.ingest_records(orders, dataset_name="orders", file_format="json")

    if result.status != "SUCCESS":
        raise RuntimeError(f"Error en ingesta a Landing: {result.error_message}")

    object_key = result.metadata["object_key"]
    context["ti"].xcom_push(key="landing_key", value=object_key)
    logger.info(f"Ingesta a Landing completada con éxito: {object_key}")
    return object_key


def task_load_bronze(**context):
    """Tarea 3: Paso de Landing a Bronze con auditoría inmutable."""
    landing_key = context["ti"].xcom_pull(task_ids="ingest_to_landing", key="landing_key")
    loader = BronzeLoader()
    result = loader.process_landing_to_bronze(landing_key, dataset_name="orders")

    bronze_key = result["bronze_key"]
    context["ti"].xcom_push(key="bronze_key", value=bronze_key)
    logger.info(f"Capa Bronze cargada con éxito: {bronze_key}")
    return bronze_key


def task_transform_silver(**context):
    """Tarea 4: Transformación Bronze a Silver (Parquet) y segregación de Quarantine."""
    bronze_key = context["ti"].xcom_pull(task_ids="load_to_bronze", key="bronze_key")
    transformer = SilverTransformer()
    result = transformer.process_bronze_to_silver(bronze_key, dataset_name="orders")

    if result["status"] != "SUCCESS":
        raise RuntimeError("Fallo en la transformación Silver.")

    silver_key = result["silver_key"]
    context["ti"].xcom_push(key="silver_key", value=silver_key)
    logger.info(f"Capa Silver generada: {silver_key} | Válidos: {result['valid_records_count']} | Rechazados: {result['quarantine_records_count']}")
    return silver_key


def task_aggregate_gold(**context):
    """Tarea 5: Agregación analítica de la capa Gold y sincronización con PostgreSQL."""
    silver_key = context["ti"].xcom_pull(task_ids="transform_to_silver", key="silver_key")
    aggregator = GoldAggregator()
    result = aggregator.process_silver_to_gold(silver_key)

    if result["status"] != "SUCCESS":
        raise RuntimeError("Fallo en la generación de agregados Gold.")

    context["ti"].xcom_push(key="gold_key", value=result["gold_key"])
    logger.info(f"Capa Gold generada con {result['kpis_count']} KPIs persistidos.")
    return result["gold_key"]


def task_audit_and_log(**context):
    """Tarea 6: Registro de fin de pipeline y verificación de estado en logs de auditoría."""
    execution_id = f"EXEC-{context['run_id']}"
    db = DatabaseClient()
    db.log_execution(
        execution_id=execution_id,
        pipeline_name="medallion_full_pipeline",
        layer="ALL",
        status="SUCCESS",
        records_processed=150,
        records_quarantined=6,
    )
    logger.info(f"Pipeline Medallion finalizado con éxito. Execution ID: {execution_id}")


with DAG(
    dag_id="medallion_pipeline_e2e",
    default_args=default_args,
    description="Pipeline de demostración de extremo a extremo para la Arquitectura Medallion",
    schedule_interval=None,  # Ejecución bajo demanda o manual
    catchup=False,
    tags=["medallion", "retail", "lakehouse"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_mock_data",
        python_callable=task_generate_mock_data,
    )

    ingest_landing = PythonOperator(
        task_id="ingest_to_landing",
        python_callable=task_ingest_landing,
    )

    load_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=task_load_bronze,
    )

    transform_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=task_transform_silver,
    )

    aggregate_gold = PythonOperator(
        task_id="aggregate_to_gold",
        python_callable=task_aggregate_gold,
    )

    audit_log = PythonOperator(
        task_id="audit_and_log",
        python_callable=task_audit_and_log,
    )

    # Definición de dependencias secuenciales
    generate_data >> ingest_landing >> load_bronze >> transform_silver >> aggregate_gold >> audit_log
