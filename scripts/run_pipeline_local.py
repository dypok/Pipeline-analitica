#!/usr/bin/env python3
"""Ejecución manual de demostración de extremo a extremo (E2E) del Pipeline Medallion."""

import json
import os
import sys
from datetime import datetime

# Añadir raíz al sys.path
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

logger = get_logger("PipelineRunner")


def run_pipeline():
    print("=" * 70)
    print(" PIPELINE MEDALLION — DEMOSTRACIÓN END-TO-END DE INTEGRACIÓN TÉCNICA")
    print("=" * 70)

    settings = get_settings()
    # Usar endpoint de host si se corre directamente desde el sistema host
    s3_endpoint = settings.minio_host_endpoint
    db_url = settings.host_analytics_db_url

    print(f"[*] Configuración S3: {s3_endpoint}")
    print(f"[*] Configuración Analytics DB: {db_url}")

    s3 = S3Client(endpoint_url=s3_endpoint)
    db = DatabaseClient(db_url=db_url)

    # 1. Generación de Datos de Prueba
    print("\n--- PASO 1: Generación de Datos Sintéticos (Retail Simulator) ---")
    gen = MockDataGenerator(seed=42)
    orders = gen.generate_orders(count=200, customer_count=40)
    print(f"[+] Generadas {len(orders)} órdenes de compra (incluye casos anómalos intencionales).")

    # 2. Ingesta a Landing
    print("\n--- PASO 2: Ingesta de Datos Crudos en MinIO Landing ---")
    ingestor = LandingIngestor(s3_client=s3)
    ingest_res = ingestor.ingest_records(orders, dataset_name="orders", file_format="json")
    print(f"[+] Ingesta en Landing exitosa:")
    print(f"    URI: {ingest_res.destination_uri}")
    print(f"    Bytes escritos: {ingest_res.bytes_written}")

    # 3. Carga a Bronze con Metadatos Inmutables
    print("\n--- PASO 3: Promoción a Capa Bronze (Auditoría Técnica) ---")
    landing_key = ingest_res.metadata["object_key"]
    loader = BronzeLoader(s3_client=s3)
    bronze_res = loader.process_landing_to_bronze(landing_key, dataset_name="orders")
    print(f"[+] Capa Bronze creada:")
    print(f"    URI: {bronze_res['bronze_uri']}")
    print(f"    Ingestion ID: {bronze_res['ingestion_id']}")

    # 4. Transformación Silver & Quarantine
    print("\n--- PASO 4: Transformación Silver (Parquet) y Segregación a Quarantine ---")
    transformer = SilverTransformer(s3_client=s3)
    silver_res = transformer.process_bronze_to_silver(bronze_res["bronze_key"], dataset_name="orders")
    print(f"[+] Capa Silver procesada:")
    print(f"    URI Parquet: {silver_res['silver_uri']}")
    print(f"    Registros válidos (Silver): {silver_res['valid_records_count']}")
    print(f"    Registros rechazados (Quarantine): {silver_res['quarantine_records_count']}")
    if silver_res.get("quarantine_uri"):
        print(f"    URI Quarantine: {silver_res['quarantine_uri']}")

    # 5. Agregación Gold & Publicación Analítica
    print("\n--- PASO 5: Agregación Analítica Gold (KPIs) y Data Mart ---")
    aggregator = GoldAggregator(s3_client=s3, db_client=db)
    gold_res = aggregator.process_silver_to_gold(silver_res["silver_key"])
    print(f"[+] Capa Gold finalizada:")
    print(f"    URI Parquet Gold: {gold_res['gold_uri']}")
    print(f"    KPIs diarios calculados: {gold_res['kpis_count']}")
    print(f"    Registros sincronizados en PostgreSQL: {gold_res['db_published_count']}")
    if gold_res.get("sample_kpi"):
        print(f"    Ejemplo de KPI diario:")
        print(f"      {json.dumps(gold_res['sample_kpi'], default=str, indent=6)}")

    # 6. Registro de Auditoría
    print("\n--- PASO 6: Registro de Auditoría Final ---")
    execution_id = f"LOCAL-RUN-{int(datetime.utcnow().timestamp())}"
    db.log_execution(
        execution_id=execution_id,
        pipeline_name="local_pipeline_test",
        layer="ALL",
        status="SUCCESS",
        records_processed=len(orders),
        records_quarantined=silver_res["quarantine_records_count"],
    )
    print(f"[+] Ejecución registrada con éxito en PostgreSQL analítico. Execution ID: {execution_id}")

    print("\n" + "=" * 70)
    print(" [EXITO TOTAL] El pipeline Medallion completó todas sus fases.")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()
