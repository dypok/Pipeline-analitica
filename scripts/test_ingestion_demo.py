#!/usr/bin/env python3
"""
scripts/test_ingestion_demo.py
------------------------------
Script interactivo de demostración y prueba de la Capa de Ingesta (US-04, US-05).
Autor: Jefri Calderin (Ingeniero de Ingesta).

Demuestra en vivo:
1. Conexión hacia MinIO (Object Storage S3).
2. Extracción y depósito inmutable de archivo CSV con cálculo de hash SHA-256.
3. Extracción y depósito de archivo JSON / NDJSON.
4. Manejo y captura de errores ante un archivo CSV corrupto (demostrando resiliencia).
5. Verificación de objetos creados en el bucket Landing.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Asegurar path raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.common.config import get_settings
from src.common.s3_client import S3Client
from src.ingestion.csv_connector import CSVConnector
from src.ingestion.json_connector import JSONConnector


def print_header(title: str):
    print("\n" + "=" * 75)
    print(f" {title}")
    print("=" * 75)


def run_demo():
    print_header("DEMOSTRACIÓN EN VIVO: CAPA DE INGESTA (JEFRI CALDERIN)")
    
    settings = get_settings()
    endpoint = settings.minio_host_endpoint
    print(f"[*] Endpoint de MinIO detectado: {endpoint}")
    print(f"[*] Usuario: {settings.minio_access_key}")
    
    # 1. Inicializar cliente S3 hacia MinIO
    s3 = S3Client(endpoint_url=endpoint)
    s3.ensure_bucket_exists("landing")
    print("[OK] Bucket 'landing' verificado y listo en MinIO.")

    # 2. Prueba de Ingesta CSV Válida (US-04)
    print_header("1. INGESTA DE ARCHIVO CSV VÁLIDO (products.csv)")
    csv_path = BASE_DIR / "data" / "mock" / "raw" / "products.csv"
    if not csv_path.exists():
        print(f"[!] Archivo {csv_path} no encontrado. Ejecutando generador mock primero...")
        import subprocess
        subprocess.run([sys.executable, str(BASE_DIR / "scripts" / "generate_mock_data.py"), "--records", "500"], check=True)

    csv_conn = CSVConnector(file_path=str(csv_path), s3_client=s3, source_name="catalog_products_csv")
    res_csv = csv_conn.ingest(destination_bucket="landing", destination_prefix="retail/products")

    print(f" -> Estado de la Ingesta : {res_csv.status}")
    print(f" -> Registros extraídos : {res_csv.records_count:,}")
    print(f" -> Tamaño del archivo  : {res_csv.bytes_written:,} bytes")
    print(f" -> URI en MinIO S3     : {res_csv.destination_uri}")
    print(f" -> Hash SHA-256        : {res_csv.metadata.get('sha256')}")
    print(f" -> Delimitador detect. : '{res_csv.metadata.get('delimiter')}'")

    # 3. Prueba de Ingesta JSON (US-04)
    print_header("2. INGESTA DE ARCHIVO JSON JERÁRQUICO (customers.json)")
    json_path = BASE_DIR / "data" / "mock" / "raw" / "customers.json"
    json_conn = JSONConnector(file_path=str(json_path), s3_client=s3, source_name="crm_customers_json")
    res_json = json_conn.ingest(destination_bucket="landing", destination_prefix="crm/customers")

    print(f" -> Estado de la Ingesta : {res_json.status}")
    print(f" -> Clientes extraídos  : {res_json.records_count:,}")
    print(f" -> Tamaño del archivo  : {res_json.bytes_written:,} bytes")
    print(f" -> URI en MinIO S3     : {res_json.destination_uri}")
    print(f" -> Hash SHA-256        : {res_json.metadata.get('sha256')}")

    # 4. Prueba de Resiliencia ante CSV Corrupto (Captura Segura de Fallos)
    print_header("3. PRUEBA DE ROBUSTEZ: ARCHIVO CSV CORRUPTO O DESFASADO")
    corrupt_path = BASE_DIR / "data" / "mock" / "raw" / "corrupt" / "dirty_data.csv"
    if not corrupt_path.exists():
        corrupt_path = BASE_DIR / "data" / "mock" / "raw" / "orders_dirty.csv"

    print(f"[*] Procesando archivo con anomalías intencionales: {corrupt_path.name}")
    corrupt_conn = CSVConnector(file_path=str(corrupt_path), s3_client=s3, source_name="anomalous_data_test")
    res_corrupt = corrupt_conn.ingest(destination_bucket="landing", destination_prefix="test/corrupt")

    print(f" -> Estado reportado    : {res_corrupt.status} (Captura controlada)")
    if res_corrupt.status == "FAILED":
        print(f" -> Mensaje de error    : {res_corrupt.error_message}")
        print(" [OK] El conector detectó la anomalía y evitó corromper la plataforma.")
    else:
        print(f" -> Registros cargados  : {res_corrupt.records_count} (inyección a zona de análisis)")

    # 5. Resumen final para el usuario
    print_header("RESUMEN: CÓMO VERIFICAR ESTOS ARCHIVOS EN MINIO")
    print(" 1. Abre tu navegador web en: http://localhost:9001")
    print(" 2. Ingresa con las credenciales:")
    print("    - Usuario : minioadmin")
    print("    - Clave   : miniopassword123")
    print(" 3. En el menú izquierdo, haz clic en 'Buckets' y selecciona 'landing'.")
    print(" 4. Verás las carpetas creadas con la fecha de hoy:")
    print("    - landing/retail/products/YYYY/MM/DD/...")
    print("    - landing/crm/customers/YYYY/MM/DD/...")
    print("=" * 75)


if __name__ == "__main__":
    run_demo()
