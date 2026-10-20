#!/usr/bin/env python3
"""Script de inicialización y verificación de buckets en MinIO."""

import os
import sys
import time

# Permitir importar src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client

logger = get_logger("MinIOInitializer")


def init_buckets():
    settings = get_settings()

    # Probar endpoint interno y endpoint de host
    candidate_endpoints = [
        os.getenv("MINIO_ENDPOINT"),
        settings.minio_host_endpoint,
        settings.minio_endpoint,
        "http://localhost:9000",
        "http://127.0.0.1:9000",
    ]
    candidate_endpoints = [ep for ep in candidate_endpoints if ep]

    s3 = None
    connected_endpoint = None

    for ep in candidate_endpoints:
        try:
            logger.info(f"Intentando conectar a MinIO en {ep}...")
            client = S3Client(endpoint_url=ep)
            client.list_objects("probe-test-bucket")
            s3 = client
            connected_endpoint = ep
            logger.info(f"[OK] Conectado exitosamente a MinIO en: {ep}")
            break
        except Exception:
            continue

    if s3 is None:
        logger.error("No se pudo conectar a MinIO en ninguno de los endpoints evaluados.")
        sys.exit(1)

    buckets = ["landing", "bronze", "silver", "gold", "quarantine"]
    logger.info("Verificando / creando buckets de la arquitectura Medallion:")
    for b in buckets:
        bucket_name = settings.get_bucket(b)
        s3.ensure_bucket_exists(bucket_name)
        logger.info(f" -> [OK] Bucket '{bucket_name}' listo.")

    logger.info("Inicialización de buckets de MinIO finalizada con éxito.")


if __name__ == "__main__":
    init_buckets()
