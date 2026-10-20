#!/usr/bin/env python3
"""
processing/silver/clean_customers.py
------------------------------------
Script ejecutable para limpieza, deduplicación y normalización de clientes (US-06).
Responsable: Esteban Padilla.
Valida emails, formatos de texto y genera dataset Silver Parquet.
"""

import argparse
import os
import sys
from datetime import datetime, timezone

# Añadir raíz al sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client
from src.silver.silver_transformer import SilverTransformer

logger = get_logger("CleanCustomers")


def main():
    parser = argparse.ArgumentParser(description="Procesamiento Silver de Clientes (US-06)")
    parser.add_argument("--bronze-key", type=str, help="Clave de objeto en MinIO Bronze")
    parser.add_argument("--input-file", type=str, help="Ruta local JSON/CSV")
    parser.add_argument("--output-dir", type=str, help="Directorio de salida local Parquet")
    args = parser.parse_args()

    transformer = SilverTransformer()

    if args.input_file:
        import pandas as pd
        logger.info(f"Procesando archivo local: {args.input_file}")
        if args.input_file.endswith(".csv"):
            df_raw = pd.read_csv(args.input_file)
        else:
            df_raw = pd.read_json(args.input_file)

        records = df_raw.to_dict(orient="records")
        df_clean, quarantine, report = transformer.transform_customers(records)

        out_dir = args.output_dir or "data/silver/customers"
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"customers_clean_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet")
        df_clean.to_parquet(out_file, index=False, compression="snappy")
        logger.info(f"[OK] {len(df_clean)} clientes limpios guardados en: {out_file}")
        logger.info(f"[!] {len(quarantine)} registros enviados a Quarantine")
    elif args.bronze_key:
        result = transformer.process_bronze_to_silver(args.bronze_key, dataset_name="customers")
        logger.info(f"Resultado Silver Clientes: {result['status']} | URI: {result.get('silver_uri')}")
    else:
        logger.info("Modo por defecto: Procesando clientes usando configuración de entorno.")
        settings = get_settings()
        s3 = S3Client()
        bronze_bucket = settings.get_bucket("bronze")
        objects = s3.list_objects(bronze_bucket, prefix="customers/")
        if objects:
            latest = sorted(objects)[-1]
            result = transformer.process_bronze_to_silver(latest, dataset_name="customers")
            logger.info(f"Procesamiento completado para {latest}: {result['status']}")
        else:
            logger.warning("No se encontraron objetos en bucket Bronze para 'customers/'.")


if __name__ == "__main__":
    main()
