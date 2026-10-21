#!/usr/bin/env python3
"""
processing/silver/clean_orders.py
---------------------------------
Script ejecutable de Spark / Python para limpieza, deduplicación y tipado de órdenes (US-06).
Responsable: Esteban Padilla.
Lee datos Bronze, segrega anomalías a Quarantine y guarda Parquet en Silver particionado por año y mes.
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

logger = get_logger("CleanOrders")


def main():
    parser = argparse.ArgumentParser(description="Procesamiento Silver de Órdenes (US-06)")
    parser.add_argument("--bronze-key", type=str, help="Clave de objeto en MinIO Bronze")
    parser.add_argument("--input-file", type=str, help="Ruta de archivo local JSON/CSV para procesamiento directo")
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

        # Si viene de mock data relacional con orders y order_items separados
        items_path = os.path.join(os.path.dirname(args.input_file), "order_items.csv")
        if "total_amount" not in df_raw.columns and os.path.exists(items_path):
            logger.info(f"Integrando items de orden desde: {items_path}")
            df_items = pd.read_csv(items_path)
            df_raw = df_raw.merge(df_items, on="order_id", how="inner")

        if "order_status" in df_raw.columns and "status" not in df_raw.columns:
            df_raw["status"] = df_raw["order_status"].astype(str).str.upper()

        if "order_date" in df_raw.columns:
            df_raw["order_date"] = df_raw["order_date"].astype(str).str.slice(0, 10)

        records = df_raw.to_dict(orient="records")
        df_clean, quarantine, report = transformer.transform_orders(records)

        out_dir = args.output_dir or "data/silver/orders"
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"orders_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df_clean.to_parquet(out_file, index=False, compression="snappy")
        logger.info(f"[OK] {len(df_clean)} registros limpios guardados en: {out_file}")
        logger.info(f"[!] {len(quarantine)} registros enviados a Quarantine")
    elif args.bronze_key:
        result = transformer.process_bronze_to_silver(args.bronze_key, dataset_name="orders")
        logger.info(f"Resultado Silver: {result['status']} | URI: {result.get('silver_uri')}")
    else:
        logger.info("Modo por defecto: Procesando orders usando configuración de entorno.")
        settings = get_settings()
        s3 = S3Client()
        bronze_bucket = settings.get_bucket("bronze")
        # Listar últimos objetos en bronze
        objects = s3.list_objects(bronze_bucket, prefix="orders/")
        if objects:
            latest = sorted(objects)[-1]
            result = transformer.process_bronze_to_silver(latest, dataset_name="orders")
            logger.info(f"Procesamiento completado para {latest}: {result['status']}")
        else:
            logger.warning("No se encontraron objetos en bucket Bronze para 'orders/'.")


if __name__ == "__main__":
    main()
