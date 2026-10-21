#!/usr/bin/env python3
"""
processing/silver/clean_returns.py
----------------------------------
Script ejecutable para limpieza y tipado de devoluciones (US-06).
Responsable: Esteban Padilla.
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

logger = get_logger("CleanReturns")


def main():
    parser = argparse.ArgumentParser(description="Procesamiento Silver de Devoluciones (US-06)")
    parser.add_argument("--bronze-key", type=str, help="Clave de objeto en MinIO Bronze")
    parser.add_argument("--input-file", type=str, help="Ruta local CSV")
    parser.add_argument("--output-dir", type=str, help="Directorio de salida local Parquet")
    args = parser.parse_args()

    transformer = SilverTransformer()

    if args.input_file:
        import pandas as pd
        logger.info(f"Procesando archivo local de devoluciones: {args.input_file}")
        df_raw = pd.read_csv(args.input_file)
        records = df_raw.to_dict(orient="records")
        df_clean, quarantine, report = transformer.transform_returns(records)

        out_dir = args.output_dir or "data/silver/returns"
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"returns_clean_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet")
        df_clean.to_parquet(out_file, index=False, compression="snappy")
        logger.info(f"[OK] {len(df_clean)} devoluciones limpias guardadas en: {out_file}")
    elif args.bronze_key:
        result = transformer.process_bronze_to_silver(args.bronze_key, dataset_name="returns")
        logger.info(f"Resultado Silver Devoluciones: {result['status']} | URI: {result.get('silver_uri')}")


if __name__ == "__main__":
    main()
