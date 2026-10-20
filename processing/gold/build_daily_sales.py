#!/usr/bin/env python3
"""
processing/gold/build_daily_sales.py
------------------------------------
Script ejecutable para construcción del modelo analítico daily_sales en capa Gold (US-07).
Responsable: Esteban Padilla.
Grano: (order_date, shipping_country, category).
Métricas: total_revenue, orders_count, units_sold, avg_ticket, returns_count, refunded_amount.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
import pandas as pd

# Añadir raíz al sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.gold.gold_aggregator import GoldAggregator

logger = get_logger("BuildDailySales")


def main():
    parser = argparse.ArgumentParser(description="Construcción Gold daily_sales (US-07)")
    parser.add_argument("--orders-silver", type=str, help="Ruta de órdenes en Silver (Parquet)")
    parser.add_argument("--returns-silver", type=str, help="Ruta de devoluciones en Silver (Parquet)")
    parser.add_argument("--output-path", type=str, help="Ruta de destino Parquet Gold")
    args = parser.parse_args()

    aggregator = GoldAggregator()

    orders_path = args.orders_silver or "data/mock/raw/orders.csv"
    if not os.path.exists(orders_path):
        logger.error(f"Archivo de entrada no encontrado: {orders_path}")
        return

    logger.info(f"Cargando dataset de órdenes desde: {orders_path}")
    if orders_path.endswith(".parquet"):
        df_orders = pd.read_parquet(orders_path)
    else:
        df_orders = pd.read_csv(orders_path)

    df_returns = None
    if args.returns_silver and os.path.exists(args.returns_silver):
        logger.info(f"Cargando dataset de devoluciones desde: {args.returns_silver}")
        df_returns = pd.read_parquet(args.returns_silver) if args.returns_silver.endswith(".parquet") else pd.read_csv(args.returns_silver)

    df_daily_sales = aggregator.build_daily_sales(df_orders, df_returns)

    out_path = args.output_path or f"data/gold/daily_sales_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    df_daily_sales.to_parquet(out_path, index=False, compression="snappy")

    logger.info(f"[OK] Modelo daily_sales generado exitosamente ({len(df_daily_sales)} registros) en: {out_path}")


if __name__ == "__main__":
    main()
