#!/usr/bin/env python3
"""
processing/gold/build_product_performance.py
--------------------------------------------
Script ejecutable para construcción del modelo analítico product_performance en capa Gold (US-07).
Responsable: Esteban Padilla.
Grano: product_id.
Métricas: units_sold, gross_revenue, orders_count, cost_price, total_cost, margin,
returns_count, return_rate y clasificación de desempeño (performance_tier).
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

logger = get_logger("BuildProductPerformance")


def main():
    parser = argparse.ArgumentParser(description="Construcción Gold product_performance (US-07)")
    parser.add_argument("--orders-silver", type=str, help="Ruta de órdenes en Silver (Parquet)")
    parser.add_argument("--products-silver", type=str, help="Ruta de productos en Silver (Parquet/CSV)")
    parser.add_argument("--returns-silver", type=str, help="Ruta de devoluciones en Silver (Parquet/CSV)")
    parser.add_argument("--output-path", type=str, help="Ruta de destino Parquet Gold")
    args = parser.parse_args()

    aggregator = GoldAggregator()

    orders_path = args.orders_silver or "data/mock/raw/orders.csv"
    if not os.path.exists(orders_path):
        logger.error(f"Archivo de órdenes no encontrado: {orders_path}")
        return

    logger.info(f"Cargando dataset de órdenes desde: {orders_path}")
    df_orders = pd.read_parquet(orders_path) if orders_path.endswith(".parquet") else pd.read_csv(orders_path)

    df_products = None
    prod_path = args.products_silver or "data/mock/raw/products.csv"
    if os.path.exists(prod_path):
        logger.info(f"Cargando catálogo de productos desde: {prod_path}")
        df_products = pd.read_parquet(prod_path) if prod_path.endswith(".parquet") else pd.read_csv(prod_path)

    df_returns = None
    if args.returns_silver and os.path.exists(args.returns_silver):
        logger.info(f"Cargando dataset de devoluciones desde: {args.returns_silver}")
        df_returns = pd.read_parquet(args.returns_silver) if args.returns_silver.endswith(".parquet") else pd.read_csv(args.returns_silver)

    df_prod_perf = aggregator.build_product_performance(df_orders, df_products, df_returns)

    out_path = args.output_path or f"data/gold/product_performance_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    df_prod_perf.to_parquet(out_path, index=False, compression="snappy")

    logger.info(f"[OK] Modelo product_performance generado exitosamente ({len(df_prod_perf)} productos) en: {out_path}")


if __name__ == "__main__":
    main()
