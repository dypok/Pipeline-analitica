#!/usr/bin/env python3
"""
processing/gold/build_customer_360.py
-------------------------------------
Script ejecutable para construcción del modelo analítico customer_360 en capa Gold (US-07).
Responsable: Esteban Padilla.
Grano: customer_id.
Métricas: first_purchase_date, last_purchase_date, total_orders, lifetime_value (LTV),
avg_order_value, favorite_category, segmentación RFM y protección PII.
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

logger = get_logger("BuildCustomer360")


def main():
    parser = argparse.ArgumentParser(description="Construcción Gold customer_360 (US-07)")
    parser.add_argument("--orders-silver", type=str, help="Ruta de órdenes en Silver (Parquet)")
    parser.add_argument("--customers-silver", type=str, help="Ruta de clientes en Silver (Parquet/JSON)")
    parser.add_argument("--output-path", type=str, help="Ruta de destino Parquet Gold")
    args = parser.parse_args()

    aggregator = GoldAggregator()

    orders_path = args.orders_silver or "data/mock/raw/orders.csv"
    if not os.path.exists(orders_path):
        logger.error(f"Archivo de órdenes no encontrado: {orders_path}")
        return

    logger.info(f"Cargando dataset de órdenes desde: {orders_path}")
    df_orders = pd.read_parquet(orders_path) if orders_path.endswith(".parquet") else pd.read_csv(orders_path)

    df_customers = None
    cust_path = args.customers_silver or "data/mock/raw/customers.json"
    if os.path.exists(cust_path):
        logger.info(f"Cargando dataset de clientes para cruce PII protegido desde: {cust_path}")
        df_customers = pd.read_parquet(cust_path) if cust_path.endswith(".parquet") else pd.read_json(cust_path)

    df_cust_360 = aggregator.build_customer_360(df_orders, df_customers)

    out_path = args.output_path or f"data/gold/customer_360_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    df_cust_360.to_parquet(out_path, index=False, compression="snappy")

    logger.info(f"[OK] Modelo customer_360 generado exitosamente ({len(df_cust_360)} clientes) en: {out_path}")


if __name__ == "__main__":
    main()
