#!/usr/bin/env python3
"""
scripts/generate_mock_gold.py
-----------------------------
Genera Parquet mock de la capa Gold respetando el **contrato vinculante** del
proyecto (``docs/contratos_integracion.md`` §4 y el esquema que ya produce
``src/gold/gold_aggregator.py`` de Esteban Padilla).

Desbloquea a Sergio (dashboards) y Dylan (chatbot) mientras Esteban publica
los modelos reales (US-07) y Luis publica las muestras Parquet oficiales
(``data/mock/gold/*.parquet``).

NOTA: Existe otro generador, ``scripts/generate_mock_data.py`` (Jefri
Calderin, US-03), que produce un Gold con grano (order_date, country,
category) y sin las columnas ``completed_orders`` / ``cancelled_orders`` /
``distinct_customers``. Ese esquema **no coincide** con el contrato
vinculante. Este script reemplaza los archivos Gold por la versión canónica
para que los dashboards y el chatbot consuman el mismo esquema que
encontrarán en producción.

Genera en ``data/mock/gold/``:
  - ``daily_sales.parquet``         (Gold US-07: fecha×país×categoría → chatbot/Dylan)
  - ``daily_sales_kpis.parquet``    (data mart §4.1: 1 fila/día → dashboard/Sergio)
  - ``customer_360.parquet``        (US-07 + columna ``country``)
  - ``product_performance.parquet`` (US-07 + columnas ``product_name``, ``category``)

Separación de capas (importante):
  ``daily_sales.parquet`` es el Gold analítico en grano US-07 que consulta el
  backend de Dylan vía DuckDB (``v_daily_sales``). ``daily_sales_kpis.parquet``
  es el data mart pre-agregado (§4.1) que consume el dashboard de Sergio. Son
  modelos distintos en el contrato (§7.2 vs §4.1) y por eso viven en archivos
  separados: no deben pisarse.

Uso::

    python scripts/generate_mock_gold.py
    python scripts/generate_mock_gold.py --days 90 --customers 500 --products 100
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constantes y catálogos compartidos (alineados con scripts/generate_mock_data.py)
# ---------------------------------------------------------------------------
CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Sports",
    "Books", "Beauty", "Toys",
]
# Peso por categoría para que el ranking sea realista (Electronics lidera).
CATEGORY_WEIGHTS = [0.30, 0.20, 0.15, 0.10, 0.10, 0.10, 0.05]

COUNTRIES = ["CO", "MX", "AR", "CL", "PE", "US", "ES"]
COUNTRY_WEIGHTS = [0.25, 0.20, 0.15, 0.10, 0.08, 0.17, 0.05]

PRODUCT_NAMES_BY_CATEGORY = {
    "Electronics": ["Smartphone", "Laptop", "Wireless Headphones", "Smartwatch", "Keyboard"],
    "Clothing": ["Denim Jacket", "Cotton T-Shirt", "Running Sneakers", "Winter Coat"],
    "Home & Kitchen": ["Coffee Maker", "Air Fryer", "Blender", "Microwave Oven"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Cycling Helmet", "Tennis Racket"],
    "Books": ["Data Engineering Handbook", "Clean Architecture", "Cloud Fundamentals"],
    "Beauty": ["Hydrating Serum", "Sunscreen SPF 50", "Organic Shampoo", "Perfume"],
    "Toys": ["Building Blocks", "Puzzle Set", "Remote Car", "Board Game"],
}


def set_seed(seed: int = 42) -> None:
    random.seed(seed)


# ---------------------------------------------------------------------------
# daily_sales (Gold US-07 — consumido por el chatbot de Dylan vía DuckDB)
# ---------------------------------------------------------------------------
def generate_daily_sales(num_days: int = 180) -> pd.DataFrame:
    """Genera el modelo Gold ``daily_sales`` en el grano US-07.

    Grano (fecha × país × categoría), consumido por ``v_daily_sales`` del
    backend de Dylan (``app/backend/query_engine/duckdb_client.py``).

    Esquema (US-07 / ``docs/contratos_integracion.md`` §7.2):
        order_date, shipping_country, category, total_revenue, orders_count,
        units_sold, avg_ticket, returns_count, refunded_amount

    NOTA: Este es el Gold "crudo" analítico. El data mart pre-agregado para
    dashboards (grano 1 fila/día) vive en ``daily_sales_kpis.parquet`` (§4.1).
    """
    today = datetime.now().date()
    start_date = today - timedelta(days=num_days - 1)

    rows = []
    for day_offset in range(num_days):
        order_date = start_date + timedelta(days=day_offset)
        weekday = order_date.weekday()
        weekend_factor = 0.70 if weekday >= 5 else 1.0
        trend_factor = 1.0 + 0.20 * (day_offset / num_days)

        for country in COUNTRIES:
            country_weight = COUNTRY_WEIGHTS[COUNTRIES.index(country)]
            for category in CATEGORIES:
                cat_weight = CATEGORY_WEIGHTS[CATEGORIES.index(category)]
                # Base de órdenes por combinación día×país×categoría.
                # Escalada para que el Gold tenga volumen representativo
                # (~4,600 filas con 180 días, grano real sparse).
                base_orders = 40.0 * country_weight * cat_weight
                expected = base_orders * weekend_factor * trend_factor
                # Poisson-ish: ruido multiplicativo + mínimo 1 para no vaciar.
                orders_count = int(round(expected * random.uniform(0.6, 1.4)))
                # Solo ~70% de las combinaciones tienen actividad (sparse real).
                if orders_count <= 0 or random.random() > 0.70:
                    continue

                units_sold = int(round(orders_count * random.uniform(1.2, 2.5)))
                units_sold = max(units_sold, orders_count)
                avg_ticket = round(random.uniform(60.0, 260.0), 2)
                total_revenue = round(units_sold * avg_ticket, 2)

                # ~8-15% de las órdenes terminan en devolución.
                returns_count = int(orders_count * random.uniform(0.05, 0.15))
                refunded_amount = round(returns_count * avg_ticket, 2)

                rows.append({
                    "order_date": order_date,
                    "shipping_country": country,
                    "category": category,
                    "total_revenue": total_revenue,
                    "orders_count": orders_count,
                    "units_sold": units_sold,
                    "avg_ticket": avg_ticket,
                    "returns_count": returns_count,
                    "refunded_amount": refunded_amount,
                })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# daily_sales_kpis (Data Mart §4.1 — consumido por los dashboards de Sergio)
# ---------------------------------------------------------------------------
def generate_daily_sales_kpis(num_days: int = 180) -> pd.DataFrame:
    """Genera el data mart ``analytics.daily_sales_kpis`` (§4.1).

    Grano: 1 fila por día. Es el modelo pre-agregado que consume el dashboard
    de Sergio (``dashboards/data_loader.load_daily_sales``).

    Esquema (``docs/contratos_integracion.md`` §4.1):
        kpi_date, total_orders, completed_orders, cancelled_orders,
        total_revenue, average_ticket, distinct_customers, top_category, updated_at
    """
    today = datetime.now().date()
    start_date = today - timedelta(days=num_days - 1)

    rows = []
    for day_offset in range(num_days):
        kpi_date = start_date + timedelta(days=day_offset)
        weekday = kpi_date.weekday()

        # Efecto fin de semana: -30% sábados y domingos.
        weekend_factor = 0.70 if weekday >= 5 else 1.0
        # Tendencia ascendente leve: +20% desde el inicio hasta el final.
        trend_factor = 1.0 + 0.20 * (day_offset / num_days)

        base_orders = 180
        total_orders = int(
            base_orders * weekend_factor * trend_factor * random.uniform(0.85, 1.15)
        )
        total_orders = max(total_orders, 20)

        # Mix de estados: 75% completadas, 15% canceladas, 10% otras.
        completed_orders = int(total_orders * random.uniform(0.70, 0.80))
        cancelled_orders = int(total_orders * random.uniform(0.10, 0.18))
        # El resto se consideran otras (no contabilizan en revenue ni canceladas).

        average_ticket = round(random.uniform(80.0, 220.0), 2)
        total_revenue = round(completed_orders * average_ticket, 2)

        # Distinct customers: entre 65% y 85% de completed (clientes recurrentes).
        distinct_customers = int(completed_orders * random.uniform(0.65, 0.85))
        distinct_customers = max(distinct_customers, 1)

        # Categoría líder del día (ponderada).
        top_category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]

        rows.append({
            "kpi_date": kpi_date,
            "total_orders": total_orders,
            "completed_orders": completed_orders,
            "cancelled_orders": cancelled_orders,
            "total_revenue": total_revenue,
            "average_ticket": average_ticket,
            "distinct_customers": distinct_customers,
            "top_category": top_category,
            "updated_at": datetime.now(),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# customer_360 (contrato: US-07 + ``country`` para el choropleth)
# ---------------------------------------------------------------------------
def generate_customer_360(num_customers: int = 1000) -> pd.DataFrame:
    """Genera el modelo ``customer_360`` (US-07).

    Esquema base (US-07):
        customer_id, first_purchase_date, last_purchase_date, total_orders,
        lifetime_value, units_bought, avg_order_value, favorite_category
    Columna extra:
        country  (para la distribución geográfica en el dashboard)

    NOTA: ``units_bought`` la requiere el backend de Dylan; ``country`` la
    requiere el coroplético del dashboard de Sergio. Se conservan ambas.
    """
    today = datetime.now().date()
    rows = []
    for i in range(1, num_customers + 1):
        customer_id = f"CUST-{i:04d}"

        # Antigüedad: distribución sesgada a clientes relativamente nuevos.
        days_since_first = int(random.paretovariate(1.5))
        days_since_first = min(days_since_first, 365)
        first_purchase_date = today - timedelta(days=days_since_first)

        # Última compra entre first_purchase y hoy.
        max_days = (today - first_purchase_date).days
        last_purchase_date = first_purchase_date + timedelta(
            days=random.randint(0, max_days)
        )

        # Total de órdenes: Pareto (mayoría con pocas, pocos con muchas).
        total_orders = int(random.paretovariate(2.0))
        total_orders = min(total_orders, 60)
        total_orders = max(total_orders, 1)

        # Unidades compradas: ~1-3 unidades por orden.
        units_bought = int(total_orders * random.uniform(1.2, 3.0))

        # Valor promedio de orden.
        avg_order_value = round(random.uniform(50.0, 500.0), 2)
        lifetime_value = round(total_orders * avg_order_value * random.uniform(0.9, 1.1), 2)

        favorite_category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]

        rows.append({
            "customer_id": customer_id,
            "first_purchase_date": first_purchase_date,
            "last_purchase_date": last_purchase_date,
            "total_orders": total_orders,
            "lifetime_value": lifetime_value,
            "units_bought": units_bought,
            "avg_order_value": avg_order_value,
            "favorite_category": favorite_category,
            "country": country,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# product_performance (contrato: US-07 + ``product_name``, ``category``)
# ---------------------------------------------------------------------------
def generate_product_performance(num_products: int = 200) -> pd.DataFrame:
    """Genera el modelo ``product_performance`` (US-07).

    Esquema base (US-07):
        product_id, units_sold, gross_revenue, returns_count, return_rate, margin
    Columnas extra:
        product_name, category  (para la tabla interactiva y el filtro)

    NOTA: ``returns_count`` la consume el backend de Dylan (``v_product_performance``).
    """
    rows = []
    for i in range(1, num_products + 1):
        product_id = f"PROD-{i:04d}"
        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        base_name = random.choice(PRODUCT_NAMES_BY_CATEGORY[category])
        product_name = f"{base_name} Model {random.randint(100, 999)}"

        # Unidades vendidas: Pareto.
        units_sold = int(random.paretovariate(1.8))
        units_sold = min(units_sold, 5000)
        units_sold = max(units_sold, 10)

        # Precio unitario: rango según categoría.
        unit_price = round(random.uniform(20.0, 500.0), 2)
        gross_revenue = round(units_sold * unit_price, 2)

        # Devoluciones: ~1-20% de las unidades vendidas.
        returns_count = int(units_sold * random.uniform(0.01, 0.20))
        return_rate = round(returns_count / units_sold, 4)

        # Margen: entre 10% y 45% del gross_revenue.
        margin_rate = random.uniform(0.10, 0.45)
        margin = round(gross_revenue * margin_rate, 2)

        rows.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "units_sold": units_sold,
            "gross_revenue": gross_revenue,
            "returns_count": returns_count,
            "return_rate": return_rate,
            "margin": margin,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Escritura Parquet (Snappy, igual que el Gold real)
# ---------------------------------------------------------------------------
def write_parquet(df: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy")
    print(f"  [OK] {path.name}  ({len(df):,} filas, {len(df.columns)} columnas)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generador de Mock Gold (esquema canónico del contrato vinculante)."
    )
    parser.add_argument("--days", type=int, default=180,
                        help="Días de historia para daily_sales (default: 180)")
    parser.add_argument("--customers", type=int, default=1000,
                        help="Cantidad de clientes en customer_360 (default: 1000)")
    parser.add_argument("--products", type=int, default=200,
                        help="Cantidad de productos en product_performance (default: 200)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semilla para reproducibilidad (default: 42)")
    parser.add_argument("--output-dir", type=str, default="data/mock/gold",
                        help="Directorio de salida (default: data/mock/gold)")
    args = parser.parse_args()

    set_seed(args.seed)

    gold_dir = Path(args.output_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)

    start = datetime.now()
    print(f"[*] Generando Mock Gold (esquema canonico) en {gold_dir}/ ...")

    daily_sales_df = generate_daily_sales(args.days)
    daily_sales_kpis_df = generate_daily_sales_kpis(args.days)
    customer_360_df = generate_customer_360(args.customers)
    product_perf_df = generate_product_performance(args.products)

    write_parquet(daily_sales_df, gold_dir / "daily_sales.parquet")
    write_parquet(daily_sales_kpis_df, gold_dir / "daily_sales_kpis.parquet")
    write_parquet(customer_360_df, gold_dir / "customer_360.parquet")
    write_parquet(product_perf_df, gold_dir / "product_performance.parquet")

    elapsed = (datetime.now() - start).total_seconds()
    print(f"[+] Mock Gold generado en {elapsed:.2f}s.")
    print(f"    daily_sales (US-07):     {len(daily_sales_df):,} filas  [chatbot/Dylan]")
    print(f"    daily_sales_kpis (§4.1): {len(daily_sales_kpis_df):,} filas  [dashboard/Sergio]")
    print(f"    customer_360:            {len(customer_360_df):,} clientes")
    print(f"    product_performance:     {len(product_perf_df):,} productos")


if __name__ == "__main__":
    main()
