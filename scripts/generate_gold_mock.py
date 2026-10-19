"""Generador de datasets analíticos de muestra (Mock Gold) en formato Parquet.
Permite a la capa backend (DuckDB/FastAPI) y frontend (Dashboards) operar sin depender
de la ejecución previa de los jobs de Spark.
"""
from datetime import date, timedelta
from pathlib import Path
import random
import pandas as pd


def generate_mock_gold(output_dir: str = "data/mock/gold") -> None:
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    random.seed(42)

    categories = ["Electrónica", "Ropa", "Hogar", "Deportes", "Libros"]
    countries = ["Colombia", "México", "Chile", "Argentina", "Perú"]

    # 1. daily_sales.parquet
    daily_sales_records = []
    base_date = date(2026, 4, 1)
    for d in range(180):  # 6 meses
        current_date = base_date + timedelta(days=d)
        for country in countries:
            for cat in categories:
                units = random.randint(10, 200)
                price_avg = random.uniform(25.0, 150.0)
                revenue = round(units * price_avg, 2)
                orders = max(1, int(units * random.uniform(0.3, 0.7)))
                returns = int(orders * random.uniform(0.01, 0.08))
                refunded = round(returns * price_avg * 0.9, 2)
                ticket = round(revenue / orders, 2) if orders > 0 else 0.0

                daily_sales_records.append({
                    "order_date": current_date.isoformat(),
                    "shipping_country": country,
                    "category": cat,
                    "total_revenue": revenue,
                    "orders_count": orders,
                    "units_sold": units,
                    "avg_ticket": ticket,
                    "returns_count": returns,
                    "refunded_amount": refunded
                })

    df_sales = pd.DataFrame(daily_sales_records)
    sales_file = out_path / "daily_sales.parquet"
    df_sales.to_parquet(sales_file, engine="pyarrow", index=False)
    print(f"Generado {sales_file} con {len(df_sales)} registros.")

    # 2. customer_360.parquet
    customer_records = []
    for i in range(1, 501):
        c_id = f"CUST-{i:04d}"
        country = random.choice(countries)
        orders = random.randint(1, 40)
        ltv = round(orders * random.uniform(40.0, 300.0), 2)
        avg_order = round(ltv / orders, 2)
        reg_days = random.randint(30, 600)
        first_date = base_date - timedelta(days=reg_days)
        last_date = base_date + timedelta(days=random.randint(0, 170))
        cat = random.choice(categories)

        customer_records.append({
            "customer_id": c_id,
            "country": country,
            "first_purchase_date": first_date.isoformat(),
            "last_purchase_date": last_date.isoformat(),
            "total_orders": orders,
            "lifetime_value": ltv,
            "avg_order_value": avg_order,
            "favorite_category": cat
        })

    df_cust = pd.DataFrame(customer_records)
    cust_file = out_path / "customer_360.parquet"
    df_cust.to_parquet(cust_file, engine="pyarrow", index=False)
    print(f"Generado {cust_file} con {len(df_cust)} registros.")

    # 3. product_performance.parquet
    product_records = []
    for p in range(1, 101):
        p_id = f"PROD-{p:04d}"
        cat = random.choice(categories)
        p_name = f"Producto {cat} Mod-{p}"
        units = random.randint(50, 2000)
        price = round(random.uniform(15.0, 500.0), 2)
        revenue = round(units * price, 2)
        returns = int(units * random.uniform(0.01, 0.07))
        return_rate = round(returns / units, 4) if units > 0 else 0.0
        margin = round(random.uniform(0.15, 0.45), 4)

        product_records.append({
            "product_id": p_id,
            "product_name": p_name,
            "category": cat,
            "units_sold": units,
            "gross_revenue": revenue,
            "returns_count": returns,
            "return_rate": return_rate,
            "profit_margin": margin
        })

    df_prod = pd.DataFrame(product_records)
    prod_file = out_path / "product_performance.parquet"
    df_prod.to_parquet(prod_file, engine="pyarrow", index=False)
    print(f"Generado {prod_file} con {len(df_prod)} registros.")


if __name__ == "__main__":
    generate_mock_gold()
