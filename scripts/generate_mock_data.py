#!/usr/bin/env python3
"""
scripts/generate_mock_data.py
-----------------------------
Generador de datos sintéticos (Mock Data) para la plataforma analítica (Arquitectura Medallion).
Cumple con US-03 (Responsable: Jefri Calderin).

Genera:
1. Clientes (customers.json)
2. Productos (products.csv y products.json)
3. Órdenes y Detalles de Órdenes (orders.csv, order_items.csv)
4. Devoluciones (returns.csv)
5. Registros Anómalos para pruebas de Quarantine (orders_dirty.csv y dirty_data.csv)
6. Flujo de API simulada (api_stream.json)
7. Capa Gold precocinada en Parquet (daily_sales.parquet, customer_360.parquet, product_performance.parquet)
"""

import argparse
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd


def set_seed(seed: int = 42) -> None:
    random.seed(seed)


def generate_customers(num_customers: int = 1000) -> list[dict]:
    first_names = [
        "Carlos", "Maria", "Juan", "Ana", "Luis", "Laura", "Pedro", "Sofia",
        "Diego", "Valentina", "Andres", "Camila", "Mateo", "Isabella", "Javier",
        "Lucia", "Santiago", "Daniela", "Fernando", "Elena", "Jefri", "Dylan",
        "Hector", "Esteban", "Sebastian", "Sergio"
    ]
    last_names = [
        "Gomez", "Rodriguez", "Perez", "Garcia", "Calderin", "Gamero", "Padilla",
        "Ortiz", "Hernandez", "Lopez", "Martinez", "Gonzalez", "Diaz", "Torres",
        "Ramirez", "Flores", "Acosta", "Castro", "Morales", "Vargas"
    ]
    countries = ["CO", "MX", "AR", "CL", "PE", "US", "ES"]
    cities_by_country = {
        "CO": ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena"],
        "MX": ["Ciudad de Mexico", "Guadalajara", "Monterrey", "Puebla"],
        "AR": ["Buenos Aires", "Cordoba", "Rosario", "Mendoza"],
        "CL": ["Santiago", "Valparaiso", "Concepcion"],
        "PE": ["Lima", "Arequipa", "Trujillo"],
        "US": ["Miami", "New York", "Austin", "Los Angeles"],
        "ES": ["Madrid", "Barcelona", "Valencia", "Sevilla"],
    }
    statuses = ["active", "active", "active", "inactive", "suspended"]

    start_date = datetime.now() - timedelta(days=730)
    customers = []

    for i in range(1, num_customers + 1):
        c_id = f"CUST-{i:04d}"
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        country = random.choice(countries)
        city = random.choice(cities_by_country[country])
        reg_date = (start_date + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d")
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(100000, 999999)}"
        status = random.choice(statuses)

        customers.append({
            "customer_id": c_id,
            "first_name": fn,
            "last_name": ln,
            "email": email,
            "phone": phone,
            "country": country,
            "city": city,
            "registration_date": reg_date,
            "status": status
        })

    return customers


def generate_products(num_products: int = 200) -> list[dict]:
    categories = {
        "Electronics": [
            ("Smartphone", 350.0, 900.0),
            ("Laptop", 600.0, 1800.0),
            ("Wireless Headphones", 40.0, 150.0),
            ("Smartwatch", 70.0, 300.0),
            ("Mechanical Keyboard", 45.0, 120.0),
            ("Gaming Mouse", 25.0, 80.0),
        ],
        "Home & Kitchen": [
            ("Coffee Maker", 30.0, 110.0),
            ("Air Fryer", 50.0, 160.0),
            ("Blender", 25.0, 90.0),
            ("Microwave Oven", 80.0, 220.0),
            ("Cookware Set", 40.0, 140.0),
        ],
        "Clothing": [
            ("Denim Jacket", 35.0, 85.0),
            ("Cotton T-Shirt", 10.0, 30.0),
            ("Running Sneakers", 45.0, 130.0),
            ("Casual Pants", 25.0, 65.0),
            ("Winter Coat", 70.0, 190.0),
        ],
        "Sports": [
            ("Yoga Mat", 15.0, 45.0),
            ("Dumbbell Set", 30.0, 95.0),
            ("Cycling Helmet", 25.0, 70.0),
            ("Tennis Racket", 50.0, 150.0),
            ("Football", 18.0, 40.0),
        ],
        "Books": [
            ("Data Engineering Handbook", 20.0, 55.0),
            ("Designing Data-Intensive Applications", 25.0, 60.0),
            ("Python Clean Architecture", 18.0, 48.0),
            ("Cloud Computing Fundamentals", 15.0, 40.0),
        ],
        "Beauty": [
            ("Hydrating Serum", 15.0, 50.0),
            ("Sunscreen SPF 50", 12.0, 35.0),
            ("Organic Shampoo", 10.0, 28.0),
            ("Perfume Eau de Parfum", 40.0, 120.0),
        ]
    }

    products = []
    prod_counter = 1
    cat_keys = list(categories.keys())

    while prod_counter <= num_products:
        cat = random.choice(cat_keys)
        subcat, min_p, max_p = random.choice(categories[cat])
        unit_price = round(random.uniform(min_p, max_p), 2)
        cost_price = round(unit_price * random.uniform(0.50, 0.70), 2)
        supplier_id = f"SUPP-{random.randint(1, 20):03d}"

        products.append({
            "product_id": f"PROD-{prod_counter:04d}",
            "product_name": f"{subcat} Model {random.randint(100, 999)}",
            "category": cat,
            "subcategory": subcat,
            "unit_price": unit_price,
            "cost_price": cost_price,
            "supplier_id": supplier_id
        })
        prod_counter += 1

    return products


def generate_orders_and_items(
    num_orders: int,
    customers: list[dict],
    products: list[dict]
) -> tuple[list[dict], list[dict]]:
    orders = []
    order_items = []

    statuses = ["completed", "completed", "completed", "completed", "cancelled", "returned"]
    payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash"]
    countries = ["CO", "MX", "AR", "CL", "PE", "US", "ES"]

    start_date = datetime.now() - timedelta(days=180)
    customer_ids = [c["customer_id"] for c in customers]
    cust_country_map = {c["customer_id"]: c["country"] for c in customers}

    item_counter = 1

    for i in range(1, num_orders + 1):
        order_id = f"ORD-{i:05d}"
        cust_id = random.choice(customer_ids)
        shipping_country = cust_country_map.get(cust_id, random.choice(countries))
        order_status = random.choice(statuses)
        payment_method = random.choice(payment_methods)
        
        # Date distribution in the last 180 days
        rand_seconds = random.randint(0, 180 * 24 * 3600)
        order_dt = start_date + timedelta(seconds=rand_seconds)
        order_date_str = order_dt.strftime("%Y-%m-%d %H:%M:%S")

        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_date": order_date_str,
            "order_status": order_status,
            "shipping_country": shipping_country,
            "payment_method": payment_method
        })

        # Generate 1 to 4 items per order
        num_items = random.randint(1, 4)
        chosen_products = random.sample(products, num_items)

        for prod in chosen_products:
            qty = random.randint(1, 5)
            u_price = prod["unit_price"]
            subtotal = round(qty * u_price, 2)
            # Discount 0% to 15%
            discount = round(subtotal * (random.choice([0.0, 0.0, 0.05, 0.10, 0.15])), 2)
            discounted = max(0.0, subtotal - discount)
            # Tax 19%
            tax = round(discounted * 0.19, 2)
            total = round(discounted + tax, 2)

            order_items.append({
                "item_id": f"ITEM-{item_counter:06d}",
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": u_price,
                "discount_amount": discount,
                "tax_amount": tax,
                "total_amount": total
            })
            item_counter += 1

    return orders, order_items


def generate_returns(orders: list[dict], order_items: list[dict]) -> list[dict]:
    returned_order_ids = {o["order_id"] for o in orders if o["order_status"] == "returned"}
    reasons = ["defective_product", "wrong_item_received", "buyer_remorse", "late_delivery"]
    
    returns = []
    ret_counter = 1

    items_by_order = {}
    for item in order_items:
        items_by_order.setdefault(item["order_id"], []).append(item)

    for order in orders:
        if order["order_id"] in returned_order_ids:
            items = items_by_order.get(order["order_id"], [])
            order_dt = datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
            return_dt = order_dt + timedelta(days=random.randint(1, 14), hours=random.randint(1, 10))

            for it in items:
                returns.append({
                    "return_id": f"RET-{ret_counter:05d}",
                    "order_id": order["order_id"],
                    "product_id": it["product_id"],
                    "return_date": return_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "reason": random.choice(reasons),
                    "refund_amount": it["total_amount"]
                })
                ret_counter += 1

    return returns


def generate_dirty_data(
    orders: list[dict],
    order_items: list[dict],
    customers: list[dict],
    dirty_ratio: float = 0.06
) -> list[dict]:
    """
    Inyecta entre 5% y 10% de registros anómalos para pruebas del motor de Quarantine.
    Casos anómalos:
    1. Fechas imposibles (ej: 2026-02-31, 2099-12-31, invalid_date)
    2. Claves foráneas inexistentes (customer_id = 999999 o CUST-9999)
    3. Precios negativos o cantidad = 0
    4. Emails malformados o nulos en campos obligatorios
    """
    dirty_records = []
    num_dirty = max(50, int(len(orders) * dirty_ratio))

    error_types = [
        "IMPOSSIBLE_DATE",
        "FUTURE_DATE",
        "INVALID_DATE_FORMAT",
        "NON_EXISTENT_CUSTOMER",
        "NEGATIVE_PRICE",
        "ZERO_QUANTITY",
        "NULL_MANDATORY_FIELD"
    ]

    for i in range(1, num_dirty + 1):
        err = random.choice(error_types)
        d_id = f"DIRTY-{i:05d}"
        
        rec = {
            "record_id": d_id,
            "order_id": f"ORD-ERR-{i:04d}",
            "customer_id": f"CUST-{random.randint(1, 100):04d}",
            "order_date": "2026-05-15 10:30:00",
            "product_id": f"PROD-{random.randint(1, 50):04d}",
            "quantity": 1,
            "unit_price": 50.0,
            "total_amount": 59.5,
            "shipping_country": "CO",
            "anomaly_type": err,
            "expected_quarantine_error": ""
        }

        if err == "IMPOSSIBLE_DATE":
            rec["order_date"] = "2026-02-31 08:00:00"
            rec["expected_quarantine_error"] = "ERR_INVALID_DATE"
        elif err == "FUTURE_DATE":
            rec["order_date"] = "2099-12-31 23:59:59"
            rec["expected_quarantine_error"] = "ERR_FUTURE_DATE"
        elif err == "INVALID_DATE_FORMAT":
            rec["order_date"] = "invalid_date_string"
            rec["expected_quarantine_error"] = "ERR_DATE_PARSE"
        elif err == "NON_EXISTENT_CUSTOMER":
            rec["customer_id"] = "999999"
            rec["expected_quarantine_error"] = "ERR_FK_NOT_FOUND"
        elif err == "NEGATIVE_PRICE":
            rec["unit_price"] = -49.99
            rec["total_amount"] = -49.99
            rec["expected_quarantine_error"] = "ERR_NEGATIVE_PRICE"
        elif err == "ZERO_QUANTITY":
            rec["quantity"] = 0
            rec["expected_quarantine_error"] = "ERR_ZERO_QUANTITY"
        elif err == "NULL_MANDATORY_FIELD":
            rec["order_id"] = ""
            rec["expected_quarantine_error"] = "ERR_NULL_KEY"

        dirty_records.append(rec)

    return dirty_records


def generate_api_stream(orders: list[dict], order_items: list[dict], num_records: int = 300) -> dict:
    """
    Genera payload simulando respuestas paginadas de la API REST externa de ventas.
    """
    items_by_order = {}
    for it in order_items:
        items_by_order.setdefault(it["order_id"], []).append(it)

    payload_records = []
    selected_orders = orders[:num_records]

    for ord_entry in selected_orders:
        its = items_by_order.get(ord_entry["order_id"], [])
        payload_records.append({
            "order_id": ord_entry["order_id"],
            "customer_id": ord_entry["customer_id"],
            "timestamp": ord_entry["order_date"],
            "status": ord_entry["order_status"],
            "country": ord_entry["shipping_country"],
            "payment": ord_entry["payment_method"],
            "items": its
        })

    return {
        "api_version": "v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "page": 1,
        "page_size": len(payload_records),
        "total_records": len(payload_records),
        "total_pages": 1,
        "next_token": None,
        "data": payload_records
    }


def generate_gold_datasets(
    orders: list[dict],
    order_items: list[dict],
    products: list[dict],
    returns: list[dict],
    gold_dir: Path
) -> None:
    """
    Genera datasets precocinados en formato Parquet para la capa Gold:
    - daily_sales.parquet
    - customer_360.parquet
    - product_performance.parquet
    """
    df_orders = pd.DataFrame(orders)
    df_items = pd.DataFrame(order_items)
    df_prods = pd.DataFrame(products)
    df_returns = pd.DataFrame(returns) if returns else pd.DataFrame(columns=["return_id", "order_id", "product_id", "refund_amount"])

    # Extraer fecha pura YYYY-MM-DD
    df_orders["order_date_day"] = df_orders["order_date"].str.slice(0, 10)

    # Merge orders + items + products
    merged = df_items.merge(df_orders, on="order_id", how="inner")
    merged = merged.merge(df_prods[["product_id", "category", "cost_price"]], on="product_id", how="left")

    # 1. daily_sales: Grano (order_date, shipping_country, category)
    completed_merged = merged[merged["order_status"] == "completed"]
    
    daily_sales_grp = completed_merged.groupby(["order_date_day", "shipping_country", "category"]).agg(
        total_revenue=("total_amount", "sum"),
        orders_count=("order_id", "nunique"),
        units_sold=("quantity", "sum")
    ).reset_index()

    daily_sales_grp["total_revenue"] = daily_sales_grp["total_revenue"].round(2)
    daily_sales_grp["avg_ticket"] = (daily_sales_grp["total_revenue"] / daily_sales_grp["orders_count"]).round(2)

    # Devoluciones por fecha y país
    if not df_returns.empty:
        ret_merged = df_returns.merge(df_orders[["order_id", "order_date_day", "shipping_country"]], on="order_id", how="left")
        ret_merged = ret_merged.merge(df_prods[["product_id", "category"]], on="product_id", how="left")
        ret_agg = ret_merged.groupby(["order_date_day", "shipping_country", "category"]).agg(
            returns_count=("return_id", "count"),
            refunded_amount=("refund_amount", "sum")
        ).reset_index()
        daily_sales_df = daily_sales_grp.merge(ret_agg, on=["order_date_day", "shipping_country", "category"], how="left")
        daily_sales_df["returns_count"] = daily_sales_df["returns_count"].fillna(0).astype(int)
        daily_sales_df["refunded_amount"] = daily_sales_df["refunded_amount"].fillna(0.0).round(2)
    else:
        daily_sales_df = daily_sales_grp
        daily_sales_df["returns_count"] = 0
        daily_sales_df["refunded_amount"] = 0.0

    daily_sales_df = daily_sales_df.rename(columns={"order_date_day": "order_date"})
    daily_sales_path = gold_dir / "daily_sales.parquet"
    daily_sales_df.to_parquet(daily_sales_path, index=False, compression="snappy")

    # 2. customer_360: Grano (customer_id)
    cust_grp = merged[merged["order_status"] == "completed"].groupby("customer_id").agg(
        first_purchase_date=("order_date_day", "min"),
        last_purchase_date=("order_date_day", "max"),
        total_orders=("order_id", "nunique"),
        lifetime_value=("total_amount", "sum"),
        units_bought=("quantity", "sum")
    ).reset_index()
    cust_grp["lifetime_value"] = cust_grp["lifetime_value"].round(2)
    cust_grp["avg_order_value"] = (cust_grp["lifetime_value"] / cust_grp["total_orders"]).round(2)

    # Categoría favorita
    fav_cat = merged.groupby(["customer_id", "category"])["quantity"].sum().reset_index()
    fav_cat = fav_cat.sort_values(["customer_id", "quantity"], ascending=[True, False]).drop_duplicates(subset=["customer_id"])
    fav_cat = fav_cat.rename(columns={"category": "favorite_category"})[["customer_id", "favorite_category"]]
    customer_360_df = cust_grp.merge(fav_cat, on="customer_id", how="left")

    customer_360_path = gold_dir / "customer_360.parquet"
    customer_360_df.to_parquet(customer_360_path, index=False, compression="snappy")

    # 3. product_performance: Grano (product_id)
    prod_perf = merged[merged["order_status"] == "completed"].groupby(["product_id", "category"]).agg(
        units_sold=("quantity", "sum"),
        gross_revenue=("total_amount", "sum"),
        orders_count=("order_id", "nunique")
    ).reset_index()
    prod_perf["gross_revenue"] = prod_perf["gross_revenue"].round(2)

    # Costos para calcular margen
    prod_costs = df_prods[["product_id", "product_name", "cost_price"]].drop_duplicates()
    prod_perf = prod_perf.merge(prod_costs, on="product_id", how="left")
    prod_perf["total_cost"] = (prod_perf["units_sold"] * prod_perf["cost_price"]).round(2)
    prod_perf["margin"] = (prod_perf["gross_revenue"] - prod_perf["total_cost"]).round(2)

    # Devoluciones por producto
    if not df_returns.empty:
        prod_ret = df_returns.groupby("product_id").agg(returns_count=("return_id", "count")).reset_index()
        prod_perf = prod_perf.merge(prod_ret, on="product_id", how="left")
        prod_perf["returns_count"] = prod_perf["returns_count"].fillna(0).astype(int)
    else:
        prod_perf["returns_count"] = 0

    prod_perf["return_rate"] = (prod_perf["returns_count"] / prod_perf["units_sold"]).round(4)
    prod_perf = prod_perf[["product_id", "product_name", "category", "units_sold", "gross_revenue", "returns_count", "return_rate", "margin"]]

    product_perf_path = gold_dir / "product_performance.parquet"
    prod_perf.to_parquet(product_perf_path, index=False, compression="snappy")


def write_csv(data: list[dict], filepath: Path) -> None:
    if not data:
        return
    keys = data[0].keys()
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


def write_json(data: object, filepath: Path) -> None:
    with open(filepath, mode="w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generador de Mock Data para Plataforma Medallion")
    parser.add_argument("--records", type=int, default=5000, help="Cantidad de órdenes a generar (default: 5000)")
    parser.add_argument("--customers", type=int, default=1000, help="Cantidad de clientes (default: 1000)")
    parser.add_argument("--products", type=int, default=200, help="Cantidad de productos (default: 200)")
    parser.add_argument("--seed", type=int, default=42, help="Semilla para reproducibilidad (default: 42)")
    parser.add_argument("--output-dir", type=str, default="data/mock/raw", help="Directorio para raw data")
    parser.add_argument("--gold-dir", type=str, default="data/mock/gold", help="Directorio para gold data")
    parser.add_argument("--api-dir", type=str, default="data/mock/api", help="Directorio para api spec y payloads")
    
    args = parser.parse_args()
    set_seed(args.seed)

    raw_dir = Path(args.output_dir)
    gold_dir = Path(args.gold_dir)
    api_dir = Path(args.api_dir)
    corrupt_dir = raw_dir / "corrupt"

    raw_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)
    api_dir.mkdir(parents=True, exist_ok=True)
    corrupt_dir.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now()
    print(f"[*] Iniciando generación de Mock Data (semilla={args.seed})...")

    # 1. Clientes
    print(f" -> Generando {args.customers} clientes...")
    customers = generate_customers(num_customers=args.customers)
    write_json(customers, raw_dir / "customers.json")

    # 2. Productos
    print(f" -> Generando {args.products} productos...")
    products = generate_products(num_products=args.products)
    write_csv(products, raw_dir / "products.csv")
    write_json(products, raw_dir / "products.json")

    # 3. Órdenes e Ítems
    print(f" -> Generando {args.records} órdenes de venta e ítems asociados...")
    orders, order_items = generate_orders_and_items(args.records, customers, products)
    write_csv(orders, raw_dir / "orders.csv")
    write_csv(order_items, raw_dir / "order_items.csv")

    # 4. Devoluciones
    print(" -> Generando devoluciones...")
    returns = generate_returns(orders, order_items)
    write_csv(returns, raw_dir / "returns.csv")

    # 5. Casos defectuosos / Quarantine (5% - 10%)
    print(" -> Inyectando anomalías sintéticas (Quarantine test cases)...")
    dirty_data = generate_dirty_data(orders, order_items, customers)
    write_csv(dirty_data, raw_dir / "orders_dirty.csv")
    write_csv(dirty_data, corrupt_dir / "dirty_data.csv")

    # 6. Stream API simulada
    print(" -> Generando payload de API REST sintética...")
    api_stream = generate_api_stream(orders, order_items, num_records=min(300, args.records))
    write_json(api_stream, raw_dir / "api_stream.json")
    write_json(api_stream, api_dir / "sample_sales_payload.json")

    # 7. Capa Gold sintética (Parquet precocinado)
    print(" -> Precocinando datasets analíticos en Capa Gold (Parquet)...")
    generate_gold_datasets(orders, order_items, products, returns, gold_dir)

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"[+] Generación completada con éxito en {elapsed:.2f} segundos.")
    print(f"    Raw data: {raw_dir}")
    print(f"    Gold data: {gold_dir}")


if __name__ == "__main__":
    main()
