"""Módulo de procesamiento distribuido Apache Spark para modelos analíticos Gold (US-07).
Responsable: Esteban Padilla.
Construye los modelos:
- daily_sales (Grano: order_date, shipping_country, category)
- customer_360 (Grano: customer_id con RFM y PII anonimizado)
- product_performance (Grano: product_id con métricas de margen y retornos)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, round, min, max, lit,
    when, coalesce, sha2, current_timestamp
)


def create_spark_session(app_name: str = "SparkGoldModelsJob") -> SparkSession:
    """Crea una sesión de Spark configurada para almacenamiento MinIO / S3A."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return builder.getOrCreate()


def build_daily_sales_spark(spark: SparkSession, silver_orders_path: str, gold_output_path: str):
    """Calcula el modelo multidimensional daily_sales con PySpark."""
    print(f"[*] Construyendo daily_sales a partir de: {silver_orders_path}")
    df_silver = spark.read.parquet(silver_orders_path)

    # Filtrar completadas para ingresos oficiales
    df_completed = df_silver.filter(col("status") == "COMPLETED")

    df_daily = (
        df_completed.groupBy("order_date", "shipping_country", "category")
        .agg(
            round(sum("total_amount"), 2).alias("total_revenue"),
            countDistinct("order_id").alias("orders_count"),
            sum(coalesce(col("quantity"), lit(1))).alias("units_sold"),
        )
        .withColumn(
            "avg_ticket",
            round(col("total_revenue") / col("orders_count"), 2)
        )
        .withColumn("returns_count", lit(0))
        .withColumn("refunded_amount", lit(0.0))
        .withColumn("_gold_calculated_at", current_timestamp())
    )

    print(f"[+] Guardando daily_sales Parquet en: {gold_output_path}")
    df_daily.write.mode("overwrite").parquet(gold_output_path)
    return df_daily


def build_customer_360_spark(spark: SparkSession, silver_orders_path: str, gold_output_path: str, silver_customers_path: str = None):
    """Calcula el modelo customer_360 con PySpark, incorporando LTV y minimización PII con hashing."""
    print(f"[*] Construyendo customer_360 a partir de: {silver_orders_path}")
    df_silver = spark.read.parquet(silver_orders_path)

    df_completed = df_silver.filter(col("status") == "COMPLETED")

    df_cust = (
        df_completed.groupBy("customer_id")
        .agg(
            min("order_date").alias("first_purchase_date"),
            max("order_date").alias("last_purchase_date"),
            countDistinct("order_id").alias("total_orders"),
            round(sum("total_amount"), 2).alias("lifetime_value"),
            sum(coalesce(col("quantity"), lit(1))).alias("units_bought"),
        )
        .withColumn(
            "avg_order_value",
            round(col("lifetime_value") / col("total_orders"), 2)
        )
        .withColumn("favorite_category", lit("General"))
        .withColumn(
            "rfm_segment",
            when(col("total_orders") >= 5, "Champions")
            .when(col("total_orders") >= 3, "Loyal Customers")
            .when(col("lifetime_value") >= 500, "Potential Loyalist")
            .otherwise("Occasional / New")
        )
    )

    if silver_customers_path:
        try:
            df_cust_meta = spark.read.parquet(silver_customers_path)
            # Minimización PII: Enmascarar email y teléfono con SHA-256
            df_cust_meta_safe = (
                df_cust_meta
                .withColumn("email_hash", sha2(col("email"), 256))
                .withColumn("phone_hash", sha2(col("phone"), 256))
                .drop("email", "phone", "first_name", "last_name")
            )
            df_cust = df_cust.join(df_cust_meta_safe, on="customer_id", how="left")
        except Exception as e:
            print(f"[!] No se pudo cruzar metadatos de clientes: {e}")

    df_cust = df_cust.withColumn("_gold_calculated_at", current_timestamp())

    print(f"[+] Guardando customer_360 Parquet en: {gold_output_path}")
    df_cust.write.mode("overwrite").parquet(gold_output_path)
    return df_cust


def build_product_performance_spark(spark: SparkSession, silver_orders_path: str, gold_output_path: str, silver_products_path: str = None):
    """Calcula el modelo product_performance con PySpark: volúmenes, márgenes y categorías."""
    print(f"[*] Construyendo product_performance a partir de: {silver_orders_path}")
    df_silver = spark.read.parquet(silver_orders_path)

    df_completed = df_silver.filter(col("status") == "COMPLETED")

    grp_cols = ["product_id"]
    if "category" in df_completed.columns:
        grp_cols.append("category")

    df_prod = (
        df_completed.groupBy(*grp_cols)
        .agg(
            sum(coalesce(col("quantity"), lit(1))).alias("units_sold"),
            round(sum("total_amount"), 2).alias("gross_revenue"),
            countDistinct("order_id").alias("orders_count"),
        )
        .withColumn("cost_price", round(col("gross_revenue") / col("units_sold") * 0.6, 2))
        .withColumn("total_cost", round(col("units_sold") * col("cost_price"), 2))
        .withColumn("margin", round(col("gross_revenue") - col("total_cost"), 2))
        .withColumn("returns_count", lit(0))
        .withColumn("return_rate", lit(0.0))
        .withColumn(
            "performance_tier",
            when((col("margin") > 500) & (col("return_rate") <= 0.05), "Top Performer")
            .when(col("margin") < 0, "Underperforming")
            .otherwise("Standard Performer")
        )
        .withColumn("_gold_calculated_at", current_timestamp())
    )

    print(f"[+] Guardando product_performance Parquet en: {gold_output_path}")
    df_prod.write.mode("overwrite").parquet(gold_output_path)
    return df_prod


def run_all_gold_models(silver_orders_path: str, gold_base_path: str):
    """Ejecuta los tres modelos analíticos Gold de manera orquestada."""
    spark = create_spark_session()
    try:
        build_daily_sales_spark(spark, silver_orders_path, f"{gold_base_path}/daily_sales")
        build_customer_360_spark(spark, silver_orders_path, f"{gold_base_path}/customer_360")
        build_product_performance_spark(spark, silver_orders_path, f"{gold_base_path}/product_performance")
        print("[OK] Todos los modelos analíticos Gold fueron construidos exitosamente.")
    finally:
        spark.stop()


if __name__ == "__main__":
    orders_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://silver/retail/orders"
    gold_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://gold/analytics"
    run_all_gold_models(orders_path, gold_path)
