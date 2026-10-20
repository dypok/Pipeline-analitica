"""Job de procesamiento Spark Medallion para ejecución distribuida o local (US-06).
Responsable: Esteban Padilla.
Aplica limpieza avanzada, tipado, deduplicación, separación a Quarantine y particionado Parquet.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_date, year, month,
    row_number, trim, upper, coalesce, sha2
)
from pyspark.sql.window import Window


def create_spark_session(app_name: str = "SparkMedallionSilverJob") -> SparkSession:
    """Crea una sesión de Spark configurada con Hadoop-AWS y soporte S3A para MinIO."""
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


def process_orders_spark(spark: SparkSession, bronze_path: str, silver_path: str, quarantine_path: str = None):
    """Transforma órdenes de Bronze a Silver con PySpark: deduplicación, validación y particionado."""
    print(f"[*] Leyendo órdenes desde Bronze: {bronze_path}")
    df_raw = spark.read.json(bronze_path)

    # 1. Validación de reglas críticas de negocio
    condition_valid = (
        col("order_id").isNotNull()
        & (trim(col("order_id")) != "")
        & col("customer_id").isNotNull()
        & (trim(col("customer_id")) != "")
        & col("total_amount").isNotNull()
        & (col("total_amount") > 0)
        & col("status").isNotNull()
        & upper(col("status")).isin("COMPLETED", "PENDING", "CANCELLED", "SHIPPED", "RETURNED")
    )

    df_valid = df_raw.filter(condition_valid)
    df_invalid = df_raw.filter(~condition_valid)

    # 2. Enrutar registros anómalos a Quarantine si aplica
    if quarantine_path and df_invalid.count() > 0:
        print(f"[!] Segregando registros anómalos hacia Quarantine: {quarantine_path}")
        df_quarantine = (
            df_invalid
            .withColumn("_rejected_at", current_timestamp())
            .withColumn("_entity_type", lit("orders"))
            .withColumn("_quarantine_reason", lit("Fallo en reglas de validación crítica (monto negativo o clave nula)"))
        )
        df_quarantine.write.mode("append").json(quarantine_path)

    # 3. Deduplicación determinista: retener el registro más reciente por ingested_at o id
    order_window = Window.partitionBy("order_id").orderBy(
        col("_metadata.ingested_at").desc_nulls_last() if "_metadata" in df_valid.columns else col("order_id")
    )
    df_dedup = (
        df_valid
        .withColumn("row_num", row_number().over(order_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # 4. Tipado estricto, normalización y particionado
    df_silver = (
        df_dedup
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("quantity", coalesce(col("quantity").cast("integer"), lit(1)))
        .withColumn("unit_price", coalesce(col("unit_price").cast("double"), lit(0.0)))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    # 5. Persistir Parquet en Silver particionado por year y month
    print(f"[+] Escribiendo Silver Parquet particionado en: {silver_path}")
    df_silver.write.mode("overwrite").partitionBy("year", "month").parquet(silver_path)
    print(f"[OK] Silver orders procesado exitosamente ({df_silver.count()} registros válidos).")


def run_spark_transformations(bronze_s3_path: str, silver_s3_path: str, quarantine_path: str = None):
    """Punto de entrada compatible para transformaciones generales en Spark."""
    spark = create_spark_session()
    try:
        process_orders_spark(spark, bronze_s3_path, silver_s3_path, quarantine_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    b_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://bronze/orders.json"
    s_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://silver/retail/orders"
    q_path = sys.argv[3] if len(sys.argv) > 3 else "s3a://quarantine/rejected/orders"

    run_spark_transformations(b_path, s_path, q_path)
