"""Job de procesamiento Spark Medallion para ejecución distribuida o local."""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date, year, month

def create_spark_session(app_name: str = "SparkMedallionPipeline") -> SparkSession:
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
        .config("spark.sql.shuffle.partitions", "2")
    )
    return builder.getOrCreate()


def run_spark_transformations(bronze_s3_path: str, silver_s3_path: str):
    """Ejecuta transformaciones Bronze -> Silver en Apache Spark."""
    spark = create_spark_session()
    print(f"Leyendo dataset Bronze desde {bronze_s3_path}...")

    df_bronze = spark.read.json(bronze_s3_path)
    print("Esquema Bronze:")
    df_bronze.printSchema()

    # Filtrar registros válidos
    df_valid = (
        df_bronze.filter(col("order_id").isNotNull())
        .filter(col("total_amount") > 0)
        .filter(col("status").isin("COMPLETED", "PENDING", "CANCELLED", "SHIPPED"))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .withColumn("_spark_processed_at", current_timestamp())
    )

    print(f"Escribiendo dataset Silver en {silver_s3_path} particionado por año y mes...")
    df_valid.write.mode("overwrite").partitionBy("year", "month").parquet(silver_s3_path)

    print("Procesamiento Spark Medallion completado con éxito.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 2:
        b_path = sys.argv[1]
        s_path = sys.argv[2]
    else:
        b_path = "s3a://bronze/orders.json"
        s_path = "s3a://silver/orders"

    run_spark_transformations(b_path, s_path)
