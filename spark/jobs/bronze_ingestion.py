"""Bronze layer: ingest raw CSV into Iceberg table on MinIO via Nessie catalog."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

from config import SparkConfig, apply_column_ops, get_spark_schema, load_spark_config, load_table_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("bronze_ingestion")

JOBS_DIR = Path(os.getenv("SPARK_JOBS_DIR", str(Path(__file__).parent)))


def build_spark(sc: SparkConfig) -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_ingestion")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", sc.nessie_uri)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse", sc.warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", sc.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", sc.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", sc.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def main() -> None:
    file_name = sys.argv[1]

    sc = load_spark_config(JOBS_DIR / "spark_config.yml")
    cfg = load_table_config(file_name, JOBS_DIR / "tables.yml")
    file_path = str(Path(sc.dataset_dir) / file_name)

    spark = build_spark(sc)
    spark.sparkContext.setLogLevel("WARN")

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_table = f"nessie.bronze.{cfg.table}"
    logger.info(
        "Starting bronze ingestion  file=%s  batch_id=%s  source=%s  target=%s",
        file_name, batch_id, file_path, target_table,
    )

    spark_schema = get_spark_schema(cfg)

    df = (
        spark.read
        .option("header", "true")
        .schema(spark_schema)
        .csv(file_path)
    )
    logger.info("CSV loaded  path=%s  columns=%s", file_path, df.columns)

    df = apply_column_ops(df, cfg)

    df = (
        df
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", lit(file_name))
        .withColumn("_batch_id", lit(batch_id))
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    writer = (
        df.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
    )

    if cfg.partition_by:
        logger.info("Partitioning by: %s", cfg.partition_by)
        writer = writer.partitionedBy(*cfg.partition_by)

    writer.createOrReplace()

    if cfg.sort_order:
        order_expr = ", ".join(cfg.sort_order)
        logger.info("Setting Iceberg write sort order: %s", order_expr)
        spark.sql(f"ALTER TABLE {target_table} WRITE ORDERED BY {order_expr}")

    count = spark.table(target_table).count()
    logger.info("Bronze done  table=%s  records=%d  batch_id=%s", target_table, count, batch_id)
    spark.stop()


if __name__ == "__main__":
    main()
