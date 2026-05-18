"""Gold layer: business aggregations from Silver for analytics and dashboards."""

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max as _max,
    min as _min,
    round as _round,
    sum as _sum,
    when,
)

from config import SparkConfig, load_spark_config, load_table_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("gold_aggregation")

JOBS_DIR = Path(os.getenv("SPARK_JOBS_DIR", str(Path(__file__).parent)))


def build_spark(sc: SparkConfig) -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold_aggregation")
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


def write_gold(df, spark, table: str) -> int:
    target = f"nessie.gold.{table}"
    logger.info("Writing gold table  target=%s", target)
    (
        df.writeTo(target)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )
    row_count = spark.table(target).count()
    logger.info("Gold table written  target=%s  rows=%d", target, row_count)
    return row_count


def main() -> None:
    file_name = sys.argv[1]

    sc = load_spark_config(JOBS_DIR / "spark_config.yml")
    cfg = load_table_config(file_name, JOBS_DIR / "tables.yml")

    spark = build_spark(sc)
    spark.sparkContext.setLogLevel("WARN")

    silver_table = f"nessie.silver.{cfg.table}"
    logger.info("Starting gold aggregation  file=%s  source=%s", file_name, silver_table)

    silver = spark.table(silver_table)
    silver.cache()
    logger.info("Silver cached  table=%s", silver_table)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    # ── 1. Fraud summary by region ────────────────────────────────────────────
    logger.info("Building fraud_by_region")
    fraud_by_region = (
        silver
        .groupBy("location_region", "anomaly")
        .agg(
            count("*").alias("transaction_count"),
            _round(_sum("amount"), 2).alias("total_amount"),
            _round(avg("amount"), 2).alias("avg_amount"),
            _round(avg("risk_score"), 2).alias("avg_risk_score"),
            _max("amount").alias("max_amount"),
            count(when(col("is_fraudulent"), True)).alias("fraudulent_count"),
        )
        .withColumn("_created_at", current_timestamp())
    )
    write_gold(fraud_by_region, spark, "fraud_by_region")

    # ── 2. Transaction stats by type ──────────────────────────────────────────
    logger.info("Building fraud_by_type")
    fraud_by_type = (
        silver
        .groupBy("transaction_type", "anomaly", "purchase_pattern")
        .agg(
            count("*").alias("transaction_count"),
            _round(_sum("amount"), 2).alias("total_amount"),
            _round(avg("amount"), 2).alias("avg_amount"),
            _round(avg("risk_score"), 2).alias("avg_risk_score"),
            _round(avg("session_duration"), 1).alias("avg_session_duration"),
        )
        .withColumn("_created_at", current_timestamp())
    )
    write_gold(fraud_by_type, spark, "fraud_by_type")

    # ── 3. Daily fraud metrics ────────────────────────────────────────────────
    logger.info("Building daily_fraud_metrics")
    daily_metrics = (
        silver
        .groupBy("transaction_date", "location_region")
        .agg(
            count("*").alias("total_transactions"),
            count(when(col("anomaly") == "high_risk", True)).alias("high_risk_count"),
            count(when(col("anomaly") == "moderate_risk", True)).alias("moderate_risk_count"),
            count(when(col("anomaly") == "low_risk", True)).alias("low_risk_count"),
            count(when(col("is_fraudulent"), True)).alias("fraudulent_count"),
            _round(_sum("amount"), 2).alias("total_amount"),
            _round(avg("risk_score"), 2).alias("avg_risk_score"),
        )
        .withColumn("_created_at", current_timestamp())
    )
    write_gold(daily_metrics, spark, "daily_fraud_metrics")

    # ── 4. High-risk transactions ─────────────────────────────────────────────
    logger.info("Building high_risk_transactions  filter=(anomaly=high_risk OR risk_score>=75)")
    high_risk = (
        silver
        .filter((col("anomaly") == "high_risk") | (col("risk_score") >= 75.0))
        .select(
            "transaction_datetime",
            "transaction_date",
            "sending_address",
            "receiving_address",
            "amount",
            "amount_tier",
            "transaction_type",
            "location_region",
            "ip_prefix",
            "risk_score",
            "anomaly",
            "age_group",
            "purchase_pattern",
            "login_frequency",
            "session_duration",
            "is_fraudulent",
        )
        .withColumn("_created_at", current_timestamp())
    )
    write_gold(high_risk, spark, "high_risk_transactions")

    # ── 5. Risk profile by user segment ──────────────────────────────────────
    logger.info("Building risk_profile")
    risk_profile = (
        silver
        .groupBy("age_group", "purchase_pattern", "anomaly")
        .agg(
            count("*").alias("transaction_count"),
            _round(avg("risk_score"), 2).alias("avg_risk_score"),
            _round(_min("risk_score"), 2).alias("min_risk_score"),
            _round(_max("risk_score"), 2).alias("max_risk_score"),
            _round(avg("amount"), 2).alias("avg_amount"),
            _round(avg("login_frequency"), 2).alias("avg_login_frequency"),
            _round(avg("session_duration"), 2).alias("avg_session_duration"),
        )
        .withColumn("_created_at", current_timestamp())
    )
    write_gold(risk_profile, spark, "risk_profile")

    silver.unpersist()
    logger.info(
        "Gold aggregation complete  source=%s  "
        "tables=[fraud_by_region, fraud_by_type, daily_fraud_metrics, high_risk_transactions, risk_profile]",
        silver_table,
    )
    spark.stop()


if __name__ == "__main__":
    main()
