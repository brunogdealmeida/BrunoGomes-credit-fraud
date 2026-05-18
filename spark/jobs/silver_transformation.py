"""Silver layer: validate Bronze data, write clean records to Silver and rejects to Quarantine."""

import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp, when

from config import (
    BucketDef,
    EnrichmentRule,
    QuarantineConfig,
    SparkConfig,
    ValidationConfig,
    load_spark_config,
    load_table_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("silver_transformation")

JOBS_DIR = Path(os.getenv("SPARK_JOBS_DIR", str(Path(__file__).parent)))

_OPERATORS = {
    ">=": lambda c, v: c >= v,
    ">":  lambda c, v: c > v,
    "<=": lambda c, v: c <= v,
    "<":  lambda c, v: c < v,
    "==": lambda c, v: c == v,
    "!=": lambda c, v: c != v,
}


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark(sc: SparkConfig) -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_transformation")
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


# ── Validation mask ───────────────────────────────────────────────────────────

def build_valid_mask(validation: ValidationConfig) -> Any:
    """Row-level filter: keeps only records that pass all validation rules."""
    return (
        col("timestamp").isNotNull()
        & col("sending_address").isNotNull()
        & col("receiving_address").isNotNull()
        & col("amount").isNotNull()
        & (col("amount").cast("double") > 0)
        & col("location_region").isin(validation.valid_regions)
        & col("transaction_type").isin(validation.valid_transaction_types)
        & col("anomaly").isin(validation.valid_anomalies)
        & col("risk_score").isNotNull()
        & (col("risk_score").cast("double") >= 0)
        & (col("risk_score").cast("double") <= 100)
    )


# ── Silver enrichment ─────────────────────────────────────────────────────────

def apply_enrichment(df: Any, rules: list[EnrichmentRule]) -> Any:
    """Apply each enrichment rule defined in tables.yml as a withColumn operation."""
    for rule in rules:
        df = _apply_rule(df, rule)
        logger.debug("Enrichment applied  column=%s  transform=%s", rule.column, rule.transform)
    logger.info("Enrichment complete  rules_applied=%d", len(rules))
    return df


def _apply_rule(df: Any, rule: EnrichmentRule) -> Any:
    t = rule.transform

    if t == "cast":
        return df.withColumn(rule.column, col(rule.source).cast(rule.type))

    if t == "to_timestamp":
        src = col(rule.source).cast(rule.source_cast) if rule.source_cast else col(rule.source)
        return df.withColumn(rule.column, to_timestamp(src))

    if t == "to_date":
        src = col(rule.source).cast(rule.source_cast) if rule.source_cast else col(rule.source)
        return df.withColumn(rule.column, to_date(to_timestamp(src)))

    if t == "isin":
        return df.withColumn(rule.column, col(rule.source).isin(rule.values))

    if t == "threshold":
        op = _OPERATORS.get(rule.operator)
        if op is None:
            raise ValueError(f"Unknown operator '{rule.operator}' in enrichment rule '{rule.column}'")
        return df.withColumn(rule.column, op(col(rule.source), rule.value))

    if t == "buckets":
        return df.withColumn(rule.column, _build_bucket_expr(col(rule.source), rule.buckets))

    if t == "current_timestamp":
        return df.withColumn(rule.column, current_timestamp())

    raise ValueError(f"Unknown enrichment transform '{t}' for column '{rule.column}'")


def _build_bucket_expr(src_col: Any, buckets: list[BucketDef]) -> Any:
    expr = None
    otherwise_label = None
    for b in buckets:
        if b.otherwise or b.max is None:
            otherwise_label = b.label
            continue
        predicate = src_col < b.max
        expr = when(predicate, b.label) if expr is None else expr.when(predicate, b.label)
    if expr is None:
        raise ValueError("Buckets rule has no conditional entries (all marked as otherwise)")
    return expr.otherwise(otherwise_label)


# ── Silver quarantine ─────────────────────────────────────────────────────────

def apply_quarantine_rules(
    df: Any,
    cfg: QuarantineConfig,
    validation: ValidationConfig,
) -> Any:
    """Tag each rejected record with a rejection reason using rules from tables.yml."""
    value_map = asdict(validation)

    expr = None
    for rule in cfg.rules:
        predicate = _build_predicate(rule.condition, value_map)
        expr = when(predicate, rule.reason) if expr is None else expr.when(predicate, rule.reason)

    rejection_expr = expr.otherwise("other") if expr is not None else current_timestamp()

    logger.info(
        "Quarantine rules applied  rules=%d  rejection_col=%s",
        len(cfg.rules), cfg.rejection_column,
    )
    return (
        df
        .withColumn(cfg.rejection_column, rejection_expr)
        .withColumn(cfg.timestamp_column, current_timestamp())
    )


def _build_predicate(cond: Any, value_map: dict) -> Any:
    if cond.type == "null_or_threshold":
        c = col(cond.column).cast("double")
        op = _OPERATORS.get(cond.operator)
        if op is None:
            raise ValueError(f"Unknown operator '{cond.operator}' in quarantine condition")
        return c.isNull() | op(c, cond.value)

    if cond.type == "not_in":
        values = value_map.get(cond.values_ref, [])
        return ~col(cond.column).isin(values)

    if cond.type == "null_or_range":
        c = col(cond.column).cast("double")
        return c.isNull() | (c < cond.min) | (c > cond.max)

    raise ValueError(f"Unknown quarantine condition type '{cond.type}'")


# ── Pipeline ──────────────────────────────────────────────────────────────────

def main() -> None:
    file_name = sys.argv[1]

    sc  = load_spark_config(JOBS_DIR / "spark_config.yml")
    cfg = load_table_config(file_name, JOBS_DIR / "tables.yml")

    spark = build_spark(sc)
    spark.sparkContext.setLogLevel("WARN")

    bronze_table     = f"nessie.bronze.{cfg.table}"
    silver_table     = f"nessie.silver.{cfg.table}"
    quarantine_table = f"nessie.quarantine.{cfg.table}"

    logger.info(
        "Starting silver transformation  file=%s  source=%s  target=%s  quarantine=%s",
        file_name, bronze_table, silver_table, quarantine_table,
    )

    bronze = spark.table(bronze_table)
    logger.info("Bronze loaded  table=%s", bronze_table)

    valid_mask  = build_valid_mask(cfg.validation)
    valid_df    = bronze.filter(valid_mask)
    invalid_df  = bronze.filter(~valid_mask)
    logger.info(
        "Validation split  regions=%d  anomalies=%d  tx_types=%d",
        len(cfg.validation.valid_regions),
        len(cfg.validation.valid_anomalies),
        len(cfg.validation.valid_transaction_types),
    )

    silver_df     = apply_enrichment(valid_df, cfg.silver.enrichment)
    quarantine_df = apply_quarantine_rules(invalid_df, cfg.silver.quarantine, cfg.validation)

    # ── Write Silver ──────────────────────────────────────────────────────────
    logger.info("Writing silver table  target=%s", silver_table)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    (
        silver_df.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )

    # ── Write Quarantine ──────────────────────────────────────────────────────
    logger.info("Writing quarantine table  target=%s", quarantine_table)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.quarantine")
    (
        quarantine_df.writeTo(quarantine_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    silver_count     = spark.table(silver_table).count()
    quarantine_count = spark.table(quarantine_table).count()
    logger.info(
        "Silver done  silver_table=%s  silver_rows=%d  quarantine_rows=%d",
        silver_table, silver_count, quarantine_count,
    )
    spark.stop()


if __name__ == "__main__":
    main()
