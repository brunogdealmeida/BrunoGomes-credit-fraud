"""
Silver Layer — Data Quality Report

Reads tb_fraud_credit from the Silver Iceberg table and computes:
  - Completeness  : null counts and percentages per column
  - Validity      : domain rule violations (invalid values, out-of-range)
  - Conformance   : % of records passing each quality rule
  - Distributions : anomaly class, transaction type, region, risk score bands
  - Quarantine    : records rejected during silver transformation

Writes results to:
  - nessie.quality.silver_report (Iceberg table, queryable via Dremio/Superset)
  - /opt/dbt/target/quality_report.json  (served at http://localhost:18080/quality_report.json)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType

from config import load_spark_config, load_table_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("silver_quality_report")

JOBS_DIR = Path(os.getenv("SPARK_JOBS_DIR", str(Path(__file__).parent)))
REPORT_PATH = Path("/opt/dbt/target/quality_report.json")

REQUIRED_COLS = [
    "timestamp", "sending_address", "receiving_address",
    "amount", "transaction_type", "location_region",
    "anomaly", "risk_score",
]


def build_spark(sc) -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_quality_report")
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


def pct(part: int, total: int) -> float:
    return round(part / total * 100, 2) if total > 0 else 0.0


def main() -> None:
    file_key = sys.argv[1] if len(sys.argv) > 1 else "df_fraud_credit.csv"

    sc = load_spark_config(JOBS_DIR / "spark_config.yml")
    tc = load_table_config(file_key, JOBS_DIR / "tables.yml")
    spark = build_spark(sc)
    spark.sparkContext.setLogLevel("WARN")

    # ── Read tables ───────────────────────────────────────────────────────────
    silver = spark.table("nessie.silver.tb_fraud_credit")
    total_silver = silver.count()

    try:
        quarantine = spark.table("nessie.quarantine.tb_fraud_credit")
        quarantine_count = quarantine.count()
    except Exception:
        quarantine_count = 0
        logger.warning("Quarantine table not found — treating as 0 rejected records")

    total_input = total_silver + quarantine_count
    report_ts = datetime.now(timezone.utc).isoformat()

    logger.info("Silver records: %d | Quarantine records: %d", total_silver, quarantine_count)

    # ── Completeness: null checks per column ──────────────────────────────────
    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_COLS]
    null_row = silver.agg(*null_exprs).collect()[0].asDict()

    completeness = [
        {
            "column": col,
            "null_count": null_row[col],
            "null_pct": pct(null_row[col], total_silver),
            "status": "PASS" if null_row[col] == 0 else "FAIL",
        }
        for col in REQUIRED_COLS
    ]

    # ── Validity: domain rule checks ──────────────────────────────────────────
    valid_types = tc.validation.valid_transaction_types
    valid_regions = tc.validation.valid_regions
    valid_anomalies = tc.validation.valid_anomalies

    validity_rules = [
        {
            "rule": "transaction_type_valid",
            "description": f"transaction_type must be one of: {', '.join(valid_types)}",
            "error_count": silver.filter(~F.col("transaction_type").isin(valid_types)).count(),
        },
        {
            "rule": "location_region_valid",
            "description": f"location_region must be one of: {', '.join(valid_regions)}",
            "error_count": silver.filter(~F.col("location_region").isin(valid_regions)).count(),
        },
        {
            "rule": "anomaly_valid",
            "description": f"anomaly must be one of: {', '.join(valid_anomalies)}",
            "error_count": silver.filter(~F.col("anomaly").isin(valid_anomalies)).count(),
        },
        {
            "rule": "risk_score_range",
            "description": "risk_score must be between 0 and 100",
            "error_count": silver.filter(
                (F.col("risk_score") < 0) | (F.col("risk_score") > 100)
            ).count(),
        },
        {
            "rule": "amount_positive",
            "description": "amount must be greater than 0",
            "error_count": silver.filter(F.col("amount") <= 0).count(),
        },
        {
            "rule": "duplicate_records",
            "description": "No duplicate (sending_address, receiving_address, timestamp) combinations",
            "error_count": total_silver - silver.dropDuplicates(
                ["sending_address", "receiving_address", "timestamp"]
            ).count(),
        },
    ]

    for rule in validity_rules:
        rule["conformance_pct"] = pct(total_silver - rule["error_count"], total_silver)
        rule["status"] = "PASS" if rule["error_count"] == 0 else "FAIL"

    # ── Overall conformance ───────────────────────────────────────────────────
    null_errors = sum(c["null_count"] for c in completeness)
    validity_errors = sum(r["error_count"] for r in validity_rules)
    total_errors = null_errors + validity_errors
    overall_conformance = pct(total_silver - total_errors, total_silver)

    # ── Distributions ─────────────────────────────────────────────────────────
    def dist(col_name: str, label: str) -> list[dict]:
        return [
            {label: row[col_name], "count": row["count"], "pct": pct(row["count"], total_silver)}
            for row in silver.groupBy(col_name).count().orderBy(col_name).collect()
        ]

    anomaly_dist = dist("anomaly", "anomaly")
    type_dist = dist("transaction_type", "transaction_type")
    region_dist = dist("location_region", "location_region")

    risk_band_col = (
        F.when(F.col("risk_score") <= 25, "0–25")
         .when(F.col("risk_score") <= 50, "26–50")
         .when(F.col("risk_score") <= 75, "51–75")
         .otherwise("76–100")
    )
    risk_bands = [
        {"band": row["band"], "count": row["count"], "pct": pct(row["count"], total_silver)}
        for row in silver.select(risk_band_col.alias("band"))
                         .groupBy("band").count().orderBy("band").collect()
    ]

    # ── Quarantine rejection reasons (if available) ───────────────────────────
    quarantine_reasons: list[dict] = []
    if quarantine_count > 0:
        try:
            reason_rows = (
                spark.table("nessie.quarantine.tb_fraud_credit")
                     .groupBy("rejection_reason").count()
                     .orderBy(F.col("count").desc())
                     .collect()
            )
            quarantine_reasons = [
                {"reason": r["rejection_reason"], "count": r["count"], "pct": pct(r["count"], total_input)}
                for r in reason_rows
            ]
        except Exception:
            logger.warning("Could not read rejection_reason column from quarantine table")

    # ── Assemble report ───────────────────────────────────────────────────────
    report = {
        "generated_at": report_ts,
        "summary": {
            "total_input_records": total_input,
            "silver_records": total_silver,
            "quarantine_records": quarantine_count,
            "quarantine_rate_pct": pct(quarantine_count, total_input),
            "total_quality_errors": total_errors,
            "overall_conformance_pct": overall_conformance,
        },
        "completeness": completeness,
        "validity": validity_rules,
        "distributions": {
            "anomaly": anomaly_dist,
            "transaction_type": type_dist,
            "location_region": region_dist,
            "risk_score_bands": risk_bands,
        },
        "quarantine_rejection_reasons": quarantine_reasons,
    }

    # ── Write JSON (served by nginx at :18080) ────────────────────────────────
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    logger.info("JSON report written to %s", REPORT_PATH)

    # ── Write Iceberg quality table ───────────────────────────────────────────
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.quality")

    rows: list[tuple] = []
    for c in completeness:
        rows.append((
            report_ts, "completeness", c["column"], f"NULL check on {c['column']}",
            int(c["null_count"]), int(total_silver),
            float(c["null_pct"]), float(100 - c["null_pct"]), c["status"],
        ))
    for r in validity_rules:
        rows.append((
            report_ts, "validity", r["rule"], r["description"],
            int(r["error_count"]), int(total_silver),
            float(round(100 - r["conformance_pct"], 2)), float(r["conformance_pct"]), r["status"],
        ))

    row_dfs = [
        spark.range(1).select(
            F.lit(r[0]).cast(StringType()).alias("report_timestamp"),
            F.lit(r[1]).cast(StringType()).alias("check_type"),
            F.lit(r[2]).cast(StringType()).alias("check_name"),
            F.lit(r[3]).cast(StringType()).alias("description"),
            F.lit(int(r[4])).cast(LongType()).alias("error_count"),
            F.lit(int(r[5])).cast(LongType()).alias("total_records"),
            F.lit(float(r[6])).cast(DoubleType()).alias("error_pct"),
            F.lit(float(r[7])).cast(DoubleType()).alias("conformance_pct"),
            F.lit(r[8]).cast(StringType()).alias("status"),
        )
        for r in rows
    ]
    df = reduce(lambda a, b: a.union(b), row_dfs)
    df.writeTo("nessie.quality.silver_report").using("iceberg").createOrReplace()
    logger.info("Iceberg table written: nessie.quality.silver_report")

    # ── Console summary ───────────────────────────────────────────────────────
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  SILVER LAYER — DATA QUALITY REPORT")
    print(f"{sep}")
    print(f"  Generated at      : {report_ts}")
    print(f"  Total input       : {total_input:>12,}")
    print(f"  Silver (valid)    : {total_silver:>12,}  ({pct(total_silver, total_input):.1f}%)")
    print(f"  Quarantine        : {quarantine_count:>12,}  ({pct(quarantine_count, total_input):.1f}%)")
    print(f"  Quality errors    : {total_errors:>12,}")
    print(f"  Overall conformance: {overall_conformance:>10.2f}%")
    print(f"{sep}")
    print(f"\n  COMPLETENESS")
    for c in completeness:
        mark = "PASS" if c["status"] == "PASS" else "FAIL"
        print(f"    [{mark}] {c['column']:<22} nulls: {c['null_count']:>6}  ({c['null_pct']:.2f}%)")
    print(f"\n  VALIDITY")
    for r in validity_rules:
        mark = "PASS" if r["status"] == "PASS" else "FAIL"
        print(f"    [{mark}] {r['rule']:<28} errors: {r['error_count']:>6}  ({r['conformance_pct']:.2f}% conform.)")
    print(f"\n  ANOMALY DISTRIBUTION")
    for a in anomaly_dist:
        print(f"    {a['anomaly']:<20} {a['count']:>10,}  ({a['pct']:.1f}%)")
    print(f"\n  RISK SCORE BANDS")
    for b in risk_bands:
        print(f"    {b['band']:<10} {b['count']:>10,}  ({b['pct']:.1f}%)")
    if quarantine_reasons:
        print(f"\n  QUARANTINE REJECTION REASONS")
        for q in quarantine_reasons:
            print(f"    {q['reason']:<35} {q['count']:>8,}  ({q['pct']:.1f}%)")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
