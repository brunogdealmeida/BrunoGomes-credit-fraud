"""Unit tests for config.py: YAML parsing, schema building, column operations."""

import pytest
from pathlib import Path


# ── load_table_config ─────────────────────────────────────────────────────────

@pytest.mark.unit
def test_table_name(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert cfg.table == "tb_fraud_credit"


@pytest.mark.unit
def test_schema_field_count(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert len(cfg.schema) == 13


@pytest.mark.unit
def test_schema_has_required_columns(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    names = {c.name for c in cfg.schema}
    assert {"timestamp", "amount", "risk_score", "anomaly", "transaction_type", "location_region"} <= names


@pytest.mark.unit
def test_unknown_file_key_raises(tables_yml):
    from config import load_table_config
    with pytest.raises(KeyError):
        load_table_config("nonexistent.csv", tables_yml)


# ── validation config ─────────────────────────────────────────────────────────

@pytest.mark.unit
def test_valid_regions_count(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert len(cfg.validation.valid_regions) == 5


@pytest.mark.unit
def test_valid_regions_values(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert set(cfg.validation.valid_regions) == {
        "Africa", "Asia", "Europe", "North America", "South America"
    }


@pytest.mark.unit
def test_valid_anomalies(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert set(cfg.validation.valid_anomalies) == {"low_risk", "moderate_risk", "high_risk"}


@pytest.mark.unit
def test_valid_transaction_types(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert "phishing" in cfg.validation.valid_transaction_types
    assert "scam" in cfg.validation.valid_transaction_types


# ── silver enrichment rules ───────────────────────────────────────────────────

@pytest.mark.unit
def test_enrichment_rule_count(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert len(cfg.silver.enrichment) == 10


@pytest.mark.unit
def test_enrichment_transforms_present(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    transforms = {r.transform for r in cfg.silver.enrichment}
    assert transforms >= {"to_timestamp", "to_date", "cast", "isin", "threshold", "buckets", "current_timestamp"}


@pytest.mark.unit
def test_bucket_rule_has_four_entries(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    bucket_rule = next(r for r in cfg.silver.enrichment if r.transform == "buckets")
    assert len(bucket_rule.buckets) == 4
    labels = [b.label for b in bucket_rule.buckets]
    assert labels == ["micro", "small", "medium", "large"]


@pytest.mark.unit
def test_threshold_rule_operator(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    threshold_rule = next(r for r in cfg.silver.enrichment if r.transform == "threshold")
    assert threshold_rule.operator == ">="
    assert threshold_rule.value == 75.0


# ── quarantine config ─────────────────────────────────────────────────────────

@pytest.mark.unit
def test_quarantine_rule_count(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    assert cfg.silver.quarantine is not None
    assert len(cfg.silver.quarantine.rules) == 5


@pytest.mark.unit
def test_quarantine_column_names(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    q = cfg.silver.quarantine
    assert q.rejection_column == "_rejection_reason"
    assert q.timestamp_column == "_quarantine_time"


@pytest.mark.unit
def test_quarantine_reasons(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    reasons = {r.reason for r in cfg.silver.quarantine.rules}
    assert reasons == {
        "invalid_amount", "invalid_region", "invalid_anomaly",
        "invalid_transaction_type", "invalid_risk_score",
    }


# ── get_spark_schema ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_spark_schema_field_count(spark, tables_yml, fraud_file_key):
    from config import load_table_config, get_spark_schema
    cfg = load_table_config(fraud_file_key, tables_yml)
    schema = get_spark_schema(cfg)
    assert len(schema.fields) == 13


@pytest.mark.unit
def test_spark_schema_types(spark, tables_yml, fraud_file_key):
    from pyspark.sql.types import DoubleType, StringType, DecimalType, IntegerType
    from config import load_table_config, get_spark_schema
    cfg = load_table_config(fraud_file_key, tables_yml)
    schema = get_spark_schema(cfg)
    field_map = {f.name: type(f.dataType) for f in schema.fields}
    assert field_map["risk_score"] is DoubleType
    assert field_map["sending_address"] is StringType
    assert field_map["amount"] is DecimalType
    assert field_map["login_frequency"] is IntegerType


# ── apply_column_ops ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_apply_column_ops_cast(spark, tables_yml, fraud_file_key):
    from pyspark.sql.types import TimestampType
    from config import load_table_config, apply_column_ops
    cfg = load_table_config(fraud_file_key, tables_yml)

    df = spark.createDataFrame([(1609459200,)], ["timestamp"])
    result = apply_column_ops(df, cfg)
    assert dict(result.dtypes)["timestamp"] == "timestamp"


@pytest.mark.unit
def test_apply_column_ops_skips_missing_column(spark, tables_yml, fraud_file_key):
    from config import load_table_config, apply_column_ops
    cfg = load_table_config(fraud_file_key, tables_yml)
    df = spark.createDataFrame([(1,)], ["other_column"])
    result = apply_column_ops(df, cfg)
    assert "other_column" in result.columns
