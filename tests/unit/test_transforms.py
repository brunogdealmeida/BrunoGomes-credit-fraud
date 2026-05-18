"""Unit tests for silver_transformation.py: validation mask, enrichment, quarantine."""

import pytest
from dataclasses import dataclass


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def validation():
    from config import ValidationConfig
    return ValidationConfig(
        valid_regions=["Europe", "Asia", "Africa", "North America", "South America"],
        valid_anomalies=["low_risk", "moderate_risk", "high_risk"],
        valid_transaction_types=["purchase", "sale", "transfer", "phishing", "scam"],
    )


@pytest.fixture(scope="module")
def valid_row():
    return {
        "timestamp": 1609459200,
        "sending_address": "addr_a",
        "receiving_address": "addr_b",
        "amount": 500.0,
        "transaction_type": "purchase",
        "location_region": "Europe",
        "ip_prefix": "192.168",
        "login_frequency": 3,
        "session_duration": 120,
        "purchase_pattern": "regular",
        "age_group": "adult",
        "risk_score": 45.0,
        "anomaly": "low_risk",
    }


def make_df(spark, rows):
    from pyspark.sql.types import (
        StructType, StructField, LongType, StringType, DoubleType, IntegerType
    )
    schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("sending_address", StringType(), True),
        StructField("receiving_address", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("location_region", StringType(), True),
        StructField("ip_prefix", StringType(), True),
        StructField("login_frequency", IntegerType(), True),
        StructField("session_duration", IntegerType(), True),
        StructField("purchase_pattern", StringType(), True),
        StructField("age_group", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("anomaly", StringType(), True),
    ])
    return spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)


# ── build_valid_mask ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_valid_mask_passes_clean_row(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    df = make_df(spark, [valid_row])
    assert df.filter(build_valid_mask(validation)).count() == 1


@pytest.mark.unit
def test_valid_mask_rejects_null_amount(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    row = {**valid_row, "amount": None}
    df = make_df(spark, [row])
    assert df.filter(build_valid_mask(validation)).count() == 0


@pytest.mark.unit
def test_valid_mask_rejects_zero_amount(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    row = {**valid_row, "amount": 0.0}
    df = make_df(spark, [row])
    assert df.filter(build_valid_mask(validation)).count() == 0


@pytest.mark.unit
def test_valid_mask_rejects_invalid_region(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    row = {**valid_row, "location_region": "Antarctica"}
    df = make_df(spark, [row])
    assert df.filter(build_valid_mask(validation)).count() == 0


@pytest.mark.unit
def test_valid_mask_rejects_invalid_anomaly(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    row = {**valid_row, "anomaly": "unknown_risk"}
    df = make_df(spark, [row])
    assert df.filter(build_valid_mask(validation)).count() == 0


@pytest.mark.unit
def test_valid_mask_rejects_risk_score_above_100(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    row = {**valid_row, "risk_score": 101.0}
    df = make_df(spark, [row])
    assert df.filter(build_valid_mask(validation)).count() == 0


@pytest.mark.unit
def test_valid_mask_rejects_null_sending_address(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    row = {**valid_row, "sending_address": None}
    df = make_df(spark, [row])
    assert df.filter(build_valid_mask(validation)).count() == 0


@pytest.mark.unit
def test_valid_mask_splits_mixed_batch(spark, validation, valid_row):
    from silver_transformation import build_valid_mask
    invalid_row = {**valid_row, "location_region": "Mars"}
    df = make_df(spark, [valid_row, invalid_row])
    mask = build_valid_mask(validation)
    assert df.filter(mask).count() == 1
    assert df.filter(~mask).count() == 1


# ── apply_enrichment ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_enrichment_cast_amount_to_double(spark, tables_yml, fraud_file_key, valid_row):
    from config import load_table_config
    from silver_transformation import apply_enrichment
    cfg = load_table_config(fraud_file_key, tables_yml)
    cast_rule = [r for r in cfg.silver.enrichment if r.transform == "cast" and r.column == "amount"]
    df = make_df(spark, [valid_row])
    result = apply_enrichment(df, cast_rule)
    assert dict(result.dtypes)["amount"] == "double"


@pytest.mark.unit
def test_enrichment_to_timestamp_adds_column(spark, tables_yml, fraud_file_key, valid_row):
    from config import load_table_config
    from silver_transformation import apply_enrichment
    cfg = load_table_config(fraud_file_key, tables_yml)
    ts_rule = [r for r in cfg.silver.enrichment if r.column == "transaction_datetime"]
    df = make_df(spark, [valid_row])
    result = apply_enrichment(df, ts_rule)
    assert "transaction_datetime" in result.columns
    assert dict(result.dtypes)["transaction_datetime"] == "timestamp"


@pytest.mark.unit
def test_enrichment_isin_marks_fraudulent_types(spark, tables_yml, fraud_file_key, valid_row):
    from config import load_table_config
    from silver_transformation import apply_enrichment
    cfg = load_table_config(fraud_file_key, tables_yml)
    isin_rule = [r for r in cfg.silver.enrichment if r.column == "is_fraudulent"]
    phishing_row = {**valid_row, "transaction_type": "phishing"}
    purchase_row = {**valid_row, "transaction_type": "purchase"}
    df = make_df(spark, [phishing_row, purchase_row])
    result = apply_enrichment(df, isin_rule)
    rows = {r["transaction_type"]: r["is_fraudulent"] for r in result.collect()}
    assert rows["phishing"] is True
    assert rows["purchase"] is False


@pytest.mark.unit
def test_enrichment_threshold_flags_high_risk(spark, tables_yml, fraud_file_key, valid_row):
    from config import load_table_config
    from silver_transformation import apply_enrichment
    cfg = load_table_config(fraud_file_key, tables_yml)
    threshold_rule = [r for r in cfg.silver.enrichment if r.column == "is_high_risk"]
    high_row = {**valid_row, "risk_score": 80.0}
    low_row  = {**valid_row, "risk_score": 40.0}
    df = make_df(spark, [high_row, low_row])
    result = apply_enrichment(df, threshold_rule)
    rows = {r["risk_score"]: r["is_high_risk"] for r in result.collect()}
    assert rows[80.0] is True
    assert rows[40.0] is False


@pytest.mark.unit
def test_enrichment_buckets_assigns_correct_tier(spark, tables_yml, fraud_file_key, valid_row):
    from config import load_table_config
    from silver_transformation import apply_enrichment
    cfg = load_table_config(fraud_file_key, tables_yml)
    bucket_rule = [r for r in cfg.silver.enrichment if r.column == "amount_tier"]
    cases = [
        ({**valid_row, "amount": 500.0},   "micro"),
        ({**valid_row, "amount": 5000.0},  "small"),
        ({**valid_row, "amount": 25000.0}, "medium"),
        ({**valid_row, "amount": 99999.0}, "large"),
    ]
    for row, expected_tier in cases:
        df = make_df(spark, [row])
        result = apply_enrichment(df, bucket_rule)
        actual = result.collect()[0]["amount_tier"]
        assert actual == expected_tier, f"amount={row['amount']} → expected {expected_tier}, got {actual}"


@pytest.mark.unit
def test_enrichment_current_timestamp_adds_column(spark, tables_yml, fraud_file_key, valid_row):
    from config import load_table_config
    from silver_transformation import apply_enrichment
    cfg = load_table_config(fraud_file_key, tables_yml)
    ts_rule = [r for r in cfg.silver.enrichment if r.column == "_processing_time"]
    df = make_df(spark, [valid_row])
    result = apply_enrichment(df, ts_rule)
    assert "_processing_time" in result.columns
    assert result.collect()[0]["_processing_time"] is not None


# ── apply_quarantine_rules ────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def quarantine_cfg(tables_yml, fraud_file_key):
    from config import load_table_config
    cfg = load_table_config(fraud_file_key, tables_yml)
    return cfg.silver.quarantine


@pytest.mark.unit
def test_quarantine_adds_rejection_column(spark, quarantine_cfg, validation, valid_row):
    from silver_transformation import apply_quarantine_rules
    invalid_row = {**valid_row, "location_region": "Mars"}
    df = make_df(spark, [invalid_row])
    result = apply_quarantine_rules(df, quarantine_cfg, validation)
    assert quarantine_cfg.rejection_column in result.columns


@pytest.mark.unit
def test_quarantine_adds_timestamp_column(spark, quarantine_cfg, validation, valid_row):
    from silver_transformation import apply_quarantine_rules
    invalid_row = {**valid_row, "amount": -1.0}
    df = make_df(spark, [invalid_row])
    result = apply_quarantine_rules(df, quarantine_cfg, validation)
    assert quarantine_cfg.timestamp_column in result.columns
    assert result.collect()[0][quarantine_cfg.timestamp_column] is not None


@pytest.mark.unit
def test_quarantine_reason_invalid_amount(spark, quarantine_cfg, validation, valid_row):
    from silver_transformation import apply_quarantine_rules
    row = {**valid_row, "amount": 0.0}
    df = make_df(spark, [row])
    result = apply_quarantine_rules(df, quarantine_cfg, validation)
    assert result.collect()[0][quarantine_cfg.rejection_column] == "invalid_amount"


@pytest.mark.unit
def test_quarantine_reason_invalid_region(spark, quarantine_cfg, validation, valid_row):
    from silver_transformation import apply_quarantine_rules
    row = {**valid_row, "location_region": "Atlantis"}
    df = make_df(spark, [row])
    result = apply_quarantine_rules(df, quarantine_cfg, validation)
    assert result.collect()[0][quarantine_cfg.rejection_column] == "invalid_region"


@pytest.mark.unit
def test_quarantine_reason_invalid_risk_score(spark, quarantine_cfg, validation, valid_row):
    from silver_transformation import apply_quarantine_rules
    row = {**valid_row, "risk_score": 150.0}
    df = make_df(spark, [row])
    result = apply_quarantine_rules(df, quarantine_cfg, validation)
    assert result.collect()[0][quarantine_cfg.rejection_column] == "invalid_risk_score"


@pytest.mark.unit
def test_quarantine_first_matching_rule_wins(spark, quarantine_cfg, validation, valid_row):
    # Row violates both amount and region — first rule (invalid_amount) should win
    from silver_transformation import apply_quarantine_rules
    row = {**valid_row, "amount": -5.0, "location_region": "Atlantis"}
    df = make_df(spark, [row])
    result = apply_quarantine_rules(df, quarantine_cfg, validation)
    assert result.collect()[0][quarantine_cfg.rejection_column] == "invalid_amount"
