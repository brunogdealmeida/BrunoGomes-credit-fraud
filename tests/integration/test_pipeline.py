"""
Integration tests for the fraud lakehouse pipeline.

Requires a running Docker stack (minio, nessie, dremio).
Run with: pytest tests/integration -m integration
"""

import pytest

TABLE = "tb_fraud_credit"
BRONZE_SQL    = f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.bronze.{TABLE}"
SILVER_SQL    = f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.silver.{TABLE}"
QUARANTINE_SQL = f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.quarantine.{TABLE}"


# ── Nessie namespace + table existence ────────────────────────────────────────

@pytest.mark.integration
def test_nessie_bronze_namespace_exists(nessie):
    assert nessie.namespace_exists("bronze"), "Namespace 'bronze' not found in Nessie"


@pytest.mark.integration
def test_nessie_silver_namespace_exists(nessie):
    assert nessie.namespace_exists("silver"), "Namespace 'silver' not found in Nessie"


@pytest.mark.integration
def test_nessie_quarantine_namespace_exists(nessie):
    assert nessie.namespace_exists("quarantine"), "Namespace 'quarantine' not found in Nessie"


@pytest.mark.integration
def test_nessie_gold_namespace_exists(nessie):
    assert nessie.namespace_exists("gold"), "Namespace 'gold' not found in Nessie"


@pytest.mark.integration
def test_nessie_bronze_table_exists(nessie):
    assert nessie.table_exists("bronze", TABLE)


@pytest.mark.integration
def test_nessie_silver_table_exists(nessie):
    assert nessie.table_exists("silver", TABLE)


@pytest.mark.integration
def test_nessie_quarantine_table_exists(nessie):
    assert nessie.table_exists("quarantine", TABLE)


@pytest.mark.integration
@pytest.mark.parametrize("gold_table", [
    "fraud_by_region",
    "fraud_by_type",
    "daily_fraud_metrics",
    "high_risk_transactions",
    "risk_profile",
])
def test_nessie_gold_tables_exist(nessie, gold_table):
    assert nessie.table_exists("gold", gold_table), f"Gold table '{gold_table}' not found in Nessie"


# ── MinIO object presence ─────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.parametrize("prefix", ["bronze/", "silver/", "quarantine/", "gold/"])
def test_minio_layer_has_objects(minio, prefix):
    response = minio.list_objects_v2(Bucket="lakehouse", Prefix=prefix, MaxKeys=1)
    assert response.get("KeyCount", 0) > 0, f"No objects found under s3://lakehouse/{prefix}"


# ── Row counts via Dremio SQL ─────────────────────────────────────────────────

@pytest.mark.integration
def test_bronze_row_count_positive(dremio):
    count = int(dremio.scalar(BRONZE_SQL))
    assert count > 0, "Bronze table is empty"


@pytest.mark.integration
def test_silver_row_count_positive(dremio):
    count = int(dremio.scalar(SILVER_SQL))
    assert count > 0, "Silver table is empty"


@pytest.mark.integration
def test_quarantine_row_count_positive(dremio):
    count = int(dremio.scalar(QUARANTINE_SQL))
    assert count > 0, "Quarantine table is empty"


@pytest.mark.integration
def test_silver_plus_quarantine_equals_bronze(dremio):
    """Every bronze row must land in either silver or quarantine."""
    bronze    = int(dremio.scalar(BRONZE_SQL))
    silver    = int(dremio.scalar(SILVER_SQL))
    quarantine = int(dremio.scalar(QUARANTINE_SQL))
    assert silver + quarantine == bronze, (
        f"Row count mismatch: bronze={bronze}, silver={silver}, quarantine={quarantine}, "
        f"diff={bronze - (silver + quarantine)}"
    )


@pytest.mark.integration
@pytest.mark.parametrize("gold_table", [
    "fraud_by_region",
    "fraud_by_type",
    "daily_fraud_metrics",
    "high_risk_transactions",
    "risk_profile",
])
def test_gold_table_row_count_positive(dremio, gold_table):
    count = int(dremio.scalar(f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.gold.{gold_table}"))
    assert count > 0, f"Gold table '{gold_table}' is empty"


# ── Silver data quality ───────────────────────────────────────────────────────

@pytest.mark.integration
def test_silver_no_null_amounts(dremio):
    null_count = int(dremio.scalar(
        f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.silver.{TABLE} WHERE amount IS NULL"
    ))
    assert null_count == 0


@pytest.mark.integration
def test_silver_no_negative_amounts(dremio):
    neg_count = int(dremio.scalar(
        f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.silver.{TABLE} WHERE amount <= 0"
    ))
    assert neg_count == 0


@pytest.mark.integration
def test_silver_risk_score_in_range(dremio):
    out_of_range = int(dremio.scalar(
        f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.silver.{TABLE} "
        f"WHERE risk_score < 0 OR risk_score > 100"
    ))
    assert out_of_range == 0


@pytest.mark.integration
def test_silver_amount_tier_values(dremio):
    rows = dremio.query(
        f"SELECT DISTINCT amount_tier FROM nessie_lakehouse.silver.{TABLE}"
    )
    tiers = {r["amount_tier"] for r in rows}
    assert tiers <= {"micro", "small", "medium", "large"}


@pytest.mark.integration
def test_silver_is_fraudulent_only_for_phishing_scam(dremio):
    wrong = int(dremio.scalar(
        f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.silver.{TABLE} "
        f"WHERE is_fraudulent = TRUE AND transaction_type NOT IN ('phishing', 'scam')"
    ))
    assert wrong == 0


@pytest.mark.integration
def test_silver_valid_regions_only(dremio):
    rows = dremio.query(
        f"SELECT DISTINCT location_region FROM nessie_lakehouse.silver.{TABLE}"
    )
    regions = {r["location_region"] for r in rows}
    assert regions <= {"Africa", "Asia", "Europe", "North America", "South America"}


# ── Quarantine data quality ───────────────────────────────────────────────────

@pytest.mark.integration
def test_quarantine_rejection_reason_never_null(dremio):
    null_count = int(dremio.scalar(
        f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.quarantine.{TABLE} "
        f"WHERE _rejection_reason IS NULL"
    ))
    assert null_count == 0


@pytest.mark.integration
def test_quarantine_rejection_reasons_are_known(dremio):
    rows = dremio.query(
        f"SELECT DISTINCT _rejection_reason FROM nessie_lakehouse.quarantine.{TABLE}"
    )
    reasons = {r["_rejection_reason"] for r in rows}
    known = {"invalid_amount", "invalid_region", "invalid_anomaly",
             "invalid_transaction_type", "invalid_risk_score", "other"}
    unknown = reasons - known
    assert not unknown, f"Unexpected rejection reasons: {unknown}"


@pytest.mark.integration
def test_quarantine_timestamp_never_null(dremio):
    null_count = int(dremio.scalar(
        f"SELECT COUNT(*) AS cnt FROM nessie_lakehouse.quarantine.{TABLE} "
        f"WHERE _quarantine_time IS NULL"
    ))
    assert null_count == 0
