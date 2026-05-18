"""
Integration tests for the fraud lakehouse pipeline.

Requires a running Docker stack (minio, nessie).
Run with: pytest tests/integration -m integration
"""

import pytest

TABLE = "tb_fraud_credit"


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


# ── Row counts via PyIceberg ──────────────────────────────────────────────────

@pytest.mark.integration
def test_bronze_row_count_positive(catalog):
    count = catalog.load_table(("bronze", TABLE)).scan().count_rows()
    assert count > 0, "Bronze table is empty"


@pytest.mark.integration
def test_silver_row_count_positive(catalog):
    count = catalog.load_table(("silver", TABLE)).scan().count_rows()
    assert count > 0, "Silver table is empty"


@pytest.mark.integration
def test_quarantine_row_count_positive(catalog):
    count = catalog.load_table(("quarantine", TABLE)).scan().count_rows()
    assert count > 0, "Quarantine table is empty"


@pytest.mark.integration
def test_silver_plus_quarantine_equals_bronze(catalog):
    """Every bronze row must land in either silver or quarantine."""
    bronze     = catalog.load_table(("bronze",     TABLE)).scan().count_rows()
    silver     = catalog.load_table(("silver",     TABLE)).scan().count_rows()
    quarantine = catalog.load_table(("quarantine", TABLE)).scan().count_rows()
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
def test_gold_table_row_count_positive(catalog, gold_table):
    count = catalog.load_table(("gold", gold_table)).scan().count_rows()
    assert count > 0, f"Gold table '{gold_table}' is empty"


# ── Silver data quality ───────────────────────────────────────────────────────

@pytest.mark.integration
def test_silver_no_null_amounts(silver_df):
    assert silver_df["amount"].isna().sum() == 0


@pytest.mark.integration
def test_silver_no_negative_amounts(silver_df):
    assert (silver_df["amount"] <= 0).sum() == 0


@pytest.mark.integration
def test_silver_risk_score_in_range(silver_df):
    out_of_range = ((silver_df["risk_score"] < 0) | (silver_df["risk_score"] > 100)).sum()
    assert out_of_range == 0


@pytest.mark.integration
def test_silver_amount_tier_values(silver_df):
    tiers = set(silver_df["amount_tier"].dropna().unique())
    assert tiers <= {"micro", "small", "medium", "large"}


@pytest.mark.integration
def test_silver_is_fraudulent_only_for_phishing_scam(silver_df):
    wrong = silver_df[
        (silver_df["is_fraudulent"] == True) &
        (~silver_df["transaction_type"].isin(["phishing", "scam"]))
    ]
    assert len(wrong) == 0


@pytest.mark.integration
def test_silver_valid_regions_only(silver_df):
    regions = set(silver_df["location_region"].dropna().unique())
    assert regions <= {"Africa", "Asia", "Europe", "North America", "South America"}


# ── Quarantine data quality ───────────────────────────────────────────────────

@pytest.mark.integration
def test_quarantine_rejection_reason_never_null(quarantine_df):
    assert quarantine_df["_rejection_reason"].isna().sum() == 0


@pytest.mark.integration
def test_quarantine_rejection_reasons_are_known(quarantine_df):
    reasons = set(quarantine_df["_rejection_reason"].dropna().unique())
    known = {"invalid_amount", "invalid_region", "invalid_anomaly",
             "invalid_transaction_type", "invalid_risk_score", "other"}
    unknown = reasons - known
    assert not unknown, f"Unexpected rejection reasons: {unknown}"


@pytest.mark.integration
def test_quarantine_timestamp_never_null(quarantine_df):
    assert quarantine_df["_quarantine_time"].isna().sum() == 0
