# Fraud Detection Data Lakehouse

A production-style open-source data lakehouse for fraud transaction analytics using the **medallion architecture** (Bronze → Silver → Quarantine → Gold).

## Architecture

```
CSV Dataset
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                     Apache Airflow                       │  Orchestration
│  bronze_ingestion → silver_transformation → gold_agg    │
│                              → dbt quality tests        │
└─────────────────────────────────────────────────────────┘
    │             │                │
    ▼             ▼                ▼
  Bronze        Silver          Gold tables
  (Iceberg)   (Iceberg)        (Iceberg)
               │
               ▼ rejected
           Quarantine
           (Iceberg)
              ▲─────────────────────────────┐
              │         MinIO (S3)           │  Storage
              │   bucket: lakehouse/         │
              └─────────────────────────────┘
              ▲
              │  Nessie REST Catalog
              │  (Iceberg table registry)
              ▼
           Dremio                            Query Engine / DW
           nessie_lakehouse.silver.*
           nessie_lakehouse.gold.*
              │
              ▼
           Apache Superset                  Dashboard
           Fraud Detection Analytics
```

## Dataset

`dataset/df_fraud_credit.csv` — 1 048 575 rows of cryptocurrency transactions:

| Column           | Type    | Description                                      |
|-----------------|---------|--------------------------------------------------|
| timestamp        | long    | Unix epoch                                       |
| sending_address  | string  | Sender wallet address                            |
| receiving_address| string  | Receiver wallet address                          |
| amount           | double  | Transaction amount (0–76 771)                    |
| transaction_type | string  | purchase / sale / transfer / phishing / scam     |
| location_region  | string  | Africa / Asia / Europe / North America / South America |
| ip_prefix        | string  | IP prefix of the connection                      |
| login_frequency  | int     | Number of logins before transaction              |
| session_duration | int     | Session length in seconds                        |
| purchase_pattern | string  | focused / high_value / random                    |
| age_group        | string  | established / new / veteran                      |
| risk_score       | double  | 0–100                                            |
| anomaly          | string  | low_risk / moderate_risk / high_risk             |

## Services

| Service        | URL                         | Credentials              |
|----------------|-----------------------------|--------------------------|
| MinIO Console  | http://localhost:9001       | admin / password123      |
| Nessie API     | http://localhost:19120/api/v1 | —                       |
| Spark UI       | http://localhost:8080       | —                        |
| Airflow UI     | http://localhost:8082       | admin / admin123         |
| Dremio UI      | http://localhost:9047       | admin / Admin1234!       |
| Superset UI    | http://localhost:8088       | admin / admin123         |

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24 with ≥ 12 GB RAM allocated
- Docker Compose v2

### 1. Start the stack

```bash
make build    # builds images (~10 min, downloads Spark + JARs once)
make start    # starts all services
```

### 2. Run the pipeline

Wait ~2 minutes for all services to become healthy, then:

```bash
make pipeline    # triggers the Airflow DAG
```

Or open the Airflow UI → DAGs → `fraud_lakehouse_pipeline` → Trigger.

### 3. View the dashboard

After the pipeline completes (~5–10 min):
1. Open Superset at http://localhost:8088
2. Navigate to **Dashboards → Fraud Detection Analytics**

## Pipeline Steps

```
bronze_ingestion        Read CSV → write Iceberg Bronze table
      │
silver_transformation   Validate → Silver (clean) + Quarantine (rejected)
      │                 Invalid rows: location_region='0', amount≤0, etc.
gold_aggregation        Aggregate → 5 Gold tables:
      │                   fraud_by_region, fraud_by_type,
      │                   daily_fraud_metrics, high_risk_transactions,
      │                   risk_profile
      │
dbt_quality_tests       Run schema + value tests on Silver via Dremio
```

## Iceberg Tables

All tables stored in `s3a://lakehouse/` (MinIO) via Nessie catalog:

```
nessie.bronze.fraud_transactions        Raw ingestion
nessie.silver.fraud_transactions        Validated + enriched
nessie.quarantine.fraud_transactions    Invalid records + rejection reason
nessie.gold.fraud_by_region            Aggregated by region × anomaly
nessie.gold.fraud_by_type              Aggregated by transaction type
nessie.gold.daily_fraud_metrics        Daily time-series metrics
nessie.gold.high_risk_transactions     risk_score≥75 OR anomaly=high_risk
nessie.gold.risk_profile               Risk by age_group × purchase_pattern
```

## Dremio Setup

The `dremio-setup` service automatically configures the Nessie source on first start.

If it fails, add it manually in the Dremio UI:
- **Add Source → Nessie**
- Endpoint: `http://nessie:19120/api/v2`
- Auth: None
- Storage: S3-compatible → `http://minio:9000`
- Access key: `admin` / Secret: `password123`
- Root path: `lakehouse`
- Extra properties: `fs.s3a.path.style.access=true`, `dremio.s3.compat=true`

## dbt Quality Tests

Tests run against Nessie Iceberg tables via Dremio:

```bash
make dbt-test    # run tests standalone
```

Tests include: `not_null`, `accepted_values` for all critical Silver columns.

## Useful Commands

```bash
make logs       # tail all container logs
make status     # show container health
make clean      # remove all volumes (resets all data)
make urls       # print all service URLs
```
