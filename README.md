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
           Apache Superset                  Dashboard / BI
           Fraud Detection Analytics
              │
              ▼
           OpenMetadata                     Data Catalog / Governance
           MinIO + Airflow lineage
              │
              ▼
       Prometheus + Grafana                 Monitoring / Observability
       Container & Postgres metrics
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

| Service           | URL                           | Credentials               |
|-------------------|-------------------------------|---------------------------|
| Airflow UI        | http://localhost:8082         | admin / admin123          |
| Dremio UI         | http://localhost:9047         | see `.env`                |
| MinIO Console     | http://localhost:9001         | see `.env`                |
| Superset UI       | http://localhost:8088         | admin / admin123          |
| OpenMetadata UI   | http://localhost:8585         | admin@openmetadata.org / admin |
| Grafana           | http://localhost:3001         | admin / admin             |
| Prometheus        | http://localhost:9090         | —                         |
| Nessie API        | http://localhost:19120/api/v1 | —                         |
| Spark Master UI   | http://localhost:8080         | —                         |

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24 with **≥ 12 GB RAM allocated** (Settings → Resources → Memory)
- Docker Compose v2
- ≥ 30 GB free disk space (Spark work artifacts accumulate — see Operational Notes)

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
dbt_quality_tests       53 tests against Spark-produced Iceberg tables via Dremio
                        Silver: not_null + accepted_values on all 8 columns
                        Gold: not_null + accepted_values + range checks on all 5 tables
                        dbt materializes nothing — pure quality gate
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

dbt is a **pure quality gate** — it materializes no models. All 53 tests run directly against the Spark-produced Iceberg tables (`nessie_lakehouse.silver.*` and `nessie_lakehouse.gold.*`) via Dremio as the SQL engine.

```bash
make dbt-test    # run tests standalone
```

Test coverage: `not_null` and `accepted_values` on Silver; `not_null`, `accepted_values`, and `dbt_utils.accepted_range` on all 5 Gold tables.

## Monitoring

- **Grafana** (http://localhost:3001) — dashboards for container and Postgres metrics
- **Prometheus** (http://localhost:9090) — metrics collection from cAdvisor and postgres-exporter
- **cAdvisor** — container resource metrics (CPU, memory, disk per container)

## Data Catalog

- **OpenMetadata** (http://localhost:8585) — automated ingestion of MinIO storage metadata and Airflow pipeline lineage via the `openmetadata-setup` init container.

## Useful Commands

```bash
make logs       # tail all container logs
make status     # show container health
make clean      # remove all volumes (resets all data)
make urls       # print all service URLs
```

## Operational Notes

### Spark work directory disk usage

Spark writes job artifacts to `/opt/spark/work` inside the `spark-worker` container. Each pipeline run accumulates ~400 MB; after ~60 runs this can consume 25+ GB and cause "No space left on device" errors.

Clean up when needed:

```bash
docker exec spark-worker sh -c "cd /opt/spark/work && for d in app-2*; do rm -rf \"\$d\"; done"
```

### Dremio OOM under memory pressure

Dremio is configured with `-Xmx1500m`. During active Spark jobs the Docker VM can run low on memory and OOMKill Dremio. The pipeline design mitigates this: Spark jobs complete first and Dremio is only needed for the `dbt_quality_tests` step at the end.

If Dremio is killed, restart it: `docker compose up -d dremio`

### Stale Nessie catalog after direct MinIO deletion

Deleting files directly from MinIO (bypassing Spark/Iceberg) leaves orphaned metadata in Nessie, causing `NotFoundException` on the next pipeline run. Always drop tables via Spark SQL or re-run ingestion with `overwrite` mode rather than deleting MinIO folders manually.
