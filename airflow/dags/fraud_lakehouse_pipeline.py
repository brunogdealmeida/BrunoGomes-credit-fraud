"""
Fraud Lakehouse ETL Pipeline
────────────────────────────
Bronze (raw CSV) → Silver (validated) + Quarantine (rejects) → Gold (aggregations)
→ dbt quality tests
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── Spark connection ──────────────────────────────────────────────────────────
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]

# config.py must travel with each job so it can be imported on the driver.
# YAML files are NOT included here — they are already on the driver filesystem
# at /opt/airflow/jobs/ via the volume mount, and each job resolves them
# explicitly using Path(__file__).parent.
EXTRA_PY_FILES = "/opt/airflow/jobs/config.py"

# JARs are pre-installed in /opt/spark/jars/ by the Airflow Dockerfile
EXTRA_JARS = ",".join([
    "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
    "/opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.76.6.jar",
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.680.jar",
])

SPARK_CONF = {
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    ),
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": NESSIE_URI,
    "spark.sql.catalog.nessie.ref": "main",
    "spark.sql.catalog.nessie.authentication.type": "NONE",
    "spark.sql.catalog.nessie.warehouse": "s3a://lakehouse/",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    # Driver runs inside the airflow-scheduler container (LocalExecutor)
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
}

SPARK_SUBMIT_KWARGS = dict(
    conn_id="spark_default",
    jars=EXTRA_JARS,
    py_files=EXTRA_PY_FILES,
    conf=SPARK_CONF,
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=2,
    num_executors=1,
    verbose=False,
)

# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}

with DAG(
    dag_id="fraud_lakehouse_pipeline",
    default_args=default_args,
    description="Fraud detection lakehouse: CSV → Bronze → Silver/Quarantine → Gold → dbt",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "fraud", "iceberg", "medallion"],
) as dag:

    bronze = SparkSubmitOperator(
        task_id="bronze_ingestion",
        application="/opt/airflow/jobs/bronze_ingestion.py",
        application_args=["df_fraud_credit.csv"],
        name="bronze_ingestion",
        **SPARK_SUBMIT_KWARGS,
    )

    silver = SparkSubmitOperator(
        task_id="silver_transformation",
        application="/opt/airflow/jobs/silver_transformation.py",
        application_args=["df_fraud_credit.csv"],
        name="silver_transformation",
        **SPARK_SUBMIT_KWARGS,
    )

    gold = SparkSubmitOperator(
        task_id="gold_aggregation",
        application="/opt/airflow/jobs/gold_aggregation.py",
        application_args=["df_fraud_credit.csv"],
        name="gold_aggregation",
        **SPARK_SUBMIT_KWARGS,
    )

    quality_report = SparkSubmitOperator(
        task_id="silver_quality_report",
        application="/opt/airflow/jobs/silver_quality_report.py",
        application_args=["df_fraud_credit.csv"],
        name="silver_quality_report",
        **SPARK_SUBMIT_KWARGS,
    )

    dbt_tests = BashOperator(
        task_id="dbt_quality_tests",
        bash_command="""
            set -e
            cd /opt/dbt
            dbt deps --profiles-dir /opt/dbt
            dbt run --select bronze --profiles-dir /opt/dbt --target dev
            dbt test --profiles-dir /opt/dbt --target dev
            dbt docs generate --profiles-dir /opt/dbt --target dev
        """,
        env={
            "DREMIO_HOST": "dremio",
            "DREMIO_USERNAME": os.environ["DREMIO_USERNAME"],
            "DREMIO_PASSWORD": os.environ["DREMIO_PASSWORD"],
        },
        append_env=True,
    )

    bronze >> silver >> [gold, quality_report] >> dbt_tests
