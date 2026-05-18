"""Integration test fixtures: Nessie, MinIO, and PyIceberg catalog."""

import os

import boto3
import pytest
import requests
from botocore.config import Config
from pyiceberg.catalog.rest import RestCatalog


# ── connection settings ───────────────────────────────────────────────────────

NESSIE_URL        = "http://localhost:19120/api/v1"
NESSIE_ICEBERG_URL = "http://localhost:19120/iceberg/v1"
MINIO_ENDPOINT    = "http://localhost:9000"
MINIO_ACCESS      = os.getenv("MINIO_ROOT_USER",     "admin")
MINIO_SECRET      = os.getenv("MINIO_ROOT_PASSWORD")

TABLE = "tb_fraud_credit"


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires running Docker stack")


# ── Nessie client ─────────────────────────────────────────────────────────────

class NessieClient:
    def __init__(self, base_url: str):
        self.base = base_url

    def namespace_exists(self, namespace: str) -> bool:
        r = requests.get(f"{self.base}/trees/tree/main/entries", timeout=10)
        if r.status_code != 200:
            return False
        for entry in r.json().get("entries", []):
            elements = entry.get("name", {}).get("elements", [])
            if len(elements) == 1 and elements[0] == namespace:
                return True
        return False

    def table_exists(self, namespace: str, table: str) -> bool:
        key = f"{namespace}.{table}"
        r = requests.get(
            f"{self.base}/trees/tree/main/entries",
            params={"filter": f"entry.encodedKey=='{key}'"},
            timeout=10,
        )
        if r.status_code != 200:
            return False
        return len(r.json().get("entries", [])) > 0


@pytest.fixture(scope="session")
def nessie():
    client = NessieClient(NESSIE_URL)
    try:
        requests.get(f"{NESSIE_URL}/config", timeout=5).raise_for_status()
    except Exception as e:
        pytest.skip(f"Nessie not reachable at {NESSIE_URL}: {e}")
    return client


# ── MinIO / S3 client ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def minio():
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        client.list_buckets()
    except Exception as e:
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {e}")
    return client


# ── PyIceberg catalog (Nessie Iceberg REST) ───────────────────────────────────

@pytest.fixture(scope="session")
def catalog():
    cat = RestCatalog(
        name="nessie",
        uri=NESSIE_ICEBERG_URL,
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS,
            "s3.secret-access-key": MINIO_SECRET,
            "s3.path-style-access": "true",
        },
    )
    try:
        cat.list_namespaces()
    except Exception as e:
        pytest.skip(f"Iceberg catalog not reachable at {NESSIE_ICEBERG_URL}: {e}")
    return cat


# ── Pre-loaded DataFrames (session-scoped to avoid repeated full scans) ───────

@pytest.fixture(scope="session")
def silver_df(catalog):
    return catalog.load_table(("silver", TABLE)).scan().to_pandas()


@pytest.fixture(scope="session")
def quarantine_df(catalog):
    return catalog.load_table(("quarantine", TABLE)).scan().to_pandas()
