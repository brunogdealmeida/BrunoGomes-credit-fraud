"""Integration test fixtures: Nessie, MinIO, and Dremio REST clients."""

import os
import time

import boto3
import pytest
import requests
from botocore.config import Config


# ── connection settings ───────────────────────────────────────────────────────
# Use localhost defaults so tests run from the host machine.
# NESSIE_URI / MINIO_ENDPOINT in .env use Docker hostnames — override here.

NESSIE_URL     = "http://localhost:19120/api/v1"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER",    "admin")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD")
DREMIO_URL     = "http://localhost:9047"
DREMIO_USER    = os.getenv("DREMIO_USERNAME",    "admin")
DREMIO_PASS    = os.getenv("DREMIO_PASSWORD")


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


# ── Dremio REST client ────────────────────────────────────────────────────────

class DremioClient:
    def __init__(self, base_url: str, user: str, password: str):
        self.base = base_url
        r = requests.post(
            f"{base_url}/apiv2/login",
            json={"userName": user, "password": password},
            timeout=15,
        )
        r.raise_for_status()
        token = r.json()["token"]
        self.headers = {
            "Authorization": f"_dremio{token}",
            "Content-Type": "application/json",
        }

    def query(self, sql: str) -> list[dict]:
        """Submit SQL to Dremio, wait for completion, return rows as dicts."""
        r = requests.post(
            f"{self.base}/api/v3/sql",
            headers=self.headers,
            json={"sql": sql, "context": []},
            timeout=30,
        )
        r.raise_for_status()
        job_id = r.json()["id"]

        for _ in range(60):
            status = requests.get(f"{self.base}/api/v3/job/{job_id}", headers=self.headers, timeout=10)
            state = status.json().get("jobState", "")
            if state == "COMPLETED":
                break
            if state in ("FAILED", "CANCELED"):
                raise RuntimeError(f"Dremio job {job_id} {state}: {status.json()}")
            time.sleep(2)
        else:
            raise TimeoutError(f"Dremio job {job_id} did not complete in time")

        results = requests.get(
            f"{self.base}/api/v3/job/{job_id}/results?limit=500",
            headers=self.headers,
            timeout=15,
        )
        results.raise_for_status()
        data = results.json()
        return data.get("rows", [])

    def scalar(self, sql: str):
        rows = self.query(sql)
        if not rows:
            return None
        return next(iter(rows[0].values()))


@pytest.fixture(scope="session")
def dremio():
    try:
        requests.get(f"{DREMIO_URL}", timeout=5).raise_for_status()
    except Exception as e:
        pytest.skip(f"Dremio not reachable at {DREMIO_URL}: {e}")
    try:
        return DremioClient(DREMIO_URL, DREMIO_USER, DREMIO_PASS)
    except Exception as e:
        pytest.skip(f"Dremio authentication failed: {e}")
