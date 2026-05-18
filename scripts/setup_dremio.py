"""
Configure Dremio after first start:
  1. Bootstrap the first admin user (fresh instance only)
  2. Authenticate
  3. Add the Nessie + MinIO lakehouse source
"""

import os
import sys
import time

import requests

DREMIO_HOST = os.getenv("DREMIO_HOST", "dremio")
DREMIO_PORT = os.getenv("DREMIO_PORT", "9047")
BASE_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}"

USERNAME = os.environ["DREMIO_USERNAME"]
PASSWORD = os.environ["DREMIO_PASSWORD"]
NESSIE_URI = os.getenv("NESSIE_URI_V2", os.getenv("NESSIE_URI", "http://nessie:19120/api/v2"))
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]


def wait_for_dremio(retries: int = 30, delay: int = 10) -> None:
    for i in range(retries):
        try:
            r = requests.get(BASE_URL, timeout=5)
            if r.status_code < 500:
                print("[dremio-setup] Dremio HTTP endpoint is responding")
                # Extra stabilisation wait — the API may still be initialising
                time.sleep(30)
                return
        except Exception:
            pass
        print(f"[dremio-setup] Waiting for Dremio… ({i + 1}/{retries})")
        time.sleep(delay)
    sys.exit("[dremio-setup] Dremio did not start in time")


def bootstrap_first_user() -> bool:
    """
    Attempt to set up the initial admin account.
    Returns True if a new account was created, False if already exists.
    """
    payload = {
        "userName": USERNAME,
        "firstName": "Data",
        "lastName": "Admin",
        "email": "admin@lakehouse.local",
        "password": PASSWORD,
    }
    r = requests.put(f"{BASE_URL}/apiv2/bootstrap/firstlogin", json=payload, timeout=15)
    if r.status_code == 200:
        print("[dremio-setup] Admin user created via bootstrap")
        return True
    elif r.status_code == 404:
        print("[dremio-setup] Bootstrap endpoint not found — Dremio may already be initialised")
        return False
    elif r.status_code in (400, 409):
        print(f"[dremio-setup] Bootstrap skipped ({r.status_code}) — likely already done")
        return False
    else:
        print(f"[dremio-setup] Bootstrap returned {r.status_code}: {r.text[:200]}")
        return False


def get_token() -> str | None:
    """Authenticate and return a Dremio API token, or None on failure."""
    r = requests.post(
        f"{BASE_URL}/apiv2/login",
        json={"userName": USERNAME, "password": PASSWORD},
        timeout=15,
    )
    if r.ok:
        token = r.json().get("token", "")
        print(f"[dremio-setup] Authenticated as '{USERNAME}'")
        return token
    print(f"[dremio-setup] Login failed ({r.status_code}): {r.text[:200]}")
    return None


def source_exists(session: requests.Session, name: str) -> bool:
    r = session.get(f"{BASE_URL}/api/v3/catalog/by-path/{name}", timeout=10)
    return r.status_code == 200


def create_nessie_source(session: requests.Session) -> None:
    if source_exists(session, "nessie_lakehouse"):
        print("[dremio-setup] Source 'nessie_lakehouse' already exists — skipping")
        return

    payload = {
        "entityType": "source",
        "name": "nessie_lakehouse",
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": NESSIE_URI,
            "nessieAuthType": "NONE",
            "awsAccessKey": MINIO_ACCESS_KEY,
            "awsAccessSecret": MINIO_SECRET_KEY,
            "awsRootPath": "lakehouse",
            "credentialType": "ACCESS_KEY",
            "propertyList": [
                {"name": "fs.s3a.endpoint",               "value": "minio:9000"},
                {"name": "fs.s3a.path.style.access",      "value": "true"},
                {"name": "dremio.s3.compat",              "value": "true"},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
                {
                    "name":  "fs.s3a.aws.credentials.provider",
                    "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                },
            ],
        },
    }

    r = session.post(f"{BASE_URL}/api/v3/catalog", json=payload, timeout=30)
    if r.status_code in (200, 201):
        print("[dremio-setup] Source 'nessie_lakehouse' created successfully")
    else:
        print(
            f"[dremio-setup] WARNING: Could not create Nessie source "
            f"({r.status_code}): {r.text[:400]}"
        )
        _print_manual_instructions()


def _print_manual_instructions() -> None:
    print(
        f"\n  ─── Manual Dremio Setup ───────────────────────────────────────────\n"
        f"  Open Dremio at {BASE_URL}\n"
        f"  Username: {USERNAME}  |  Password: {PASSWORD}\n"
        f"\n"
        f"  Add a new source:\n"
        f"    Type:      Nessie\n"
        f"    Name:      nessie_lakehouse\n"
        f"    Endpoint:  {NESSIE_URI}\n"
        f"    Auth:      None\n"
        f"\n"
        f"  Storage settings:\n"
        f"    Provider:  S3-compatible\n"
        f"    Access key: {MINIO_ACCESS_KEY}\n"
        f"    Secret key: {MINIO_SECRET_KEY}\n"
        f"    Endpoint:  http://minio:9000\n"
        f"    Root path: lakehouse\n"
        f"    Connection properties:\n"
        f"      fs.s3a.path.style.access = true\n"
        f"      dremio.s3.compat         = true\n"
        f"  ────────────────────────────────────────────────────────────────────\n"
    )


def main() -> None:
    wait_for_dremio()

    bootstrap_first_user()

    token = get_token()
    if not token:
        print(
            "[dremio-setup] Could not authenticate. "
            "If this is a fresh instance, open Dremio in your browser to complete first-time setup, "
            f"then restart this container:\n  docker compose restart dremio-setup"
        )
        _print_manual_instructions()
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"_dremio{token}",
        "Content-Type": "application/json",
    })

    create_nessie_source(session)

    print(f"\n[dremio-setup] Done — Dremio UI: {BASE_URL}")
    print(f"  Username: {USERNAME}  |  Password: {PASSWORD}")


if __name__ == "__main__":
    main()
