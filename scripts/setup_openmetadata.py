"""
Bootstrap OpenMetadata: obtain an admin JWT token then run metadata
ingestion for Dremio (tables), Airflow (pipelines), and MinIO (storage).

Runs inside the openmetadata/ingestion container — all env-vars are
injected by docker-compose from the job-level environment block.
"""

import base64
import os
import re
import subprocess
import sys
import tempfile
import time

import requests

# ── connection settings ───────────────────────────────────────────────────────

OM_HOST  = os.getenv("OM_HOST",  "openmetadata-server")
OM_PORT  = os.getenv("OM_PORT",  "8585")
BASE_URL = f"http://{OM_HOST}:{OM_PORT}/api"

ADMIN_EMAIL    = os.getenv("OM_ADMIN_EMAIL",    "admin@openmetadata.org")
ADMIN_PASSWORD = os.getenv("OM_ADMIN_PASSWORD", "admin")

INGESTION_DIR = "/openmetadata/ingestion"


# ── helpers ───────────────────────────────────────────────────────────────────

def wait_for_openmetadata(retries: int = 40, delay: int = 15) -> None:
    for i in range(retries):
        try:
            r = requests.get(f"{BASE_URL}/v1/system/status", timeout=5)
            # 200 = healthy (no auth); 401 = server up but requires auth
            if r.status_code in (200, 401):
                print("[om-setup] OpenMetadata is ready")
                return
        except Exception:
            pass
        print(f"[om-setup] Waiting for OpenMetadata… ({i + 1}/{retries})")
        time.sleep(delay)
    sys.exit("[om-setup] OpenMetadata did not start in time")


def get_jwt_token() -> str:
    password_b64 = base64.b64encode(ADMIN_PASSWORD.encode()).decode()
    r = requests.post(
        f"{BASE_URL}/v1/users/login",
        json={"email": ADMIN_EMAIL, "password": password_b64},
        timeout=15,
    )
    r.raise_for_status()
    token = r.json().get("accessToken", "")
    if not token:
        sys.exit("[om-setup] Login succeeded but no accessToken in response")
    print(f"[om-setup] Authenticated as '{ADMIN_EMAIL}'")
    return token


def render_config(template_path: str, env: dict) -> str:
    """Substitute ${VAR} placeholders with values from env."""
    with open(template_path) as f:
        content = f.read()
    for key, value in env.items():
        content = content.replace(f"${{{key}}}", value)
    # Warn about any remaining unresolved placeholders
    remaining = re.findall(r"\$\{[^}]+\}", content)
    if remaining:
        print(f"[om-setup] WARNING: unresolved placeholders in {template_path}: {remaining}")
    return content


def run_ingestion(name: str, config_content: str) -> bool:
    """Write config to a temp file and run `metadata ingest`."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix=f"om_{name}_", delete=False
    ) as f:
        f.write(config_content)
        tmp_path = f.name

    print(f"\n[om-setup] ── Running {name} ingestion ──────────────────────")
    result = subprocess.run(
        ["metadata", "ingest", "-c", tmp_path],
        capture_output=False,
    )
    os.unlink(tmp_path)

    if result.returncode == 0:
        print(f"[om-setup] {name} ingestion completed successfully")
        return True
    else:
        print(f"[om-setup] WARNING: {name} ingestion finished with errors (exit {result.returncode})")
        return False


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    wait_for_openmetadata()
    token = get_jwt_token()

    # Build substitution map from all relevant env-vars + the live token
    env = {
        "OM_JWT_TOKEN":      token,
        "MINIO_ROOT_USER":   os.getenv("MINIO_ROOT_USER",   "admin"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", ""),
        "POSTGRES_USER":     os.getenv("POSTGRES_USER",     "airflow"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
        "POSTGRES_DB":       os.getenv("POSTGRES_DB",       "airflow"),
    }

    # Dremio connector is not available in OpenMetadata 1.5.0; skip it.
    results = {}
    for source in ("minio", "airflow"):
        template = os.path.join(INGESTION_DIR, f"{source}.yaml")
        if not os.path.exists(template):
            print(f"[om-setup] Config not found, skipping: {template}")
            continue
        config = render_config(template, env)
        results[source] = run_ingestion(source, config)

    print("\n[om-setup] ── Summary ───────────────────────────────────────────")
    for source, ok in results.items():
        status = "OK" if ok else "FAILED (check logs)"
        print(f"  {source:<10} {status}")
    print(f"\n[om-setup] OpenMetadata UI: http://localhost:8585")
    print(f"  Login: {ADMIN_EMAIL}  |  Password: {ADMIN_PASSWORD}")


if __name__ == "__main__":
    main()
