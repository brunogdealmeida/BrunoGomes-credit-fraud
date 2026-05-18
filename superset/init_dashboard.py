"""
Create the Fraud Analytics dashboard in Superset via REST API.

Runs once after Superset and Dremio are healthy (see docker-compose superset-init service).
"""

import json
import os
import sys
import time

import requests

SUPERSET_HOST = os.getenv("SUPERSET_HOST", "superset")
SUPERSET_PORT = os.getenv("SUPERSET_PORT", "8088")
SUPERSET_URL = f"http://{SUPERSET_HOST}:{SUPERSET_PORT}"

SUPERSET_USER = os.environ["SUPERSET_ADMIN_USERNAME"]
SUPERSET_PASS = os.environ["SUPERSET_ADMIN_PASSWORD"]

DREMIO_HOST = os.getenv("DREMIO_HOST", "dremio")
DREMIO_PORT = os.getenv("DREMIO_PORT", "9047")
DREMIO_USER = os.environ["DREMIO_USERNAME"]
DREMIO_PASS = os.environ["DREMIO_PASSWORD"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def wait_for_superset(retries: int = 20, delay: int = 10) -> None:
    for i in range(retries):
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.ok:
                print("[init] Superset is ready")
                return
        except Exception:
            pass
        print(f"[init] Waiting for Superset… ({i+1}/{retries})")
        time.sleep(delay)
    sys.exit("Superset did not become healthy in time")


def get_csrf_and_session(session: requests.Session) -> str:
    r = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=10)
    r.raise_for_status()
    return r.json()["result"]


def login(session: requests.Session) -> None:
    payload = {
        "username": SUPERSET_USER,
        "password": SUPERSET_PASS,
        "provider": "db",
        "refresh": True,
    }
    r = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=payload, timeout=10)
    r.raise_for_status()
    token = r.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    csrf = get_csrf_and_session(session)
    session.headers.update({"X-CSRFToken": csrf, "Referer": SUPERSET_URL})
    print("[init] Logged in to Superset")


def post(session: requests.Session, path: str, payload: dict) -> dict:
    r = session.post(f"{SUPERSET_URL}{path}", json=payload, timeout=30)
    if not r.ok:
        print(f"  WARN {path} → {r.status_code}: {r.text[:200]}")
        return {}
    return r.json()


def get_or_create(session: requests.Session, list_path: str, create_path: str,
                  payload: dict, key: str = "result") -> int | None:
    r = session.get(f"{SUPERSET_URL}{list_path}", timeout=10)
    data = r.json().get(key, []) if r.ok else []
    name = payload.get("database_name") or payload.get("table_name") or payload.get("slice_name") or payload.get("dashboard_title", "")
    for item in data:
        if item.get("database_name") == name or item.get("table_name") == name \
                or item.get("slice_name") == name or item.get("dashboard_title") == name:
            print(f"  EXISTS  {name}")
            return item["id"]
    result = post(session, create_path, payload)
    created = result.get("id") or (result.get(key, {}) or {}).get("id")
    print(f"  CREATED {name}  id={created}")
    return created


# ── Database connection ───────────────────────────────────────────────────────

def create_dremio_database(session: requests.Session) -> int:
    uri = (
        f"dremio+connector://{DREMIO_USER}:{DREMIO_PASS}"
        f"@{DREMIO_HOST}:{DREMIO_PORT}/"
    )
    payload = {
        "database_name": "Dremio - Fraud Lakehouse",
        "sqlalchemy_uri": uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "extra": json.dumps({"metadata_params": {}, "engine_params": {}}),
    }
    db_id = get_or_create(
        session,
        "/api/v1/database/?q=(page:0,page_size:100)",
        "/api/v1/database/",
        payload,
    )
    return db_id


# ── Datasets ──────────────────────────────────────────────────────────────────

GOLD_TABLES = [
    ("nessie_lakehouse", "gold", "fraud_by_region"),
    ("nessie_lakehouse", "gold", "fraud_by_type"),
    ("nessie_lakehouse", "gold", "daily_fraud_metrics"),
    ("nessie_lakehouse", "gold", "high_risk_transactions"),
    ("nessie_lakehouse", "gold", "risk_profile"),
]


def create_datasets(session: requests.Session, db_id: int) -> dict[str, int]:
    ids: dict[str, int] = {}
    for catalog, schema, table in GOLD_TABLES:
        payload = {
            "database": db_id,
            "table_name": table,
            "schema": f"{catalog}.{schema}",
        }
        ds_id = get_or_create(
            session,
            f"/api/v1/dataset/?q=(page:0,page_size:200)",
            "/api/v1/dataset/",
            payload,
        )
        if ds_id:
            ids[table] = ds_id
    return ids


# ── Charts ────────────────────────────────────────────────────────────────────

def create_charts(session: requests.Session, datasets: dict[str, int]) -> list[int]:
    chart_ids: list[int] = []

    def chart(name, viz, ds_key, params):
        ds_id = datasets.get(ds_key)
        if not ds_id:
            return
        payload = {
            "slice_name": name,
            "viz_type": viz,
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        cid = get_or_create(
            session,
            "/api/v1/chart/?q=(page:0,page_size:200)",
            "/api/v1/chart/",
            payload,
        )
        if cid:
            chart_ids.append(cid)

    # 1 – Transaction count by region (bar)
    chart(
        "Transactions by Region", "echarts_bar", "fraud_by_region",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "transaction_count"}, "aggregate": "SUM", "label": "Transactions"}],
            "groupby": ["location_region"],
            "color_scheme": "supersetColors",
            "rich_tooltip": True,
            "show_legend": True,
        },
    )

    # 2 – Anomaly distribution (pie)
    chart(
        "Anomaly Distribution", "pie", "fraud_by_region",
        {
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "transaction_count"}, "aggregate": "SUM", "label": "Count"},
            "groupby": ["anomaly"],
            "color_scheme": "supersetColors",
            "show_labels": True,
            "show_legend": True,
        },
    )

    # 3 – Average risk score by region (bar)
    chart(
        "Avg Risk Score by Region", "echarts_bar", "fraud_by_region",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "avg_risk_score"}, "aggregate": "AVG", "label": "Avg Risk Score"}],
            "groupby": ["location_region"],
            "color_scheme": "supersetColors",
        },
    )

    # 4 – Total amount by transaction type (bar)
    chart(
        "Total Amount by Transaction Type", "echarts_bar", "fraud_by_type",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_amount"}, "aggregate": "SUM", "label": "Total Amount"}],
            "groupby": ["transaction_type"],
            "color_scheme": "supersetColors",
        },
    )

    # 5 – Fraudulent vs non-fraudulent transactions (bar stacked)
    chart(
        "Fraud Count by Transaction Type", "echarts_bar", "fraud_by_type",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "transaction_count"}, "aggregate": "SUM", "label": "Count"}],
            "groupby": ["transaction_type"],
            "columns": ["anomaly"],
            "color_scheme": "supersetColors",
            "bar_stacked": True,
        },
    )

    # 6 – Daily transaction volume (line)
    chart(
        "Daily Transaction Volume", "echarts_timeseries_line", "daily_fraud_metrics",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_transactions"}, "aggregate": "SUM", "label": "Total Transactions"}],
            "groupby": ["location_region"],
            "x_axis": "transaction_date",
            "color_scheme": "supersetColors",
            "rich_tooltip": True,
        },
    )

    # 7 – High-risk transaction count trend (line)
    chart(
        "High-Risk Transactions Over Time", "echarts_timeseries_line", "daily_fraud_metrics",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "high_risk_count"}, "aggregate": "SUM", "label": "High Risk"}],
            "groupby": [],
            "x_axis": "transaction_date",
            "color_scheme": "supersetColors",
        },
    )

    # 8 – Risk score heatmap (region × anomaly)
    chart(
        "Risk Score Heatmap (Region × Anomaly)", "heatmap", "fraud_by_region",
        {
            "all_columns_x": "location_region",
            "all_columns_y": "anomaly",
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "avg_risk_score"}, "aggregate": "AVG", "label": "Avg Risk Score"},
            "normalize_across": "heatmap",
            "canvas_image_rendering": "auto",
        },
    )

    # 9 – High-risk transactions table
    chart(
        "High-Risk Transactions Detail", "table", "high_risk_transactions",
        {
            "all_columns": [
                "transaction_datetime", "location_region", "transaction_type",
                "amount", "risk_score", "anomaly", "age_group", "purchase_pattern",
                "is_fraudulent",
            ],
            "order_desc": True,
            "page_length": 25,
            "include_search": True,
        },
    )

    # 10 – Avg risk score by user segment (grouped bar)
    chart(
        "Risk Score by User Segment", "echarts_bar", "risk_profile",
        {
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "avg_risk_score"}, "aggregate": "AVG", "label": "Avg Risk Score"}],
            "groupby": ["age_group"],
            "columns": ["purchase_pattern"],
            "color_scheme": "supersetColors",
        },
    )

    return chart_ids


# ── Dashboard ─────────────────────────────────────────────────────────────────

def create_dashboard(session: requests.Session, chart_ids: list[int]) -> None:
    position = {}
    for i, cid in enumerate(chart_ids):
        col = (i % 3) * 4
        row = (i // 3) * 8
        key = f"CHART-{cid}"
        position[key] = {
            "type": "CHART",
            "id": key,
            "children": [],
            "meta": {"chartId": cid, "width": 4, "height": 8},
            "parents": ["ROOT_ID", "GRID_ID"],
        }

    payload = {
        "dashboard_title": "Fraud Detection Analytics",
        "slug": "fraud-analytics",
        "position_json": json.dumps(position),
        "published": True,
        "css": "",
        "json_metadata": json.dumps({"color_scheme": "supersetColors", "refresh_frequency": 0}),
    }
    get_or_create(
        session,
        "/api/v1/dashboard/?q=(page:0,page_size:100)",
        "/api/v1/dashboard/",
        payload,
        key="result",
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    wait_for_superset()

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    login(session)

    print("\n[init] Creating Dremio database connection…")
    db_id = create_dremio_database(session)
    if not db_id:
        sys.exit("Failed to create Dremio database connection")

    print("\n[init] Creating datasets…")
    datasets = create_datasets(session, db_id)

    print("\n[init] Creating charts…")
    chart_ids = create_charts(session, datasets)

    print("\n[init] Creating dashboard…")
    create_dashboard(session, chart_ids)

    print(f"\n[init] Done — dashboard available at {SUPERSET_URL}/dashboard/fraud-analytics/")


if __name__ == "__main__":
    main()
