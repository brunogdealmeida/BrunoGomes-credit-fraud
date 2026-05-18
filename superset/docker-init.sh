#!/bin/sh
set -e

echo "[Superset] Upgrading metadata database..."
superset db upgrade

echo "[Superset] Creating admin user..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname Admin \
    --lastname User \
    --email "${SUPERSET_ADMIN_EMAIL}" \
    --password "${SUPERSET_ADMIN_PASSWORD}" 2>/dev/null || true

echo "[Superset] Initializing roles and permissions..."
superset init

echo "[Superset] Starting server..."
exec gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 2 \
    --worker-class gthread \
    --threads 20 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
