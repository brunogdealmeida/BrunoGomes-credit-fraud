import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]

# Allow Dremio and other custom DB connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,
    "EMBEDDED_SUPERSET": True,
}

# Extra database drivers allowed
ADDITIONAL_MIDDLEWARE = []

# CORS (allow iframe embedding in demos)
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"],
}

# Talisman / CSP settings (relaxed for local dev)
TALISMAN_ENABLED = False

WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log"]

# Increase query row limit for the fraud dataset
ROW_LIMIT = 500_000
SQL_MAX_ROW = 500_000
