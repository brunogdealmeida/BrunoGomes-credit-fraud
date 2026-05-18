"""Root conftest: path setup and shared fixtures available to all tests."""

import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Make spark/jobs importable without installation
JOBS_DIR = Path(__file__).parent.parent / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

# Load .env so integration tests can read credentials
load_dotenv(Path(__file__).parent.parent / ".env")


@pytest.fixture(scope="session")
def jobs_dir() -> Path:
    return JOBS_DIR


@pytest.fixture(scope="session")
def tables_yml(jobs_dir: Path) -> Path:
    return jobs_dir / "tables.yml"


@pytest.fixture(scope="session")
def spark_config_yml(jobs_dir: Path) -> Path:
    return jobs_dir / "spark_config.yml"


@pytest.fixture(scope="session")
def fraud_file_key() -> str:
    return "df_fraud_credit.csv"
