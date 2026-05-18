"""
Loads YAML configs into typed dataclasses consumed by all Spark jobs.

config.py is pure Python — no PySpark imports.
The execution logic (enrichment, quarantine transforms) lives in the job files.

Usage:
    from config import load_spark_config, load_table_config, get_spark_schema, apply_column_ops
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

_DEFAULT_SPARK_CONFIG = Path(__file__).parent / "spark_config.yml"
_DEFAULT_TABLES_CONFIG = Path(__file__).parent / "tables.yml"

_TYPE_MAP: dict[str, Any] = {
    "string":    StringType(),
    "integer":   IntegerType(),
    "int":       IntegerType(),
    "long":      LongType(),
    "bigint":    LongType(),
    "double":    DoubleType(),
    "float":     FloatType(),
    "boolean":   BooleanType(),
    "bool":      BooleanType(),
    "timestamp": TimestampType(),
    "date":      DateType(),
}


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class SparkConfig:
    nessie_uri: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    warehouse: str
    dataset_dir: str


@dataclass
class ValidationConfig:
    valid_regions: list[str] = field(default_factory=list)
    valid_anomalies: list[str] = field(default_factory=list)
    valid_transaction_types: list[str] = field(default_factory=list)


@dataclass
class ColumnDef:
    name: str
    type: str
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class BucketDef:
    label: str
    max: Optional[float] = None
    otherwise: bool = False


@dataclass
class EnrichmentRule:
    column: str
    transform: str
    source: Optional[str] = None
    type: Optional[str] = None
    source_cast: Optional[str] = None
    values: Optional[list] = None
    operator: Optional[str] = None
    value: Optional[float] = None
    buckets: list[BucketDef] = field(default_factory=list)


@dataclass
class QuarantineCondition:
    type: str
    column: Optional[str] = None
    operator: Optional[str] = None
    value: Optional[float] = None
    values_ref: Optional[str] = None
    min: Optional[float] = None
    max: Optional[float] = None


@dataclass
class QuarantineRule:
    reason: str
    condition: QuarantineCondition


@dataclass
class QuarantineConfig:
    rejection_column: str
    timestamp_column: str
    rules: list[QuarantineRule] = field(default_factory=list)


@dataclass
class SilverConfig:
    enrichment: list[EnrichmentRule] = field(default_factory=list)
    quarantine: Optional[QuarantineConfig] = None


@dataclass
class TableConfig:
    file_key: str
    table: str
    schema: list[ColumnDef] = field(default_factory=list)
    rename_columns: dict[str, str] = field(default_factory=dict)
    partition_by: list[str] = field(default_factory=list)
    sort_order: list[str] = field(default_factory=list)
    drop_columns: list[str] = field(default_factory=list)
    cast_columns: dict[str, str] = field(default_factory=dict)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    silver: SilverConfig = field(default_factory=SilverConfig)


# ── YAML helpers ──────────────────────────────────────────────────────────────

def _load_yaml(config_path: Path) -> dict:
    with open(config_path) as fh:
        return yaml.safe_load(fh)


def _to_list(value: Any) -> list[str]:
    if value is None:
        return []
    return [str(v) for v in value] if isinstance(value, list) else [str(value)]


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_spark_config(config_path: Optional[Path] = None) -> SparkConfig:
    """Load Spark connection settings from spark_config.yml, overridden by env vars."""
    if config_path is None:
        config_path = _DEFAULT_SPARK_CONFIG

    s = _load_yaml(config_path).get("spark", {})
    cfg = SparkConfig(
        nessie_uri=os.getenv("NESSIE_URI", s.get("nessie_uri", "http://nessie:19120/api/v1")),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", s.get("minio_endpoint", "http://minio:9000")),
        minio_access_key=os.getenv("MINIO_ROOT_USER", s.get("minio_access_key")),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", s.get("minio_secret_key")),
        warehouse=s.get("warehouse", "s3a://lakehouse/"),
        dataset_dir=os.getenv("DATASET_DIR", s.get("dataset_dir", "/opt/spark/dataset")),
    )
    logger.info(
        "Spark config loaded  nessie_uri=%s  minio_endpoint=%s  warehouse=%s  dataset_dir=%s",
        cfg.nessie_uri, cfg.minio_endpoint, cfg.warehouse, cfg.dataset_dir,
    )
    return cfg


def load_table_config(file_key: str, config_path: Optional[Path] = None) -> TableConfig:
    """Load full configuration for a source file from tables.yml."""
    if config_path is None:
        config_path = _DEFAULT_TABLES_CONFIG

    logger.debug("Reading config from '%s' for key '%s'", config_path, file_key)
    raw = _load_yaml(config_path)

    files = raw.get("files", {})
    if file_key not in files:
        raise KeyError(f"No config found for '{file_key}' in {config_path}")

    entry = files[file_key]

    columns = [
        ColumnDef(
            name=c["name"], type=c["type"],
            nullable=c.get("nullable", True),
            precision=c.get("precision"), scale=c.get("scale"),
        )
        for c in entry.get("schema", [])
    ]

    val_raw = entry.get("validation", {})
    validation = ValidationConfig(
        valid_regions=val_raw.get("valid_regions", []),
        valid_anomalies=val_raw.get("valid_anomalies", []),
        valid_transaction_types=val_raw.get("valid_transaction_types", []),
    )

    silver = _parse_silver_config(entry.get("silver", {}))

    cfg = TableConfig(
        file_key=file_key,
        table=entry["table"],
        schema=columns,
        rename_columns=entry.get("rename_columns") or {},
        partition_by=_to_list(entry.get("partition_by")),
        sort_order=_to_list(entry.get("sort_order")),
        drop_columns=list(entry.get("drop_columns") or []),
        cast_columns=entry.get("cast_columns") or {},
        validation=validation,
        silver=silver,
    )

    logger.info(
        "Table config loaded  file=%s  table=%s  enrichment_rules=%d  quarantine_rules=%d",
        file_key, cfg.table,
        len(cfg.silver.enrichment),
        len(cfg.silver.quarantine.rules) if cfg.silver.quarantine else 0,
    )
    return cfg


def _parse_silver_config(raw: dict) -> SilverConfig:
    enrichment = [_parse_enrichment_rule(r) for r in raw.get("enrichment", [])]

    quarantine = None
    q_raw = raw.get("quarantine")
    if q_raw:
        rules = [
            QuarantineRule(
                reason=r["reason"],
                condition=QuarantineCondition(**r["condition"]),
            )
            for r in q_raw.get("rules", [])
        ]
        quarantine = QuarantineConfig(
            rejection_column=q_raw["rejection_column"],
            timestamp_column=q_raw["timestamp_column"],
            rules=rules,
        )

    return SilverConfig(enrichment=enrichment, quarantine=quarantine)


def _parse_enrichment_rule(raw: dict) -> EnrichmentRule:
    buckets = [
        BucketDef(
            label=b["label"],
            max=b.get("max"),
            otherwise=b.get("otherwise", False),
        )
        for b in raw.get("buckets", [])
    ]
    return EnrichmentRule(
        column=raw["column"],
        transform=raw["transform"],
        source=raw.get("source"),
        type=raw.get("type"),
        source_cast=raw.get("source_cast"),
        values=raw.get("values"),
        operator=raw.get("operator"),
        value=raw.get("value"),
        buckets=buckets,
    )


# ── Schema builder ────────────────────────────────────────────────────────────

def get_spark_schema(cfg: TableConfig) -> StructType:
    """Build a PySpark StructType from a TableConfig's schema definition."""
    fields: list[StructField] = []
    for col_def in cfg.schema:
        col_type = col_def.type.lower()
        if col_type == "decimal":
            spark_type = DecimalType(col_def.precision or 18, col_def.scale or 2)
        else:
            spark_type = _TYPE_MAP.get(col_type)
            if spark_type is None:
                logger.warning(
                    "Unknown type '%s' for column '%s' — falling back to StringType",
                    col_def.type, col_def.name,
                )
                spark_type = StringType()
        fields.append(StructField(col_def.name, spark_type, nullable=col_def.nullable))
    logger.debug("Built Spark schema with %d fields", len(fields))
    return StructType(fields)


# ── Bronze column operations ──────────────────────────────────────────────────

def apply_column_ops(df: Any, cfg: TableConfig) -> Any:
    """Apply rename → drop → cast operations in order, as defined in the config."""
    for old_name, new_name in cfg.rename_columns.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            logger.info("Renamed  '%s' → '%s'", old_name, new_name)
        else:
            logger.warning("Rename skipped — column '%s' not found in DataFrame", old_name)

    cols_to_drop = [c for c in cfg.drop_columns if c in df.columns]
    missing_drops = [c for c in cfg.drop_columns if c not in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)
        logger.info("Dropped columns: %s", cols_to_drop)
    if missing_drops:
        logger.warning("Drop skipped — columns not found: %s", missing_drops)

    for col_name, target_type in cfg.cast_columns.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast(target_type))
            logger.info("Cast  '%s' → %s", col_name, target_type)
        else:
            logger.warning("Cast skipped — column '%s' not found in DataFrame", col_name)

    return df
