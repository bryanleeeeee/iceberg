#!/usr/bin/env python3
"""Controlled Hive/HDFS to Apache Iceberg migration utility for Cloudera CDP.

Commands:
  preflight  Validate cluster-level prerequisites and write evidence JSON.
  inventory  Build an editable CSV migration plan.
  run        Create and validate a sandbox copy, or execute approved cutovers.
  validate   Compare schema, row count, dual checksums and optional partitions.
  rollback   Restore one migrated/CTAS table after explicit confirmation.
  optimize   Compact approved Iceberg tables as a separate maintenance step.
  status     Summarize the append-only state journal.

This utility deliberately does not freeze upstream writers, change Ranger
policies, run Impala/beeline, or delete retained backups. Those are operational
change controls and remain explicit runbook steps.

Use the Iceberg libraries bundled and supported by the target CDP runtime. Do
not add a generic upstream Iceberg runtime JAR unless Cloudera support confirms
the exact Spark/Scala/Iceberg combination.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, functions as F


LOG = logging.getLogger("hdfs-to-iceberg")
BACKUP_SUFFIX = "_BACKUP_"
CTAS_SUFFIX = "__ice"
LEGACY_SUFFIX = "__legacy"
SMALL_FILE_MB = 64
TARGET_FILE_BYTES = 512 * 1024 * 1024

ICEBERG_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.distribution-mode": "hash",
}

PLAN_FIELDS = [
    "table", "source_type", "strategy", "approved", "writer_freeze_ticket",
    "format", "schema_compatible", "cast_columns", "partitions",
    "target_partition_spec", "location", "target_location", "size_gb",
    "files", "avg_file_mb", "compact", "target_format_version", "note",
]

RESULT_FIELDS = [
    "run_id", "table", "strategy", "mode", "status", "source_rows",
    "target_rows", "seconds", "state", "note",
]

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PARTITION_EXPR = re.compile(
    r"^(?:[A-Za-z_][A-Za-z0-9_]*|(?:years|months|days|hours)\([A-Za-z_][A-Za-z0-9_]*\)|"
    r"bucket\([1-9][0-9]*,[A-Za-z_][A-Za-z0-9_]*\)|"
    r"truncate\([1-9][0-9]*,[A-Za-z_][A-Za-z0-9_]*\))$",
    re.IGNORECASE,
)


@dataclass
class ValidationResult:
    ok: bool
    source_rows: Optional[int]
    target_rows: Optional[int]
    schema_ok: bool
    global_hash_ok: bool
    partition_ok: Optional[bool]
    details: List[str]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def new_run_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def get_spark(app_name: str = "hdfs-to-iceberg") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def quote_ident(value: str) -> str:
    if not IDENT.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return f"`{value}`"


def parse_table(value: str) -> Tuple[str, str]:
    pieces = value.split(".")
    if len(pieces) != 2 or not all(IDENT.fullmatch(x) for x in pieces):
        raise ValueError(f"Expected a safe db.table identifier, got {value!r}")
    return pieces[0], pieces[1]


def qtable(value: str) -> str:
    db, table = parse_table(value)
    return f"{quote_ident(db)}.{quote_ident(table)}"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def properties_map_sql(properties: Dict[str, str]) -> str:
    args: List[str] = []
    for key, value in properties.items():
        args.extend([sql_literal(key), sql_literal(value)])
    return "map(" + ",".join(args) + ")"


def properties_ddl_sql(properties: Dict[str, str]) -> str:
    return ",".join(f"{sql_literal(k)}={sql_literal(v)}" for k, v in properties.items())


def table_exists(spark: SparkSession, fq: str) -> bool:
    db, table = parse_table(fq)
    return spark.catalog.tableExists(f"{db}.{table}")


def append_jsonl(path: str, record: Dict[str, object]) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    payload = dict(record)
    payload.setdefault("timestamp_utc", utc_now())
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def journal(path: str, run_id: str, table: str, state: str, **extra: object) -> None:
    append_jsonl(path, {"run_id": run_id, "table": table, "state": state, **extra})


def describe_table(spark: SparkSession, fq: str) -> Tuple[List[Tuple[str, str]], List[str], Dict[str, str]]:
    columns: List[Tuple[str, str]] = []
    partitions: List[str] = []
    metadata: Dict[str, str] = {}
    section = "columns"
    for row in spark.sql(f"DESCRIBE EXTENDED {qtable(fq)}").collect():
        name = (row[0] or "").strip()
        value = (row[1] or "").strip()
        if name == "# Partition Information":
            section = "partitions"
        elif name == "# Detailed Table Information":
            section = "metadata"
        elif not name or name.startswith("#"):
            continue
        elif section == "columns":
            columns.append((name, value))
        elif section == "partitions":
            partitions.append(name)
        else:
            metadata[name] = value
    return columns, partitions, metadata


def source_format(metadata: Dict[str, str]) -> str:
    provider = metadata.get("Provider", "").lower()
    blob = " ".join(
        [provider, metadata.get("Serde Library", ""), metadata.get("InputFormat", "")]
    ).lower()
    return next((fmt for fmt in ("parquet", "orc", "avro") if fmt in blob), "other")


def source_type(metadata: Dict[str, str]) -> str:
    raw = metadata.get("Type", "").upper()
    if "EXTERNAL" in raw:
        return "EXTERNAL"
    if "MANAGED" in raw:
        return "MANAGED"
    if "VIEW" in raw:
        return "VIEW"
    return raw or "UNKNOWN"


def schema_actions(columns: Sequence[Tuple[str, str]]) -> Tuple[bool, Dict[str, str], List[str]]:
    casts: Dict[str, str] = {}
    blockers: List[str] = []
    for name, dtype in columns:
        lower = dtype.lower().strip()
        if lower in ("tinyint", "smallint"):
            casts[name] = "int"
        elif lower.startswith("char(") or lower.startswith("varchar("):
            casts[name] = "string"
        elif any(token in lower for token in ("uniontype<", "interval ")):
            blockers.append(f"{name}:{dtype}")
        elif any(token in lower for token in ("tinyint", "smallint", "char(", "varchar(")):
            blockers.append(f"nested conversion required for {name}:{dtype}")
    return not blockers, casts, blockers


def classify(
    metadata: Dict[str, str],
    columns: Sequence[Tuple[str, str]],
    allow_avro_migrate: bool,
) -> Tuple[str, str, bool, Dict[str, str], str]:
    fmt = source_format(metadata)
    kind = source_type(metadata)
    props = metadata.get("Table Properties", "").lower()
    provider = metadata.get("Provider", "").lower()
    buckets = metadata.get("Num Buckets", "")
    compatible, casts, blockers = schema_actions(columns)

    if kind == "VIEW":
        return fmt, "skip", compatible, casts, "view"
    if provider == "iceberg" or "table_type=iceberg" in props:
        return fmt, "skip", compatible, casts, "already Iceberg"
    if "transactional=true" in props:
        return fmt, "hive_ctas", compatible, casts, "Hive ACID; execute through approved Hive/HWC procedure"
    if blockers:
        return fmt, "manual", False, casts, "; ".join(blockers)
    if buckets.isdigit() and int(buckets) > 0:
        return fmt, "ctas", compatible, casts, "bucketed Hive layout requires rewrite"
    if kind != "EXTERNAL":
        return fmt, "ctas", compatible, casts, f"{kind} table is not eligible for in-place migrate"
    if casts:
        return fmt, "ctas", compatible, casts, "schema conversion required"
    if fmt == "avro" and not allow_avro_migrate:
        return fmt, "ctas", compatible, casts, "rewrite Avro to Parquet for broad Impala compatibility"
    if fmt not in ("parquet", "orc", "avro"):
        return fmt, "ctas", compatible, casts, "source file format requires rewrite"
    if "translated_to_external=true" in props:
        return fmt, "manual", compatible, casts, "TRANSLATED_TO_EXTERNAL must be reviewed before migrate"
    return fmt, "migrate", compatible, casts, "external non-ACID table eligible for pilot validation"


def directory_stats(spark: SparkSession, location: str) -> Tuple[int, int]:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(location)
    summary = path.getFileSystem(conf).getContentSummary(path)
    return int(summary.getLength()), int(summary.getFileCount())


def fs_preflight(spark: SparkSession, require_ha: bool = True) -> Dict[str, object]:
    conf = spark._jsc.hadoopConfiguration()
    fs_default = conf.get("fs.defaultFS", "")
    nameservices = conf.get("dfs.nameservices", "")
    extensions = spark.conf.get("spark.sql.extensions", "")
    catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")
    checks = {
        "fs_default": fs_default,
        "dfs_nameservices": nameservices,
        "spark_extensions": extensions,
        "spark_catalog": catalog,
        "spark_version": spark.version,
        "hdfs_uri": fs_default.startswith("hdfs://"),
        "logical_nameservice_configured": bool(nameservices.strip()),
        "iceberg_extension_configured": "IcebergSparkSessionExtensions" in extensions,
        "session_catalog_configured": "Iceberg" in catalog,
    }
    errors: List[str] = []
    if not checks["hdfs_uri"]:
        errors.append(f"fs.defaultFS is not HDFS: {fs_default!r}")
    if require_ha and not checks["logical_nameservice_configured"]:
        errors.append("dfs.nameservices is empty; Iceberg metadata may capture a physical NameNode")
    if not checks["iceberg_extension_configured"]:
        errors.append("Iceberg Spark SQL extension is not configured")
    if not checks["session_catalog_configured"]:
        errors.append("spark_catalog is not configured as an Iceberg session catalog")
    checks["errors"] = errors
    checks["ok"] = not errors
    return checks


def cmd_preflight(spark: SparkSession, args: argparse.Namespace) -> int:
    evidence = fs_preflight(spark, require_ha=not args.allow_non_ha)
    evidence["timestamp_utc"] = utc_now()
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(evidence, handle, indent=2, sort_keys=True)
    if evidence["ok"]:
        LOG.info("Cluster preflight passed. Evidence: %s", args.out)
        return 0
    for error in evidence["errors"]:
        LOG.error("Preflight: %s", error)
    return 2


def cmd_inventory(spark: SparkSession, args: argparse.Namespace) -> int:
    rows: List[Dict[str, object]] = []
    for database in [x.strip() for x in args.databases.split(",") if x.strip()]:
        quote_ident(database)
        for table_row in spark.sql(f"SHOW TABLES IN {quote_ident(database)}").collect():
            if table_row["isTemporary"]:
                continue
            fq = f"{database}.{table_row['tableName']}"
            try:
                columns, partitions, metadata = describe_table(spark, fq)
                fmt, strategy, compatible, casts, note = classify(
                    metadata, columns, args.allow_avro_migrate
                )
                location = metadata.get("Location", "")
                size_gb: object = ""
                files: object = ""
                avg_mb: object = ""
                compact = ""
                if args.with_size and location and strategy not in ("skip", "manual"):
                    byte_count, file_count = directory_stats(spark, location)
                    size_gb = round(byte_count / 1024**3, 2)
                    files = file_count
                    avg_mb = round(byte_count / max(file_count, 1) / 1024**2, 1)
                    compact = "Y" if avg_mb < SMALL_FILE_MB else "N"
                kind = source_type(metadata)
                rows.append(
                    {
                        "table": fq,
                        "source_type": kind,
                        "strategy": strategy,
                        "approved": "N",
                        "writer_freeze_ticket": "",
                        "format": fmt,
                        "schema_compatible": "Y" if compatible else "N",
                        "cast_columns": ";".join(f"{k}:{v}" for k, v in casts.items()),
                        "partitions": ";".join(partitions),
                        "target_partition_spec": ";".join(partitions),
                        "location": location,
                        "target_location": "",
                        "size_gb": size_gb,
                        "files": files,
                        "avg_file_mb": avg_mb,
                        "compact": compact,
                        "target_format_version": "2",
                        "note": note,
                    }
                )
                LOG.info("%-55s %-10s %s", fq, strategy, note)
            except Exception as exc:
                LOG.exception("Inventory failed for %s", fq)
                row = {field: "" for field in PLAN_FIELDS}
                row.update(table=fq, strategy="manual", approved="N", note=f"inventory error: {exc}")
                rows.append(row)
    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    LOG.info("Wrote %d plan rows to %s", len(rows), args.out)
    return 0


def parse_casts(value: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in [x for x in value.split(";") if x]:
        if ":" not in item:
            raise ValueError(f"Invalid cast_columns item: {item!r}")
        name, dtype = item.split(":", 1)
        quote_ident(name)
        if dtype.lower() not in ("int", "string"):
            raise ValueError(f"Disallowed automatic cast target: {dtype!r}")
        result[name] = dtype.lower()
    return result


def parse_partition_spec(value: str) -> List[str]:
    expressions = [x.strip() for x in value.split(";") if x.strip()]
    for expression in expressions:
        compact = expression.replace(" ", "")
        if not PARTITION_EXPR.fullmatch(compact):
            raise ValueError(f"Unsafe/unsupported partition expression: {expression!r}")
    return [x.replace(" ", "") for x in expressions]


def ctas_projection(spark: SparkSession, source: str, casts: Dict[str, str]) -> str:
    names = spark.table(source).columns
    unknown = set(casts) - set(names)
    if unknown:
        raise ValueError(f"cast_columns contains unknown columns: {sorted(unknown)}")
    expressions = []
    for name in names:
        qname = quote_ident(name)
        if name in casts:
            expressions.append(f"CAST({qname} AS {casts[name].upper()}) AS {qname}")
        else:
            expressions.append(qname)
    return ", ".join(expressions)


def canonical_columns(df: DataFrame) -> List[object]:
    result = []
    for field in df.schema.fields:
        col = F.col(f"`{field.name}`")
        dtype = field.dataType.simpleString().lower()
        if dtype.startswith(("struct<", "array<", "map<")):
            rendered = F.to_json(col, options={"ignoreNullFields": "false"})
        elif dtype in ("float", "double"):
            rendered = F.format_string("%.17g", col.cast("double"))
        elif dtype.startswith("timestamp"):
            rendered = F.date_format(col.cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
        else:
            rendered = col.cast("string")
        result.append(F.coalesce(rendered, F.lit("<ICEBERG_MIGRATION_NULL>")))
    return result


def fingerprint_df(df: DataFrame, group_columns: Sequence[str] = ()) -> DataFrame:
    values = canonical_columns(df)
    hash_a = F.xxhash64(*values)
    hash_b = F.xxhash64(F.lit("validation-salt-v1"), *values)
    aggregates = [
        F.count(F.lit(1)).alias("row_count"),
        F.sum(hash_a.cast("decimal(38,0)")).alias("hash_a"),
        F.sum(hash_b.cast("decimal(38,0)")).alias("hash_b"),
    ]
    if group_columns:
        return df.groupBy(*[F.col(f"`{name}`") for name in group_columns]).agg(*aggregates)
    return df.agg(*aggregates)


def normalized_type(dtype: str) -> str:
    lower = dtype.lower()
    if lower in ("tinyint", "smallint"):
        return "int"
    if lower.startswith("char(") or lower.startswith("varchar("):
        return "string"
    return lower


def compare_schema(source_df: DataFrame, target_df: DataFrame) -> Tuple[bool, List[str]]:
    source_fields = source_df.schema.fields
    target_fields = target_df.schema.fields
    details: List[str] = []
    if [x.name.lower() for x in source_fields] != [x.name.lower() for x in target_fields]:
        details.append("column names/order differ")
        return False, details
    for source_field, target_field in zip(source_fields, target_fields):
        source_type_name = normalized_type(source_field.dataType.simpleString())
        target_type_name = normalized_type(target_field.dataType.simpleString())
        if source_type_name != target_type_name:
            details.append(
                f"type mismatch {source_field.name}: {source_field.dataType.simpleString()} -> "
                f"{target_field.dataType.simpleString()}"
            )
    return not details, details


def collect_fingerprint(df: DataFrame) -> Tuple[int, Optional[Decimal], Optional[Decimal]]:
    row = fingerprint_df(df).first()
    return int(row["row_count"]), row["hash_a"], row["hash_b"]


def compare_partitions(
    source_df: DataFrame,
    target_df: DataFrame,
    partitions: Sequence[str],
    max_partitions: int,
) -> Tuple[Optional[bool], List[str]]:
    if not partitions:
        return None, []
    for name in partitions:
        if name not in source_df.columns or name not in target_df.columns:
            return False, [f"partition column missing from data: {name}"]
    source_fp = fingerprint_df(source_df, partitions)
    target_fp = fingerprint_df(target_df, partitions)
    source_count = source_fp.count()
    target_count = target_fp.count()
    if max(source_count, target_count) > max_partitions:
        return False, [
            f"partition validation refused: {source_count}/{target_count} partitions exceed limit {max_partitions}"
        ]
    keys = list(partitions)
    joined = source_fp.alias("s").join(target_fp.alias("t"), keys, "full_outer")
    mismatch = joined.where(
        F.col("s.row_count").isNull()
        | F.col("t.row_count").isNull()
        | (F.col("s.row_count") != F.col("t.row_count"))
        | (F.col("s.hash_a") != F.col("t.hash_a"))
        | (F.col("s.hash_b") != F.col("t.hash_b"))
    )
    examples = mismatch.limit(20).collect()
    if examples:
        return False, [f"partition mismatches={mismatch.count()}; examples={examples}"]
    return True, []


def validate_tables(
    spark: SparkSession,
    source: str,
    target: str,
    partitions: Sequence[str],
    max_partitions: int,
) -> ValidationResult:
    source_df = spark.table(source)
    target_df = spark.table(target)
    schema_ok, schema_details = compare_schema(source_df, target_df)
    source_count, source_a, source_b = collect_fingerprint(source_df)
    target_count, target_a, target_b = collect_fingerprint(target_df)
    global_ok = (
        source_count == target_count and source_a == target_a and source_b == target_b
    )
    partition_ok, partition_details = compare_partitions(
        source_df, target_df, partitions, max_partitions
    )
    details = schema_details + partition_details
    if not global_ok:
        details.append(
            f"global mismatch rows {source_count}/{target_count}; "
            f"hash_a {source_a}/{target_a}; hash_b {source_b}/{target_b}"
        )
    ok = schema_ok and global_ok and partition_ok is not False
    return ValidationResult(ok, source_count, target_count, schema_ok, global_ok, partition_ok, details)


def write_validation(path: str, run_id: str, source: str, target: str, result: ValidationResult) -> None:
    append_jsonl(
        path,
        {
            "run_id": run_id,
            "source": source,
            "target": target,
            "validation": asdict(result),
        },
    )


def require_safe_location(location: str, fs_default: str) -> None:
    if not location:
        return
    if location.startswith("hdfs://"):
        authority = location[len("hdfs://") :].split("/", 1)[0]
        default_authority = fs_default[len("hdfs://") :].split("/", 1)[0]
        if authority != default_authority:
            raise ValueError(
                f"target_location authority {authority!r} differs from fs.defaultFS {default_authority!r}"
            )
    elif not location.startswith("/"):
        raise ValueError("target_location must be an absolute HDFS path or hdfs:// logical-nameservice URI")


def create_ctas(
    spark: SparkSession,
    source: str,
    target: str,
    partition_spec: Sequence[str],
    casts: Dict[str, str],
    target_location: str,
) -> None:
    if table_exists(spark, target):
        raise RuntimeError(f"Refusing to overwrite existing staging table {target}")
    partition_sql = f"PARTITIONED BY ({', '.join(partition_spec)})" if partition_spec else ""
    location_sql = f"LOCATION {sql_literal(target_location)}" if target_location else ""
    select_sql = ctas_projection(spark, source, casts)
    spark.sql(
        f"CREATE TABLE {qtable(target)} USING iceberg {partition_sql} {location_sql} "
        f"TBLPROPERTIES ({properties_ddl_sql(ICEBERG_PROPERTIES)}) "
        f"AS SELECT {select_sql} FROM {qtable(source)}"
    )


def cutover_ctas(
    spark: SparkSession,
    source: str,
    target: str,
    legacy: str,
    journal_path: str,
    run_id: str,
) -> None:
    if table_exists(spark, legacy):
        raise RuntimeError(f"Refusing cutover because retained legacy table exists: {legacy}")
    spark.sql(f"ALTER TABLE {qtable(source)} RENAME TO {qtable(legacy)}")
    journal(journal_path, run_id, source, "SOURCE_RENAMED", legacy=legacy)
    try:
        spark.sql(f"ALTER TABLE {qtable(target)} RENAME TO {qtable(source)}")
        journal(journal_path, run_id, source, "TARGET_RENAMED", target=target)
    except Exception:
        LOG.exception("Second rename failed; attempting immediate source-name restoration")
        if not table_exists(spark, source) and table_exists(spark, legacy):
            spark.sql(f"ALTER TABLE {qtable(legacy)} RENAME TO {qtable(source)}")
            journal(journal_path, run_id, source, "CUTOVER_AUTO_ROLLED_BACK")
        raise


def rollback_migrate(spark: SparkSession, source: str, backup: str) -> None:
    if not table_exists(spark, source) or not table_exists(spark, backup):
        raise RuntimeError(f"Rollback requires both {source} and {backup}")
    spark.sql(
        f"ALTER TABLE {qtable(source)} SET TBLPROPERTIES ('external.table.purge'='false')"
    )
    spark.sql(f"DROP TABLE {qtable(source)}")
    spark.sql(f"ALTER TABLE {qtable(backup)} RENAME TO {qtable(source)}")


def emit_hive_acid_hql(path: str, row: Dict[str, str]) -> None:
    source = row["table"]
    db, table = parse_table(source)
    target = f"{db}.{table}{CTAS_SUFFIX}"
    target_location = row.get("target_location", "").strip()
    if not target_location:
        raise ValueError("Hive ACID HQL generation requires target_location in the plan")
    partition_spec = parse_partition_spec(row.get("target_partition_spec", ""))
    partition_sql = f"PARTITIONED BY SPEC ({', '.join(partition_spec)})\n" if partition_spec else ""
    hql = (
        f"-- MANUAL_ACTION_REQUIRED: validate this syntax on the exact CDP release.\n"
        f"-- Source: {source}\n"
        f"CREATE EXTERNAL TABLE {target}\n"
        f"{partition_sql}"
        f"STORED BY ICEBERG\nSTORED AS PARQUET\n"
        f"LOCATION {sql_literal(target_location)}\n"
        f"TBLPROPERTIES ('format-version'='2')\n"
        f"AS SELECT * FROM {source};\n\n"
    )
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(hql)


def sandbox_target(sandbox: str, source: str, run_id: str) -> str:
    db, table = parse_table(source)
    safe_run = run_id.lower().replace("-", "_")
    return f"{sandbox}.{db}__{table}__{safe_run}"


def verify_execute_row(row: Dict[str, str], require_freeze_ticket: bool) -> None:
    if row.get("approved", "N").upper() != "Y":
        raise ValueError("plan row is not approved=Y")
    if row.get("target_format_version", "") != "2":
        raise ValueError("target_format_version must be 2")
    if row.get("schema_compatible", "N").upper() != "Y":
        raise ValueError("schema_compatible is not Y")
    if require_freeze_ticket and not row.get("writer_freeze_ticket", "").strip():
        raise ValueError("writer_freeze_ticket is required for execute mode")


def migrate_one(
    spark: SparkSession,
    row: Dict[str, str],
    args: argparse.Namespace,
    run_id: str,
) -> Dict[str, object]:
    source = row["table"]
    strategy = row["strategy"].strip().lower()
    db, table = parse_table(source)
    mode = "execute" if args.execute else "dry-run"
    started = time.time()
    result: Dict[str, object] = {
        "run_id": run_id,
        "table": source,
        "strategy": strategy,
        "mode": mode,
        "status": "",
        "source_rows": "",
        "target_rows": "",
        "seconds": "",
        "state": "",
        "note": "",
    }
    journal(args.journal, run_id, source, "STARTED", strategy=strategy, mode=mode)

    if strategy in ("skip", "manual"):
        result.update(status="SKIPPED", state="SKIPPED", note=row.get("note", ""))
        return result
    if strategy == "hive_ctas":
        emit_hive_acid_hql(args.hql, row)
        result.update(
            status="MANUAL_ACTION_REQUIRED",
            state="HQL_EMITTED",
            note=f"Review and execute {args.hql} through the approved Hive/HWC path",
        )
        journal(args.journal, run_id, source, "HQL_EMITTED", hql=args.hql)
        return result
    if strategy not in ("migrate", "ctas"):
        raise ValueError(f"Unknown strategy: {strategy}")

    if args.execute:
        verify_execute_row(row, not args.no_require_freeze_ticket)
    partition_columns = [x for x in row.get("partitions", "").split(";") if x]
    target_partition_spec = parse_partition_spec(row.get("target_partition_spec", ""))
    casts = parse_casts(row.get("cast_columns", ""))
    target_location = row.get("target_location", "").strip()
    fs_default = spark._jsc.hadoopConfiguration().get("fs.defaultFS", "")
    require_safe_location(target_location, fs_default)

    if not args.execute:
        quote_ident(args.sandbox)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {quote_ident(args.sandbox)}")
        target = sandbox_target(args.sandbox, source, run_id)
        if table_exists(spark, target):
            raise RuntimeError(f"Sandbox table unexpectedly exists: {target}")
        if strategy == "migrate":
            spark.sql(
                "CALL spark_catalog.system.snapshot("
                f"source_table => {sql_literal(source)}, "
                f"table => {sql_literal(target)}, "
                f"properties => {properties_map_sql(ICEBERG_PROPERTIES)})"
            )
        else:
            create_ctas(spark, source, target, target_partition_spec, casts, target_location)
        journal(args.journal, run_id, source, "SANDBOX_CREATED", target=target)
        validation = validate_tables(
            spark, source, target, partition_columns, args.max_partitions
        )
        write_validation(args.validation_log, run_id, source, target, validation)
        result.update(
            source_rows=validation.source_rows,
            target_rows=validation.target_rows,
            status="DRY_RUN_VALIDATED" if validation.ok else "VALIDATION_FAILED",
            state="SANDBOX_VALIDATED" if validation.ok else "SANDBOX_VALIDATION_FAILED",
            note="; ".join(validation.details) or f"sandbox retained as {target}",
        )
        journal(args.journal, run_id, source, result["state"], target=target)
    elif strategy == "migrate":
        if row.get("source_type", "").upper() != "EXTERNAL":
            raise ValueError("migrate is only permitted for source_type=EXTERNAL")
        backup = f"{db}.{table}{BACKUP_SUFFIX}"
        if table_exists(spark, backup):
            raise RuntimeError(f"Refusing migrate because backup already exists: {backup}")
        spark.sql(
            "CALL spark_catalog.system.migrate("
            f"{sql_literal(source)}, {properties_map_sql(ICEBERG_PROPERTIES)})"
        )
        journal(args.journal, run_id, source, "MIGRATED", backup=backup)
        validation = validate_tables(
            spark, backup, source, partition_columns, args.max_partitions
        )
        write_validation(args.validation_log, run_id, backup, source, validation)
        result.update(source_rows=validation.source_rows, target_rows=validation.target_rows)
        if validation.ok:
            result.update(
                status="CUTOVER_VALIDATED",
                state="CUTOVER_VALIDATED",
                note=f"Retain shared-file backup {backup}; do not PURGE or run orphan cleanup",
            )
            journal(args.journal, run_id, source, "CUTOVER_VALIDATED", backup=backup)
        elif args.auto_rollback:
            rollback_migrate(spark, source, backup)
            result.update(
                status="AUTO_ROLLED_BACK",
                state="AUTO_ROLLED_BACK",
                note="; ".join(validation.details),
            )
            journal(args.journal, run_id, source, "AUTO_ROLLED_BACK", reason=validation.details)
        else:
            result.update(
                status="VALIDATION_FAILED",
                state="ROLLBACK_REQUIRED",
                note="; ".join(validation.details) + f"; backup={backup}",
            )
            journal(args.journal, run_id, source, "ROLLBACK_REQUIRED", backup=backup)
    else:
        target = f"{db}.{table}{CTAS_SUFFIX}"
        legacy = f"{db}.{table}{LEGACY_SUFFIX}"
        create_ctas(spark, source, target, target_partition_spec, casts, target_location)
        journal(args.journal, run_id, source, "CTAS_CREATED", target=target)
        validation = validate_tables(
            spark, source, target, partition_columns, args.max_partitions
        )
        write_validation(args.validation_log, run_id, source, target, validation)
        result.update(source_rows=validation.source_rows, target_rows=validation.target_rows)
        if not validation.ok:
            result.update(
                status="VALIDATION_FAILED",
                state="CTAS_VALIDATION_FAILED",
                note="; ".join(validation.details) + f"; staging retained as {target}",
            )
            journal(args.journal, run_id, source, "CTAS_VALIDATION_FAILED", target=target)
        else:
            journal(args.journal, run_id, source, "CTAS_VALIDATED", target=target)
            cutover_ctas(spark, source, target, legacy, args.journal, run_id)
            post = validate_tables(
                spark, legacy, source, partition_columns, args.max_partitions
            )
            write_validation(args.validation_log, run_id, legacy, source, post)
            if post.ok:
                result.update(
                    status="CUTOVER_VALIDATED",
                    state="CUTOVER_VALIDATED",
                    note=f"Retain legacy table {legacy} through the rollback window",
                )
                journal(args.journal, run_id, source, "CUTOVER_VALIDATED", legacy=legacy)
            elif args.auto_rollback:
                # Keep the failed Iceberg table under a diagnostic name; never drop it automatically.
                failed = f"{db}.{table}__failed_{run_id.lower().replace('-', '_')}"
                spark.sql(f"ALTER TABLE {qtable(source)} RENAME TO {qtable(failed)}")
                spark.sql(f"ALTER TABLE {qtable(legacy)} RENAME TO {qtable(source)}")
                result.update(
                    status="AUTO_ROLLED_BACK",
                    state="AUTO_ROLLED_BACK",
                    note="; ".join(post.details) + f"; failed target retained as {failed}",
                )
                journal(args.journal, run_id, source, "AUTO_ROLLED_BACK", failed_target=failed)
            else:
                result.update(
                    status="VALIDATION_FAILED",
                    state="ROLLBACK_REQUIRED",
                    note="; ".join(post.details) + f"; legacy={legacy}",
                )
                journal(args.journal, run_id, source, "ROLLBACK_REQUIRED", legacy=legacy)

    result["seconds"] = round(time.time() - started, 1)
    return result


def load_plan(path: str) -> List[Dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    missing = [field for field in PLAN_FIELDS if field not in (rows[0].keys() if rows else [])]
    if missing:
        raise ValueError(f"Plan is missing required columns: {missing}")
    return rows


def selected_rows(rows: Iterable[Dict[str, str]], only: str) -> List[Dict[str, str]]:
    selected = {x.strip() for x in only.split(",") if x.strip()} if only else None
    output = [row for row in rows if not selected or row["table"] in selected]
    if selected:
        found = {row["table"] for row in output}
        missing = selected - found
        if missing:
            raise ValueError(f"--only tables not found in plan: {sorted(missing)}")
    return output


def confirm_execute(message: str, yes: bool) -> None:
    LOG.warning(message)
    if yes:
        return
    if input("Type EXECUTE to continue: ").strip() != "EXECUTE":
        raise RuntimeError("Aborted by operator")


def cmd_run(spark: SparkSession, args: argparse.Namespace) -> int:
    run_id = args.run_id or new_run_id()
    rows = selected_rows(load_plan(args.plan), args.only)
    if args.execute:
        rows = [row for row in rows if row.get("approved", "N").upper() == "Y"]
        if not rows:
            raise ValueError("No approved rows selected")
        cluster = fs_preflight(spark, require_ha=not args.allow_non_ha)
        if not cluster["ok"]:
            raise RuntimeError("Cluster preflight failed: " + "; ".join(cluster["errors"]))
        confirm_execute(
            f"EXECUTE mode selected for {len(rows)} table(s). Confirm writers are frozen and approvals are valid.",
            args.yes,
        )
    results: List[Dict[str, object]] = []
    for row in rows:
        LOG.info("[%s] %s -> %s", "EXEC" if args.execute else "DRY", row["table"], row["strategy"])
        try:
            results.append(migrate_one(spark, row, args, run_id))
        except Exception as exc:
            LOG.exception("Migration failed for %s", row["table"])
            journal(args.journal, run_id, row["table"], "FAILED", error=str(exc)[:2000])
            results.append(
                {
                    "run_id": run_id,
                    "table": row["table"],
                    "strategy": row["strategy"],
                    "mode": "execute" if args.execute else "dry-run",
                    "status": "FAILED",
                    "source_rows": "",
                    "target_rows": "",
                    "seconds": "",
                    "state": "FAILED",
                    "note": str(exc)[:1000],
                }
            )
            if args.stop_on_error:
                break
    with open(args.report, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    failures = [
        row for row in results if row["status"] in ("FAILED", "VALIDATION_FAILED", "AUTO_ROLLED_BACK")
    ]
    manual = [row for row in results if row["status"] == "MANUAL_ACTION_REQUIRED"]
    LOG.info(
        "Run %s complete: %d total, %d failures/rollbacks, %d manual actions. Report: %s",
        run_id,
        len(results),
        len(failures),
        len(manual),
        args.report,
    )
    return 1 if failures or manual else 0


def cmd_validate(spark: SparkSession, args: argparse.Namespace) -> int:
    run_id = args.run_id or new_run_id()
    partitions = [x for x in args.partitions.split(",") if x]
    result = validate_tables(spark, args.source, args.target, partitions, args.max_partitions)
    write_validation(args.validation_log, run_id, args.source, args.target, result)
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0 if result.ok else 1


def cmd_rollback(spark: SparkSession, args: argparse.Namespace) -> int:
    source = args.table
    db, table = parse_table(source)
    confirm_execute(f"ROLLBACK requested for {source}; writers must be frozen.", args.yes)
    if args.strategy == "migrate":
        backup = f"{db}.{table}{BACKUP_SUFFIX}"
        rollback_migrate(spark, source, backup)
        note = f"restored {backup} to {source}"
    else:
        legacy = f"{db}.{table}{LEGACY_SUFFIX}"
        if not table_exists(spark, source) or not table_exists(spark, legacy):
            raise RuntimeError(f"Rollback requires both {source} and {legacy}")
        failed = f"{db}.{table}__failed_{new_run_id().lower().replace('-', '_')}"
        spark.sql(f"ALTER TABLE {qtable(source)} RENAME TO {qtable(failed)}")
        try:
            spark.sql(f"ALTER TABLE {qtable(legacy)} RENAME TO {qtable(source)}")
        except Exception:
            spark.sql(f"ALTER TABLE {qtable(failed)} RENAME TO {qtable(source)}")
            raise
        note = f"restored {legacy}; failed Iceberg target retained as {failed}"
    journal(args.journal, args.run_id or new_run_id(), source, "OPERATOR_ROLLBACK_COMPLETE", note=note)
    LOG.warning(note)
    return 0


def cmd_optimize(spark: SparkSession, args: argparse.Namespace) -> int:
    rows = selected_rows(load_plan(args.plan), args.only)
    rows = [
        row for row in rows
        if row.get("approved", "N").upper() == "Y" and row.get("compact", "N").upper() == "Y"
    ]
    if not rows:
        raise ValueError("No approved compact=Y rows selected")
    confirm_execute(f"OPTIMIZE will rewrite data files for {len(rows)} table(s).", args.yes)
    failures = 0
    run_id = args.run_id or new_run_id()
    for row in rows:
        source = row["table"]
        try:
            spark.sql(
                "CALL spark_catalog.system.rewrite_data_files("
                f"table => {sql_literal(source)}, "
                f"options => map('target-file-size-bytes',{sql_literal(str(args.target_file_bytes))}))"
            )
            journal(args.journal, run_id, source, "OPTIMIZE_COMPLETE")
        except Exception as exc:
            failures += 1
            LOG.exception("Optimize failed for %s", source)
            journal(args.journal, run_id, source, "OPTIMIZE_FAILED", error=str(exc))
    return 1 if failures else 0


def cmd_status(args: argparse.Namespace) -> int:
    latest: Dict[Tuple[str, str], Dict[str, object]] = {}
    with open(args.journal, encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            latest[(str(record.get("run_id")), str(record.get("table")))] = record
    for (run_id, table), record in sorted(latest.items()):
        print(f"{run_id}\t{table}\t{record.get('state')}\t{record.get('timestamp_utc')}")
    return 0


def add_common_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", help="operator-supplied unique run ID; generated when omitted")
    parser.add_argument("--journal", default="migration_state.jsonl", help="append-only state journal")
    parser.add_argument("--validation-log", default="validation_evidence.jsonl")
    parser.add_argument("--max-partitions", type=int, default=10000)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    preflight = sub.add_parser("preflight", help="check cluster prerequisites")
    preflight.add_argument("--out", default="preflight.json")
    preflight.add_argument("--allow-non-ha", action="store_true", help="pilot exception only")

    inventory = sub.add_parser("inventory", help="scan databases and create plan.csv")
    inventory.add_argument("--databases", required=True, help="comma-separated Hive databases")
    inventory.add_argument("--out", default="plan.csv")
    inventory.add_argument("--with-size", action="store_true")
    inventory.add_argument("--allow-avro-migrate", action="store_true")

    run = sub.add_parser("run", help="sandbox dry-run or approved execute")
    run.add_argument("--plan", default="plan.csv")
    run.add_argument("--only", default="")
    run.add_argument("--execute", action="store_true")
    run.add_argument("--yes", action="store_true")
    run.add_argument("--auto-rollback", action="store_true")
    run.add_argument("--stop-on-error", action="store_true")
    run.add_argument("--no-require-freeze-ticket", action="store_true", help="exception requiring change approval")
    run.add_argument("--allow-non-ha", action="store_true", help="pilot exception only")
    run.add_argument("--sandbox", default="iceberg_sandbox")
    run.add_argument("--report", default="migration_results.csv")
    run.add_argument("--hql", default="acid_migration.hql")
    add_common_run_options(run)

    validate = sub.add_parser("validate", help="compare any source and target")
    validate.add_argument("source")
    validate.add_argument("target")
    validate.add_argument("--partitions", default="", help="comma-separated identity partition columns")
    add_common_run_options(validate)

    rollback = sub.add_parser("rollback", help="restore one table")
    rollback.add_argument("--table", required=True)
    rollback.add_argument("--strategy", choices=("migrate", "ctas"), required=True)
    rollback.add_argument("--yes", action="store_true")
    rollback.add_argument("--run-id")
    rollback.add_argument("--journal", default="migration_state.jsonl")

    optimize = sub.add_parser("optimize", help="post-cutover file compaction")
    optimize.add_argument("--plan", default="plan.csv")
    optimize.add_argument("--only", default="")
    optimize.add_argument("--target-file-bytes", type=int, default=TARGET_FILE_BYTES)
    optimize.add_argument("--yes", action="store_true")
    optimize.add_argument("--run-id")
    optimize.add_argument("--journal", default="migration_state.jsonl")

    status = sub.add_parser("status", help="show last journal state per run/table")
    status.add_argument("--journal", default="migration_state.jsonl")
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    if args.command == "status":
        return cmd_status(args)
    spark = get_spark()
    try:
        commands = {
            "preflight": cmd_preflight,
            "inventory": cmd_inventory,
            "run": cmd_run,
            "validate": cmd_validate,
            "rollback": cmd_rollback,
            "optimize": cmd_optimize,
        }
        return commands[args.command](spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
