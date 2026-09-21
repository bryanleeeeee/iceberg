# HDFS to Iceberg Migration Controls

A controlled migration utility and production runbook for converting Hive/HDFS tables to Apache Iceberg v2 on Cloudera Data Platform (CDP) Private Cloud Base.

This project keeps HDFS as the storage layer. It does not move data to object storage, alter Ranger policies, stop writers, purge backup data, or perform orphan-file cleanup automatically.

## Contents

- `hdfs_to_iceberg_hardened.py` - PySpark command-line tool for read-only HDFS/Hive assessment, preflight checks, inventory, sandbox validation, governed cutover, validation, rollback, optimization, and status reporting.
- `HDFS_to_Iceberg_Migration_Plan_and_Runbook.docx` - detailed migration plan and production operating runbook.

## Safety model

The utility is designed to fail closed. It validates identifiers and table eligibility, does not overwrite existing staging or recovery objects, and records state and validation evidence in append-only JSONL files.

Migration strategies are selected per table:

| Source condition | Strategy |
| --- | --- |
| External, non-ACID Parquet or ORC with a compatible schema | Metadata-only `migrate` after a validated sandbox run and writer freeze |
| Managed, bucketed, text/CSV/JSON, incompatible schema, or most Avro workloads | `ctas` rewrite |
| Hive ACID / transactional tables | `hive_ctas` - emits reviewable HiveQL for a separately approved Hive/HWC process |
| Existing Iceberg tables and views | `skip` |

Cutover requires an approved plan row and, by default, a writer-freeze ticket. Source backups and legacy tables are retained for rollback; file compaction is a separate maintenance action.

## Prerequisites

- A CDP-supported Spark, Hive, Impala, and Iceberg combination.
- The Iceberg libraries bundled with the target CDP runtime. Do not add a generic upstream Iceberg runtime JAR unless Cloudera confirms the exact compatible combination.
- HDFS high availability with a logical nameservice, plus the appropriate HMS, HDFS, Ranger, and encryption-zone permissions.
- A rehearsed pilot, cross-engine validation plan, writer-freeze process, and rollback window.

Pin the runbook and utility to the exact target CDP release and validate procedure signatures and Hive Iceberg DDL on a non-production table before production use.

## Quick start

Run commands with the CDP-provided Spark runtime. Store generated evidence in a restricted directory and do not place keytabs there.

```bash
# 1. Validate cluster configuration and capture evidence
spark-submit --master yarn --deploy-mode client \
  hdfs_to_iceberg_hardened.py preflight \
  --out evidence/preflight.json

# 2. Build an editable migration plan for a bounded database set
spark-submit ... hdfs_to_iceberg_hardened.py inventory \
  --databases sales,risk --with-size \
  --out evidence/plan.csv

# 2a. Produce the detailed, read-only assessment for the current HDFS estate.
# It reads Hive metastore and HDFS metadata only; it makes no table or file changes.
spark-submit --master yarn --deploy-mode client \
  hdfs_to_iceberg_hardened.py assess \
  --databases sales,risk \
  --report-out evidence/iceberg_migration_assessment.md \
  --json-out evidence/iceberg_migration_assessment.json \
  --plan-out evidence/plan.csv \
  --rewrite-throughput-gb-per-hour 250

# 3. Create and validate sandbox targets before approval
spark-submit ... hdfs_to_iceberg_hardened.py run \
  --plan evidence/plan.csv \
  --only sales.customer,risk.exposure \
  --run-id PILOT-001 \
  --report evidence/pilot_results.csv \
  --journal evidence/migration_state.jsonl \
  --validation-log evidence/validation_evidence.jsonl \
  --stop-on-error
```

Review the generated plan, sandbox evidence, Impala behavior, and business controls. Only then set the specific plan rows to `approved=Y` and record a verified `writer_freeze_ticket`.

```bash
# Execute only approved rows during the governed cutover window
spark-submit ... hdfs_to_iceberg_hardened.py run \
  --plan evidence/approved_plan.csv \
  --only sales.customer \
  --execute --yes --stop-on-error \
  --run-id CHG123456-MIG-01 \
  --report evidence/execute_results.csv \
  --journal evidence/migration_state.jsonl \
  --validation-log evidence/validation_evidence.jsonl
```

## Validation and recovery

The tool compares compatible schemas, exact row counts, two order-independent hashes, and optionally identity partitions. These automated checks are evidence, not a substitute for table-specific business reconciliations or Impala and application tests.

Rollback is intentionally explicit and operates one table at a time:

```bash
spark-submit ... hdfs_to_iceberg_hardened.py rollback \
  --table sales.customer --strategy ctas \
  --journal evidence/migration_state.jsonl
```

Do not use `DROP ... PURGE`, aggressive snapshot expiry, or orphan-file cleanup while rollback objects or dependent snapshots may need the underlying files.

## Readiness assessment

`assess` is the recommended starting point for a current Cloudera HDFS estate. It produces:

- A Markdown decision report with strategy mix, total and rewrite volume, planning-duration estimate, preflight result, required gates, and a risk-ranked table inventory.
- A JSON report containing the same data plus HDFS URI, owner, group, permission, replication, and encryption-zone metadata where the cluster permits inspection.
- An editable CSV plan compatible with the existing `run` command. Every row starts unapproved.

The report’s risk band is a transparent prioritisation aid, not a production approval. It does not inspect Ranger policies, data quality, lineage, active writers, or cross-engine behavior. Calibrate the throughput flag with a representative pilot and complete the runbook controls before cutover.

## Operational commands

```bash
# Compare an existing source and target
spark-submit ... hdfs_to_iceberg_hardened.py validate \
  sales.customer__legacy sales.customer \
  --partitions business_date \
  --validation-log evidence/validation_evidence.jsonl

# Perform separately approved post-cutover file compaction
spark-submit ... hdfs_to_iceberg_hardened.py optimize \
  --plan evidence/approved_plan.csv \
  --only sales.customer \
  --journal evidence/migration_state.jsonl

# Read the local append-only journal without starting Spark
python hdfs_to_iceberg_hardened.py status \
  --journal evidence/migration_state.jsonl
```

## Before production

Use the included runbook to complete the pilot, ownership and approval gates, writer-freeze proof, cross-engine test pack, recovery rehearsal, evidence retention, and later backup-retirement process.
