# Databricks live acceptance runbook

> **Status:** automated as `.github/workflows/databricks-live.yml`; passed on Databricks Free
> Edition (AWS, serverless) in runs 42771706619 and 42816483568 — see `evidence/databricks-live/`. The
> Azure Databricks gate below is still open: no Azure workspace has been used.

**Status: not executed.** This session had no Azure Databricks workspace, credentials or
authorization. Nothing here has run on Databricks. Generated code has been executed only on
local Apache Spark (see `docs/EVIDENCE.md`), which is not Databricks acceptance.

## Prerequisites (all must be explicitly authorized)

1. A non-production Azure Databricks workspace with Unity Catalog, a catalog/schema the run
   may write to, and a Databricks unified-auth profile (credentials outside this repo).
2. Compute that supports Databricks Connect or a job cluster; record the exact Databricks
   Runtime version.
3. Read access to the bound source tables; write/create-table privilege on the output schema.
4. A source-status evidence table or file giving `[coverage_start, coverage_end)` or outage
   per source, with provenance (never inferred from row counts).
5. Budget approval matching the deployment `budget` (scan bytes, rows, memory, concurrency).

## Procedure for `projects/01_fault_count.yaml`, deployment `fault_prod`

```sh
cargo build --release
fe-spec plan projects/01_fault_count.yaml --deployment fault_prod --format text
fe-spec generate projects/01_fault_count.yaml --deployment fault_prod --out generated/fault_prod
pip install -e python[databricks]   # pin the versions that succeed (below)
featlas-databricks projects/01_fault_count.yaml --deployment fault_prod \
  --feature-set device_reliability --as-of 2026-06-08T00:00:00Z --status status.json   # dry run
```

1. **Preflight**: the host runs `DESCRIBE TABLE` for every bound source and compares the exact
   physical columns/types with `expected_source_schemas` (rename-only mappings; no casts).
2. **Differential acceptance**: load the reviewed fixture histories into scratch tables, run
   `python/tests/test_local_spark_differential.py` against a Databricks Connect session instead
   of local Spark, and compare keys, values, states and diagnostics with the core.
3. **Plan inspection**: `EXPLAIN FORMATTED` the feature views; confirm no event-window or
   business predicate is pushed below the `ROW_NUMBER()` revision selection and that
   availability filtering precedes it.
4. **Publication (authorized only)**: `--publish --environment … --approved-by …
   --publication-id … --publication-sequence … --materialization-id … [--allow-create-table]`.
   Capture the returned evidence (table version, commit timestamp, operation metrics).
5. **Failure/retry**: kill the run between MERGE and evidence recording, rerun; verify no
   duplicate rows and that the consumer API serves nothing until `published_at` is recorded.
6. **Consumer read**: run the generated `consumer_read` statement and pass the records to
   `fe-spec consume --records`; compare with fixture expectations, including replay cases.
7. **Retention/permissions/classification**: verify output retention, grants for the read and
   write principals, and table properties; record observations, not declarations.

## Evidence to persist (no credentials or raw logs)

Exact candidate commit, `fe-spec --version`, executable SHA-256, Databricks Runtime and
Databricks Connect/SDK versions, workspace region/edition, generated plan and script digests
(`manifest.json`), source snapshot identities (Delta versions), assertion results, output
digests, commit evidence, and timings. Then pin successful versions in an adapter lock
(`python/adapter-lock.json`, to be created only from observed runs) and set capability checks to
`supported_*` with `live_target` evidence for exactly the verified operations.

## Out of scope until separately evidenced

Online store/Feature Serving, Feature Views lowering, job scheduling (`core.scheduling`),
learned/custom execution, label dataset construction on Spark.
