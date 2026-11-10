---
name: featlas-regenerate
description: Recreate or regenerate feature tables on Databricks from a featlas specification (for example one produced by featlas-reverse-engineer or featlas-split-wide-table), and prove with read-only parity SQL that the new tables reproduce the legacy table before any cutover. Use when asked to rebuild, regenerate, migrate or re-platform feature tables from a spec.
---

# Regenerate feature tables from a spec and prove parity

You never modify or drop the legacy table, and you publish only with explicit authorization for the
target environment. Planning, generation and parity SQL are read-only.

## 1. Plan and generate

1. `spec_validate(document)` → must pass with `fixtures.failed == 0`.
2. `spec_generate(document, deployment)` (or `fe-spec generate <spec> --deployment <id> --out dir`)
   → per feature set, the Spark SQL artifacts and `script.sql`; the capability report says what is
   `supported_native` with live evidence and what is `unverified`. `execution_ready: false` is
   normal until every required capability has live evidence; report which ones are open.
3. Review the generated DDL: target table names must be the new ones (never the legacy table), with
   the publication metadata columns and the `PRIMARY KEY ... TIMESERIES` constraint.

## 2. Build (only with authorization)

Run with the host in `python/featlas_databricks` (see `docs/DATABRICKS_RUNBOOK.md`):
`SnapshotRun(spark, artifacts, as_of, coverage, ...)` → `preflight_sources()` (physical schemas
must match), then `publish(PublicationAuthorization(environment=..., approved_by=...,
allow_create_table=True, publication_id=..., publication_sequence=..., materialization_id=...))`.
`coverage` comes from the core (`Project.coverage`) with real source-status evidence; without
evidence, features are published as `insufficient_history`/`source_unavailable`, not as zeros.
For a backfill, publish each historical `as_of` in order with increasing `publication_sequence`.
If you are not authorized to write, stop after step 1 and hand over the generated artifacts.

## 3. Prove parity (read-only)

`parity_sql(legacy_table, legacy_as_of_column, keys, as_of, document=<spec>)` returns one query
comparing the legacy table with the regenerated table(s) at that `as_of`:
`missing_in_legacy`, `missing_in_new`, `mismatched_<column>` (null-safe; pass `tolerances` for
floating columns, e.g. the feature's `numeric_tolerance`). Run it for several `as_of` values,
including a recent one and one before any known data correction.

Interpret, do not paper over:

* `missing_in_legacy > 0` — usually entities with no events: the spec's population includes them
  with the `empty` value; the legacy table dropped them. Expected; document it.
* mismatches concentrated on recent `as_of`s — late-arriving rows the legacy code read after the
  fact (the legacy table is not point-in-time correct). Expected and desirable; document it.
* anything else — a translation error in the spec: go back to `featlas-reverse-engineer`, fix the
  spec, add a characterization case reproducing it, regenerate.

## 4. Cut over

Report parity per `as_of`, the explained differences, and the compatibility view (from
`split_apply`) for consumers that still read the wide shape. Cutover (repointing consumers,
deprecating the legacy table) is a human decision; propose it, do not perform it.
