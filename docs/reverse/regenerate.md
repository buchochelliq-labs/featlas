# Regeneration and parity

**In plain terms:** once the spec is right, featlas generates the Spark SQL for the new tables.
Before anyone switches over, a single read-only query compares the old table with the new ones
for a chosen date: rows that exist on only one side, and, per column, how many values differ.
Some differences are expected and good (the old table counted late data, or dropped customers
with no activity); anything else means the spec needs fixing.

## 1. Plan and generate (read-only)

1. Validate: `fe-spec validate spec.yaml` and `fe-spec test spec.yaml` (or the MCP tool
   `spec_validate`, which also runs the fixtures) must pass with `failed == 0`.
2. Generate: `fe-spec generate spec.yaml --deployment <id> --out dir` (or `spec_generate`). The
   capability report says which checks are `supported_native` with live evidence and which are
   `unverified`; `execution_ready: false` is normal until every required capability has live
   evidence ([Capabilities](../concepts/capabilities.md)).
3. Review the DDL: the target tables must be the new ones (`…_featlas…`, never the legacy table),
   with the publication metadata columns and the `PRIMARY KEY … TIMESERIES` constraint.

## 2. Build (only with authorization)

The tables are built by the Databricks host ([Databricks guide](../guides/databricks.md)):
`SnapshotRun(...).preflight_sources()`, then `publish(PublicationAuthorization(...))`, with
coverage from the core and real source-status evidence (without evidence, features publish as
`insufficient_history`/`source_unavailable`, not as zeros). For a backfill the skill says to
publish each historical `as_of` in order with increasing `publication_sequence`. Without write
authorization, stop after step 1 and hand over the artifacts.

## 3. Prove parity (read-only)

```sh
featlas-reverse parity split.yaml --legacy-table main.features.customer_wide \
  --legacy-as-of-column feature_ts --key tenant_id --key customer_id \
  --as-of 2026-06-01T00:00:00Z > parity.sql
```

`parity.parity_sql` returns one query that, at one `as_of`:

* selects the legacy rows (`legacy_as_of_column = as_of`) and each regenerated table's rows
  (`feature_as_of = as_of`), using the mapping from the spec's output targets
  (`mapping_from_project`) or an explicit mapping;
* forms the union of keys on both sides;
* reports `rows_compared`, `missing_in_legacy`, `missing_in_new` and one `mismatched_<column>`
  per feature column, compared null-safe with `<=>`; pass `tolerances` (MCP `parity_sql`) for
  floating-point columns, e.g. the feature's `numeric_tolerance`.

A clean result is `missing_in_new = missing_in_legacy = 0` and every `mismatched_* = 0`. The
skill asks for several `as_of` values, including a recent one and one before any known data
correction.

### Interpreting differences

From `.claude/skills/featlas-regenerate` and the `parity.py` docstring:

| Observation | Usual cause | Action |
| --- | --- | --- |
| `missing_in_legacy > 0` | Entities with no events: the spec's population includes them with the `empty` value; the legacy table dropped them | Expected; document it |
| Mismatches concentrated on recent `as_of`s | Late-arriving rows the legacy code read after the fact (not point-in-time correct) | Expected and desirable; document it |
| Anything else | A translation error in the spec | Fix the spec, add a characterization case that reproduces it, regenerate |

The late-arrival divergence is shown on synthetic data by
`test_late_arrival_is_a_reported_legacy_divergence` ([Characterization](characterization.md#the-late-arrival-divergence-demonstrated)).
Without late rows, `test_split_by_domain_regenerates_legacy_values` runs the generated parity
SQL over the legacy rows and the regenerated snapshots on local Spark and gets
`rows_compared = 3`, `missing_in_new = missing_in_legacy = 0` and every mismatch count 0.

## 4. Cut over

Report parity per `as_of`, the explained differences, and the compatibility view from
`split-apply` for consumers that still read the wide shape. Repointing consumers and deprecating
the legacy table are human decisions: propose them, do not perform them.
