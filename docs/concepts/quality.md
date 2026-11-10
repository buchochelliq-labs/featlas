# Quality policies

**In plain terms:** a feature can declare checks on its own values ("never negative", "at most
1", "not null"). When a check fails, the feature says what should happen: **stop** the run
(`error`), **record a warning** and publish the values unchanged (`warn`), or **quarantine**:
keep the bad values out of the published table entirely and hand them to an operator. featlas
never quietly changes or drops a value because a check failed.

## What is checked (DECISIONS D12, SEM-OUT-001, SEM-QUAL-001)

Every value of a feature is checked, in this fixed order of *rules*:

| Rank | Rule | Source in the spec |
| --- | --- | --- |
| 0 | output type and nullability | `output.type`, `output.nullable` |
| 1 | minimum | `output.constraints.minimum` |
| 2 | maximum | `output.constraints.maximum` |
| 3 | enumeration | `output.constraints.enumeration` |
| 4 + *i* | declared check *i* | `quality.checks[i]` (`range`, `not_null`, `finite`, each with `severity: error` or `warn`) |

## What happens when a rule is violated

| Rule | `quality.on_violation: error` | `quality.on_violation: quarantine` |
| --- | --- | --- |
| type/nullability, constraints | fail (`OUTPUT_TYPE_MISMATCH` / `QUALITY_VIOLATION`) | fail (same) |
| check with `severity: error` | fail (`QUALITY_VIOLATION`) | quarantine finding |
| check with `severity: warn` | warning finding | warning finding |

* **Warnings never change data.** Value, state and publication are unchanged; the finding
  (feature, check, check index, entity keys, value) is returned with the evaluation as
  `quality_warnings`, and the Databricks host includes it in its publication evidence.
* **Quarantine blocks the whole snapshot.** Once every member feature has passed its error-level
  rules, any quarantine finding stops the run with `QUALITY_QUARANTINED`; nothing is merged. The
  Spark host attaches the violating rows to the error (`detail["quarantined"]`) for the operator.
* **Rule-major precedence.** Across a feature's values the first failing *rule* (lowest rank)
  wins, then population order. Ranking by rule lets the generated SQL — one query per rule, in
  rank order — report the same first code as the core, even though Spark does not reproduce the
  core's row order. In the overall diagnostic precedence these rules come after a feature's
  runtime errors, and quarantine comes after every feature (DECISIONS D8 as amended).

Implementation: `crates/featlas-core/src/eval.rs` (`check_output`, `QualityFinding`),
`crates/featlas-adapters/src/databricks.rs` (`quality_statements`),
`python/featlas_databricks/runner.py` (`SnapshotRun.execute`).

## Worked example: `projects/quality/`

`projects/quality/derive.py` copies project 01, keeps only the `tenant_isolation` case
(tenant_a has 1 fault, tenant_b has 2) and adds a `range` check with `maximum: 1` to
`device.fault_count_7d`, in three variants:

| Project | Check | `on_violation` | Expected |
| --- | --- | --- | --- |
| `quality_warn.yaml` | `severity: warn` | `error` | rows unchanged, one warning for tenant_b |
| `quality_quarantine.yaml` | `severity: error` | `quarantine` | `QUALITY_QUARANTINED` |
| `quality_error.yaml` | `severity: error` | `error` | `QUALITY_VIOLATION` |

All three pass `fe-spec test` on this commit. The warn case, from
`fe-spec evaluate projects/quality/quality_warn.yaml --fixture quality_warn_tenant_b_exceeds_range`
(selected fields):

```json
{
 "status": "passed",
 "actual_rows": [
  {"entity_keys": {"tenant_id": "tenant_a", "device_id": "device_1"}, "value": 1, "state": "ok"},
  {"entity_keys": {"tenant_id": "tenant_b", "device_id": "device_1"}, "value": 2, "state": "ok"}
 ],
 "quality_warnings": [
  {
   "feature": "device.fault_count_7d",
   "check": "range",
   "check_index": 1,
   "severity": "warn",
   "outcome": "warning",
   "entity_keys": {"tenant_id": "tenant_b", "device_id": "device_1"},
   "value": 2
  }
 ]
}
```

The quarantine case:

```json
{
 "status": "passed",
 "actual_diagnostic": {
  "code": "QUALITY_QUARANTINED",
  "rule": "SEM-QUAL-001",
  "message": "1 value(s) violate error-level quality checks under on_violation: quarantine (device.fault_count_7d range {\"tenant_id\": String(\"tenant_b\"), \"device_id\": String(\"device_1\")}); the snapshot must not be published as successful data"
 }
}
```

## In generated Spark SQL

Each feature's entry in `artifacts.json` carries a `quality` list, one statement per rule, each
returning the violating rows (keys, `value`, `state`) with its `rank`, `rule`, `code` and
`outcome`. For `quality_warn.yaml`:

```json
{"name": "fl_feature_device_fault_count_7d_quality_1_minimum", "rank": 1, "rule": "SEM-OUT-001", "code": "QUALITY_VIOLATION", "outcome": "error", "check": "minimum", "sql": "SELECT `tenant_id`, `device_id`, fl_value AS value, fl_state AS state FROM fl_feature_device_fault_count_7d WHERE fl_value < 0"}
{"name": "fl_feature_device_fault_count_7d_quality_4_finite", "rank": 4, "rule": "SEM-QUAL-001", "code": "QUALITY_VIOLATION", "outcome": "error", "check": "finite", "sql": "SELECT `tenant_id`, `device_id`, fl_value AS value, fl_state AS state FROM fl_feature_device_fault_count_7d WHERE false"}
{"name": "fl_feature_device_fault_count_7d_quality_5_range", "rank": 5, "rule": "SEM-QUAL-001", "code": "QUALITY_WARNING", "outcome": "warning", "check": "range", "sql": "SELECT `tenant_id`, `device_id`, fl_value AS value, fl_state AS state FROM fl_feature_device_fault_count_7d WHERE (fl_value > 1)"}
```

(The `finite` check on an integer output can never fail, hence `WHERE false`.) These statements
are in `artifacts.json` only; the review file `script.sql` does not list them on this commit.
The host runs them per feature after the feature's view is materialized: the first `error` rule
with rows raises its code; `warning` rows go to `SnapshotRun.quality_warnings`; `quarantine`
rows are collected and raise `QUALITY_QUARANTINED` after the last feature. The differential
harness requires Spark's warnings to equal the core's exactly
(`differential.py::compare_warnings`).
