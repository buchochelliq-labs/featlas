# Missingness and coverage

**In plain terms:** "no value" can mean very different things. The device may exist but have
had no faults (a real zero). The data may not go back far enough to answer honestly. The source
system may be down. Or the input values may simply be null. featlas keeps these cases apart, so
an outage never turns into a healthy-looking `0`.

## Output states

Every feature value carries a **state** next to its value. The states are fixed by the schema
(`outputState`) and by `crates/featlas-core/src/consumer.rs` (`OUTPUT_STATES`):

| State | Meaning | Typical value |
| --- | --- | --- |
| `ok` | Computed from established coverage | The result (may be `null` under some null policies, see below) |
| `empty` | Entity is eligible, coverage is established, and no rows qualified | `missingness.empty` (for example `0`) |
| `insufficient_history` | Required history is not proven for this entity/time | `null`, or an error if the policy says `error` |
| `source_unavailable` | A direct, transitive or population source is reported unavailable | `null`, or an error |
| `not_published` | (consumer reads) No publication was selected for that feature | `null` |

Execution failures are diagnostics; `missingness.execution_failure` can only be `error`
(schema), so a failure can never become a default value.

## Coverage is positive evidence (SEM-MISS-002/003)

A zero count is valid only when the entity exists **and** the relevant source interval is known
to be complete. An empty table proves nothing. Coverage therefore comes from **source-status
records**, not from data:

```json
[
  {"source": "device_registry", "state": "available",
   "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2026-06-09T00:00:00Z"},
  {"source": "fault_events", "state": "unavailable"}
]
```

An `available` record gives a half-open envelope `[coverage_start, coverage_end)`. For a feature
at *T* with `history.required_seconds = H`, `coverage_state` in
`crates/featlas-core/src/eval.rs` reports `insufficient_history` when any of these holds for any
direct, transitive or population source of the feature:

* `coverage_start > T − H`;
* the end does not cover *T*: with an inclusive window end `coverage_end` must be `> T`, with an
  exclusive end `coverage_end >= T`;
* `T < history.earliest_as_of`;
* `T < retention.earliest_reproducible_at` of the source.

An `unavailable` record for any of those sources gives `source_unavailable`, which takes
precedence over `insufficient_history` (DECISIONS D4). A source with no status record at all is
an error (`MISSING_SOURCE_STATUS`, SEM-MISS-003).

The same computation is exposed to hosts through `fe-spec coverage`; with the records above:

```sh
fe-spec coverage projects/01_fault_count.yaml --deployment fault_prod \
  --feature-set device_reliability --as-of 2026-06-08T00:00:00Z --status status.json
```

```json
{
  "as_of": "2026-06-08T00:00:00Z",
  "features": {
    "device.fault_count_7d": {
      "coverage_state": "source_unavailable",
      "policy": "null",
      "variant": "degraded"
    }
  },
  "schema": "featlas.api/v1",
  "operation": "coverage",
  "command": "coverage"
}
```

`variant` tells the Databricks host which generated feature view to register: `computed` when
coverage is `established`, or the `degraded` view, which yields a typed null with the
core-computed state and reads no source (see [Databricks guide](../guides/databricks.md)).

## Coverage versus data errors

Available sources are still reconstructed when history is insufficient, so contract violations
in supplied rows (duplicates, invalid rows) still fail rather than being masked by a null
(DECISIONS D4, review item C6). Transform stages are skipped when coverage is not established.

## Derived features

A dependency's `source_unavailable` or `insufficient_history` state propagates to the derived
feature, which then applies its own missingness policy, rather than becoming an `ok` null
(DECISIONS D4).

## Nulls inside the computation

* Filters keep only rows whose predicate is `true`; `false` and `null` do not pass
  (SEM-TRANS-003). The `null_predicate` case of example 01 expects state `empty`, value `0`.
* Aggregate `nulls: ignore` excludes null inputs; if eligible rows exist but all values are null,
  the result is `null` with state `ok`, never the empty identity (SEM-TRANS-004).
* `propagate` returns null when an eligible value is null; `error` fails.

## Example 01 cases

From the `fault_count_conformance` suite (all passing; see `docs/evidence/fixtures-01_fault_count.json`):

| Case | Expected |
| --- | --- |
| `known_empty` | value `0`, state `empty` |
| `null_predicate` | value `0`, state `empty` |
| `insufficient_history` | value `null`, state `insufficient_history` |
| `source_outage` | value `null`, state `source_unavailable` |
| `tenant_isolation` | two rows for `device_1` under `tenant_a` (1) and `tenant_b` (2) |
