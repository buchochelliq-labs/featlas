# Characterization tests

**In plain terms:** to check that the new spec means the same thing as the old table, we treat
the old table as a second, independent implementation. We take a handful of real (or anonymised)
customers at one point in time, note what the old table says about them, and turn those numbers
into test cases for the spec. If the spec disagrees, that is a finding to explain — often the old
table was wrong — never something to "tune away".

## Why

A drafted spec only has structural cases (empty population → no rows), which prove little. The
specification requires goldens that are independently derived, not produced by the algorithm
under test (SEM-TEST-002). The legacy SQL is exactly such an independent implementation
(`draft.py` and `characterize.py` docstrings).

## Producing samples: `characterize.legacy_sample`

```python
from featlas_reverse.analyze import analyze_sql
from featlas_reverse.characterize import legacy_sample

rows = legacy_sample(spark, analysis, views={"main.sales.orders": "sample_orders",
                                             "main.crm.customers": "sample_customers"},
                     as_of="2026-06-01T00:00:00Z")
```

`legacy_sample` runs the **legacy SQL unchanged** in a Spark session, with its table names
rebound to temp views holding the sampled source rows, and the `:as_of` parameter bound. It also
runs one companion query built from the same `FROM`/`WHERE`/`GROUP BY`
(`characterize.eligibility_sql`) that counts, per aggregate column, the rows its filter keeps.
That count decides each expected state — `empty` when no row qualifies, else `ok` — from the
legacy code itself, independently of the featlas core (`fl_state_<column>` in each row).

Only entities present in the legacy output are sampled: legacy tables usually omit entities with
no events, whereas the spec's population includes them.

## Feeding them to the drafter

```json
{"samples": [{
  "as_of": "2026-06-01T00:00:00Z",
  "coverage_end": "2026-06-02T00:00:00Z",
  "source_rows": {"customers": [...], "orders": [...]},
  "legacy_rows": [...]
}]}
```

`source_rows` is keyed by the spec's logical source ids (population and fact source). For each
feature, the drafter writes a case `<column>_characterization_<i>` with the sampled population,
source rows, source status `[earliest_as_of, coverage_end)`, and expected rows (entity keys, the
legacy value, the derived state). For a derived feature the state follows from its inputs: it is
asserted only when every input was `ok`; otherwise the feature is left out of that case and the
report records the assumption `samples[i].<column> = omitted` ("Expected state is not derivable
from the legacy code for every sampled entity").

The skill asks for 3–20 entities covering the interesting paths (zero activity in one filter
branch, nulls, boundary timestamps, several tenants) and says to put only synthetic or anonymised
rows in the spec — never copy personal data into a repository.

## When a characterization case fails

From `.claude/skills/featlas-reverse-engineer`, the usual causes, each of which goes into the
review note:

* **Late-arriving rows** the legacy code read although they arrived after `as_of` (the legacy
  table is not point-in-time correct; the spec is). Confirm by checking the availability column.
* **Entities without events** that the legacy table omitted (the spec's population keeps them,
  with the `empty` value).
* **Duplicates or corrections** the legacy code double-counted (the spec rejects or collapses
  them).

Do not change goldens to match the spec, and do not change the spec to reproduce a legacy bug
unless a human explicitly decides the bug is the intended behaviour.

## The late-arrival divergence, demonstrated

`python/tests/test_reverse.py::test_late_arrival_is_a_reported_legacy_divergence` adds one order
for `tenant_b`/`c1` with `ordered_at = 2026-05-20T00:00:00Z` (inside the 30-day window) but
`ingested_at = 2026-06-03T00:00:00Z` (after `as_of = 2026-06-01`). The legacy SQL filters only on
`ordered_at`, so it counts the order; the spec's availability cutoff (`ingested_at <= as_of`)
excludes it. The test asserts that the characterization cases then **fail**, including
`orders_30d` and `store_orders_30d`, and that only `tenant_b` diverges. That failure is the
intended evidence that the legacy table leaked the future.
