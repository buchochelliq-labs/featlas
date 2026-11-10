---
name: featlas-split-wide-table
description: Decide how to split one very wide feature table (dozens to hundreds of columns) into several smaller featlas feature tables, choose the split dimension (domain, business unit, filter family, freshness, consumers...) with an explicit decision record, and produce the split specification. Use after featlas-reverse-engineer, or on any featlas spec whose feature set is too wide.
---

# Split a wide feature table

Input: a spec whose feature set mirrors the wide table (from `featlas-reverse-engineer`) and,
ideally, the legacy table description with column tags. Output: the split spec, the legacy column →
new table mapping, a compatibility view, and a **decision record** explaining the chosen split.

Tools (read-only): `split_analyze` and `split_apply` on the `featlas-reverse` MCP server, or
`featlas-reverse split-analyze` / `split-apply`.

## 1. Measure

`split_analyze(document, feature_set, legacy_table, max_groups, min_size)` evaluates every
available strategy and returns per strategy its groups and metrics:

| metric | meaning |
| --- | --- |
| `groups`, `sizes`, `singletons` | how many tables, how balanced |
| `unassigned` | features lacking the attribute (e.g. no `domain` tag) |
| `cohesion` | mean similarity of the source columns used inside a group (0–1) |
| `single_owner_groups` | groups whose features share one business unit/owner |
| `cross_set_dependencies` | derived features whose inputs live in another table; their inputs are recomputed in the dependent table's job (correct, but duplicated compute) |

Strategies: `tag:<name>` for each column tag on the legacy table (`domain`, `business_unit`,
`owner`, `classification`...), `filter_family` (features differing only by a filter, e.g. every
`channel`), `value_family` (same aggregated column), `window` (lookback), `source` (tables read).
`recommended` is a heuristic over those metrics. It is a starting point, not the decision.

## 2. Decide (you, with evidence)

Weigh, in this order, and write down what you found for each:

1. **Hard boundaries first.** Columns with different `classification` (PII/restricted vs internal)
   or residency must not share a table, whatever else the metrics say; that alone can force a split
   by classification, then a second dimension inside each.
2. **Accountability.** Who answers for a feature when it breaks? If business units own features and
   are on call for them, split by `business_unit` so each table has one owner
   (`single_owner_groups == groups`). This usually wins for organisations with federated ownership.
3. **Consumption.** Which models/dashboards read which columns? If you can read query history
   (`system.access.column_lineage` / `system.query.history` on Databricks, read-only), prefer groups
   that consumers read together: a model should join as few tables as possible. Record the evidence.
4. **Semantic cohesion (domain).** When ownership is central and consumers read broadly, split by
   `domain` (or `filter_family`/`value_family` when there are no tags): features about the same
   business concept change together, which keeps reviews and versioning local.
5. **Operational shape.** Different freshness/SLA or very different lookbacks (`window`) → separate
   tables so a slow, wide window does not hold back a fresh one.
6. **Size and balance.** Avoid singletons and tables over ~50 features unless one of the above
   demands it; stay within `max_groups` unless there is a reason.
7. **Dependencies.** Keep `cross_set_dependencies` low: prefer moving a ratio next to its
   numerator/denominator when it does not violate 1–3. If it must cross, it is still correct.

If two strategies are close, combine them deliberately (e.g. business unit, then domain inside a
large unit) by editing the assignment: any `{feature_id: group}` map is valid input to
`split_apply`. Ask a human when the choice changes ownership or on-call responsibility; that is an
organisational decision, not a technical one.

## 3. Apply and check

`split_apply(document, feature_set, assignment, tables={group: "cat.sch.table"})` returns the new
spec (one feature set, output table and consumer per group), the mapping, a compatibility view
(`<legacy>_featlas_compat`) that reassembles the wide shape for consumers that cannot move yet, and
validation (must pass, fixtures `failed == 0`). Use `colocate_dependencies=true` if your platform
policy forbids recomputing inputs in another job.

## 4. Decision record

Write `split-decision.md`: the candidate strategies with their metrics (a table), the chosen one,
the reasons by criterion 1–7, rejected alternatives and why, open questions for owners, and the
mapping. Continue with `featlas-regenerate` to build and verify the new tables.
