# Splitting wide tables

**In plain terms:** a single table with hundreds of feature columns is hard to own, review and
refresh. featlas can propose several ways to cut it into smaller tables — by business domain,
by owning business unit, by "family" of similar columns, and so on — and scores each proposal.
The score is only a starting point: the choice depends on who is responsible for what, who
reads the data and how fresh it must be, so a person (or an agent following the documented
criteria) makes the decision and writes down why.

## Step 1: measure (`split-analyze`)

```sh
featlas-reverse split-analyze spec.yaml --feature-set customer_wide \
  --table customer_wide.table.json [--max-groups 6] [--min-size 2] > split-analysis.json
```

`split.analyze_split` profiles every feature (legacy column, kind, operation, column tags,
owner, sources, value and filter columns, lookback, dependencies) and evaluates these
strategies:

| Strategy | Groups features by |
| --- | --- |
| `tag:<name>` | a column tag on the legacy table (`domain`, `business_unit`, `owner`, `classification`…), one strategy per tag present |
| `filter_family` | the filter columns they differ by (for example every `channel` split) |
| `value_family` | the aggregated source column |
| `window` | lookback |
| `source` | the set of source tables read |

Derived (expression) features that have no value for the attribute follow their first dependency.

### Metrics per strategy

| Metric | Meaning |
| --- | --- |
| `groups`, `sizes`, `smallest`, `largest`, `singletons` | how many tables and how balanced |
| `unassigned` | features lacking the attribute (for example no `domain` tag) |
| `cohesion` | mean Jaccard similarity of the value and family-filter columns of feature pairs inside a group (0–1) |
| `single_owner_groups` | groups whose features share one business unit (or owner) |
| `cross_set_dependencies`, `recomputed` | derived features whose inputs land in another group |

### The heuristic score (`split._score`)

A strategy that does not split (fewer than 2 groups) scores −10⁹. Otherwise, starting from 0:
−2 per group above `max_groups`; −3 per unassigned feature; −1.5 per group smaller than
`min_size`; −0.25 per cross-set dependency; then + 2 × `cohesion` + 1.5 × (`single_owner_groups` /
`groups`). Strategies are sorted by score and `recommended` is the best one that splits. The
output also states: "The recommendation is a heuristic over the metrics; the agent must confirm
it against ownership, consumers and SLAs (see the split skill) and call apply_split."

## Step 2: decide

The decision framework from `.claude/skills/featlas-split-wide-table`, weighed **in this order**,
with what was found recorded for each:

1. **Hard boundaries first.** Columns with different `classification` (PII/restricted vs
   internal) or residency must not share a table, whatever the metrics say. That alone can force
   a split by classification, then a second dimension inside each part.
2. **Accountability.** If business units own features and are on call for them, split by
   `business_unit` so each table has one owner (`single_owner_groups == groups`). This usually
   wins in organisations with federated ownership.
3. **Consumption.** Prefer groups that consumers read together, so a model joins as few tables
   as possible. On Databricks, read-only query history (`system.access.column_lineage`,
   `system.query.history`) is the evidence; record it.
4. **Semantic cohesion (domain).** When ownership is central and consumers read broadly, split
   by `domain` (or `filter_family`/`value_family` when there are no tags): features about the same
   business concept change together, which keeps reviews and versioning local.
5. **Operational shape.** Different freshness/SLA or very different lookbacks (`window`) →
   separate tables, so a slow, wide window does not hold back a fresh one.
6. **Size and balance.** Avoid singletons and tables over ~50 features unless one of the above
   demands it; stay within `max_groups` unless there is a reason.
7. **Dependencies.** Keep `cross_set_dependencies` low (move a ratio next to its numerator and
   denominator when that does not violate 1–3). If it must cross, it is still correct.

Strategies can be combined deliberately (for example business unit, then domain inside a large
unit) by editing the assignment: any `{feature_id: group}` map is valid input to `split-apply`.
A choice that changes ownership or on-call responsibility is an organisational decision: ask a
human. The skill's deliverable is a `split-decision.md` with the candidates and metrics, the
choice, the reasons by criterion, rejected alternatives and open questions.

### Domain vs business unit in the worked example

From `examples/reverse/generated/split-analysis.json`:

| Strategy | Score | Groups (sizes) | Cohesion | Single-owner groups | Cross-set deps |
| --- | --- | --- | --- | --- | --- |
| `tag:domain` | 1.888 | 6 (2, 2, 2, 2, 3, 4) | 0.444 | 6/6 | 2 |
| `filter_family` | 1.57 | 4 (2, 2, 2, 9) | 0.66 | 2/4 | 2 |
| `tag:business_unit` | 1.35 | 4 (2, 3, 4, 6) | 0.175 | 4/4 | 2 |
| `value_family` | −0.57 | 5 (1, 1, 2, 3, 8) | 0.615 | 4/5 | 0 |
| `window` | −10⁹ | 1 | — | — | — ("does not split") |
| `source` | −10⁹ | 1 | — | — | — ("does not split") |

Both tag strategies give every group a single business unit. `tag:domain` scores higher here
because its groups are more cohesive (0.444 vs 0.175: `business_unit: finance` lumps revenue and
payment columns together). Under criterion 2, an organisation whose business units are on call
for their features would still choose `tag:business_unit` (4 tables: commerce, finance,
marketing, operations); with central ownership, criterion 4 favours `tag:domain` (6 tables:
channel, fulfilment, payments, purchasing, returns, revenue). The example applies `tag:domain`.

## Cross-table dependencies are recomputed, not read

A derived feature may land in a different table than its inputs (in the example,
`return_rate_30d` goes to `returns` while `orders_30d` is in `purchasing`; `web_share_30d` goes to
`channel`). The split keeps the dependency: the generated code recomputes such inputs from the
same sources and `as_of` inside the dependent table's job — no cross-table read and no skew
between tables. Each crossing edge is counted in `cross_set_dependencies` because it duplicates
compute. You can see it in the generated code: `fe-spec generate
examples/reverse/generated/customer_wide.split.yaml --deployment customer_wide_databricks`
writes one directory per table, and `databricks/customer_wide_returns/script.sql` defines the
views `fl_feature_customer_orders_30d`, `fl_feature_customer_returns_30d` and
`fl_feature_customer_return_rate_30d` — `orders_30d` is computed again there, while only
`returns_30d` and `return_rate_30d` are published to that table. `split-apply --colocate-dependencies` (`colocate_dependencies=True`) instead rejects any
assignment that separates a feature from its inputs ("a derived feature must live with its
inputs") (`split.py` docstring and `apply_split`).

## Step 3: apply (`split-apply`)

```sh
featlas-reverse split-apply spec.yaml --feature-set customer_wide \
  --assignment split-analysis.json --strategy tag:domain \
  --out split.yaml --mapping split-mapping.json
```

`--assignment` is either a `{feature_id: group}` JSON or a `split-analyze` output plus
`--strategy`. `apply_split` (the MCP tool also accepts `tables={group: "cat.sch.table"}`):

* replaces the feature set by one per group (`<feature_set>_<group>`), each with its own output
  target (default `<legacy target>_<group>`), consumer (`<set>_batch`) and consumer binding, and
  write permissions per new set;
* returns the mapping legacy column → new table, column, state column, feature and group;
* returns a **compatibility view** `<target>_compat` that reassembles the wide shape with
  `FULL OUTER JOIN`s on the entity keys and `feature_as_of`, for consumers that cannot move yet;
* validates the new spec with the core (the CLI exits 1 if it fails).
