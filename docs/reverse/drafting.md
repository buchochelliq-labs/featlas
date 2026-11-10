# Drafting a spec from a legacy table

**In plain terms:** you give featlas the SQL that builds the old table and a short JSON
description of each table involved. It works out the entity (for example "customer"), the time
window, the filters and each column's calculation, and writes a spec. Things it cannot know for
sure — which table lists all customers, which column says when a row arrived — become
*assumptions* (it picked a default) or *questions* (it needs you). You answer and run it again.

## Inputs

### The SQL

`python/featlas_reverse/analyze.py` parses one `SELECT` with sqlglot (Databricks dialect). The
supported shape is the usual batch feature table:

```sql
SELECT <entity keys>, [<as_of>,] <aggregate or expression> AS <feature>, ...
FROM <fact table> [alias] [JOIN <dimension> ON <equi-join>]
WHERE <event time> >= <anchor> - INTERVAL n <unit> AND <event time> < <anchor> [AND <filters>]
GROUP BY <entity keys>
```

* Aggregates: `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`, `SUM`, `AVG`, `MIN`, `MAX`.
* Filters per column: `FILTER (WHERE c)`, `CASE WHEN c THEN x END`, or shared `WHERE` conjuncts.
* Derived outputs that combine other outputs (`a / b`, `a / NULLIF(b, 0)`, `COALESCE(...)`)
  become expression features with dependencies.
* Interval units `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK` become elapsed seconds.
* Anything else (non-equi joins, `HAVING`, window functions, UDFs…) is returned in
  `unsupported` with its SQL fragment. The skill tells the agent to translate each by hand
  (relationships, `recency`/`lookup`, a learned/custom contract) or drop it with a reason — never
  leave a legacy column silently unmapped.

`featlas-reverse analyze <sql>` prints what was understood without drafting.

### Table descriptions (`featlas.table-description/v1`)

One JSON per table (`python/featlas_reverse/describe.py`):

```json
{"table": "main.sales.orders", "comment": "...", "owner": "sales-data",
 "primary_key": ["order_id"], "timeseries_key": null,
 "tags": {"domain": "sales"},
 "columns": [{"name": "order_id", "type": "string", "nullable": false,
              "comment": "...", "tags": {"domain": "sales", "business_unit": "retail"}}]}
```

Types are Spark SQL names, mapped to logical types by `describe.logical_type` (integral types and
`decimal(p,0)` → `integer`; other decimals, `double`, `float` → `number`; `string`/`varchar`/
`char` → `string`; `boolean`; `timestamp*`; `date`). On Databricks,
`describe.from_spark(spark, "cat.sch.tbl")` builds a description read-only from `DESCRIBE TABLE`,
`SHOW TBLPROPERTIES`, `SHOW CREATE TABLE` and `information_schema.column_tags`. Column **tags**
(`domain`, `business_unit`, `owner`, `classification`, `unit`) are what the
[split analysis](splitting.md) uses.

## Drafting

```sh
featlas-reverse draft --table wide.table.json --sql wide.sql \
  --source orders.table.json --source customers.table.json \
  [--decisions decisions.json] --out spec.yaml --report drafting-report.json
```

`draft.draft_spec` builds the project and a **report** with four parts:

| Part | Meaning |
| --- | --- |
| `assumptions` | Each default taken: `decision` (dotted key), `value`, `why` |
| `questions` | Each gap: `decision`, `question`, `options`, `blocking` |
| `unsupported` | SQL the analyzer did not translate |
| `mapping` | Every legacy column → feature id, version and the legacy SQL |

A blocking question stops the draft (no spec is written; the CLI exits 2). Otherwise the spec is
written and validated with the core; the CLI prints a one-line summary and exits 1 if validation
fails.

### What is decided, and how

| Decision | Default rule (`draft.py`) | Blocking if missing? |
| --- | --- | --- |
| `entity.keys` | the `GROUP BY` columns | yes (no `GROUP BY`) |
| `entity.id` | most specific key without `_id` | — |
| `entity.tenant_key` | a key matching `tenant`/`org`/`account` (null = single-tenant) | — |
| `window` / `window.lookback` | the `WHERE` window | yes (no window, or no lower bound) |
| `population.table` | a provided table whose primary key equals the entity keys, preferring registry-like names (`customers`, `accounts`, `dim_`…) | yes if none — "The fact table cannot be the population: it has many rows per entity." |
| `population.event_time` | a column like `registered_at`, `created_at`, `signup_ts`… | — |
| `sources.<table>.available_time` | a column like `available_at`, `ingested_at`, `_loaded_at`, `_commit_timestamp`… | no: falls back to the event time with the assumption "No availability column: rows assumed visible at their event time (NOT point-in-time safe)" and a non-blocking question |
| `missingness.empty` (derived) | `0` for `count`, `count_distinct` and `COALESCE(SUM(...), 0)`; otherwise `null` (derived expressions: `null`) | — (the core warns `EMPTY_IDENTITY_REVIEW` for a sum/mean/min/max empty value) |
| `earliest_as_of` | `2026-01-01T00:00:00Z`, also used as the sources' `earliest_reproducible_at` | — |
| `target.table` | `<legacy>_featlas`, next to the legacy table | — |
| `samples` | none | no: a non-blocking question asks for characterization samples |

Answers go in `decisions` (a JSON object, `--decisions` on the CLI). The keys the drafter reads
(`draft.py`):

| Key | Example |
| --- | --- |
| `population.table`, `population.event_time` | `{"population": {"table": "main.crm.customers"}}` |
| `sources[<fully qualified table>].available_time` / `.primary_key` | `{"sources": {"main.sales.orders": {"available_time": "ingested_at"}}}` |
| `project.id`, `project.owner`, `entity.id`, `entity.tenant_key`, `feature_set.id`, `target.table`, `deployment.id`, `deployment.environment`, `earliest_as_of` | nested objects following the dotted key, e.g. `{"entity": {"tenant_key": null}}` |
| `samples` | characterization samples ([next page](characterization.md)) |
| `structural_as_of` | `as_of` of the structural cases (default `2026-06-01T00:00:00Z`) |

`missingness.empty` is derived from the SQL and is not a decision key: review the
`EMPTY_IDENTITY_REVIEW` warnings and edit the spec if "no rows" does not mean 0 for the business.

!!! warning "Availability time is the assumption that matters most"
    If no ingestion/availability column exists, the spec — like the legacy code — is not
    point-in-time safe: late rows leak into history and replay is not reproducible. The skill
    asks the agent to say so prominently and look for an ingestion timestamp (`_ingested_at`,
    CDF `_commit_timestamp`, a landing-zone column).

### What the spec contains

For each output column: a feature with the declared input columns, the window, availability
cutoff `as_of` (inclusive), the transform (filters combined with `and`), output type, missingness,
lineage roles, `history` with `earliest_as_of`, and a structural acceptance case (empty population
→ no rows). Sources get `quality.completeness.mode: external_status` ("an empty table proves no
coverage"). The deployment binds the legacy source tables read-only and targets the new table.
YAML is written in the restricted profile: no anchors, strings quoted where YAML 1.1 would retype
them (`draft.py::to_yaml`).

See the [worked example](example.md) for real report entries and an excerpt of the drafted spec.
