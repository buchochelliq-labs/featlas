---
name: featlas-reverse-engineer
description: Turn an existing feature table and the code that builds it into a featlas specification (Feature Engineering Specification 0.2.0) with characterization tests. Use when asked to document, port, migrate, re-platform or "spec out" a legacy feature table, or as the first step before splitting a wide feature table or regenerating tables from a spec.
---

# Reverse-engineer a feature table into a featlas spec

You produce three artifacts: `spec.yaml` (validates and passes its fixtures), `drafting-report.json`
(every assumption, open question and unsupported construct), and a short review note for a human.
A spec with unreviewed assumptions stays `metadata.status: draft`.

Tools: the `featlas-reverse` MCP server (`reverse_draft_spec`, `spec_validate`, `reverse_analyze_sql`)
or the same operations on the CLI (`featlas-reverse draft ...`). Both are read-only. You run SQL
against the warehouse only through your own authorized connection, and only read-only statements.

## 1. Gather inputs (read-only)

1. **The code.** The SQL (or notebook cell) that builds the table. If it is PySpark, translate the
   DataFrame pipeline into one equivalent Spark SQL `SELECT` first and keep the original next to it;
   say in the report that you translated it. Simple CTEs (`SELECT columns/expressions/* FROM
   table [WHERE ...]`, chained or not) are inlined by the analyzer. Other CTEs and subqueries
   (with GROUP BY, joins, window functions) you must inline into one
   `SELECT ... FROM fact [JOIN dim] WHERE window AND filters GROUP BY keys` where that is
   semantics-preserving; otherwise split the table into the parts that are.
2. **Table descriptions** (`featlas.table-description/v1`, see `python/featlas_reverse/describe.py`)
   for the feature table and every table the code reads: columns with types, comments, nullability,
   tags (`domain`, `business_unit`, `owner`, `classification`, `unit`), primary key. On Databricks,
   `featlas_reverse.describe.from_spark(spark, "cat.sch.tbl")` builds one from `DESCRIBE TABLE`,
   `SHOW TBLPROPERTIES`, `SHOW CREATE TABLE` and `information_schema.column_tags`. Tags matter: the
   split skill uses them.

## 2. Draft, then answer the questions

Call `reverse_draft_spec(target_table, sql, sources, decisions={})` (`reverse_analyze_sql` shows
what the analyzer understood without drafting).

* `report.questions` with `blocking: true` stop the draft (`project_yaml` is null). Typical ones:
  - **population table** — a table with one row per entity (its primary key equals the GROUP BY
    keys) and a registration time. The fact table can never be the population.
  - **primary key** of the fact table, **window** when the SQL has none.
* Non-blocking questions and every `report.assumptions` entry must be confirmed or overridden in
  `decisions` (same dotted keys), then draft again. Pay most attention to:
  - `sources.<table>.available_time` — the column saying when a row became visible. If the drafter
    had to fall back to the event time, the spec (like the legacy code) is **not point-in-time
    safe**: say so prominently and look for an ingestion timestamp (`_ingested_at`, CDF
    `_commit_timestamp`, a landing-zone column).
  - `missingness.empty` for `COALESCE(SUM(...), 0)` features (`EMPTY_IDENTITY_REVIEW` warnings):
    confirm that "no rows" really means 0 for the business.
  - `entity.tenant_key`, owners, `target.table` (default: `<legacy>_featlas`; never the legacy
    table itself).
* The analyzer already translates per-column windows (`FILTER (WHERE ts >= :as_of - INTERVAL 7
  DAYS)` inside a 30-day `WHERE`), recency (`TIMESTAMPDIFF(SECOND, MAX(ts), :as_of)`,
  `unix_timestamp` differences) and lookups (`MAX_BY`/`MIN_BY`). Their legacy-semantics notes are
  assumptions in the report: legacy `MAX_BY` ignores null ordering values and picks arbitrarily on
  ties, the spec rejects both; check the ordering column is non-null and unique per entity.
* `report.unsupported` lists SQL the analyzer did not translate (non-equi joins, HAVING, window
  functions, UDFs, `DATEDIFF` / non-second `TIMESTAMPDIFF`, windows that end before `as_of`...).
  For each: translate it by hand into the spec (relationships for joins, a learned/custom
  contract for UDFs), or drop the column and record why. Never leave a legacy column silently unmapped: every column appears in
  `report.mapping`.

## 3. Characterization tests (the legacy table as an independent implementation)

The structural cases (empty population → no rows) prove little. Add characterization samples:

1. Pick 3–20 entities that cover the interesting paths (zero activity in one filter branch, nulls,
   boundary timestamps, several tenants) and one `as_of` the legacy table holds.
2. Extract, read-only, their population rows and fact rows in `[as_of - lookback - margin, as_of]`.
3. Either read their legacy rows from the table, or recompute them by running the legacy SQL over the
   extracted rows: `featlas_reverse.characterize.legacy_sample(spark, analysis, views, as_of)`; it also
   derives each aggregate's expected state (`ok`/`empty`) from the legacy code itself.
4. Pass `decisions.samples = [{as_of, coverage_end, source_rows: {<population source id>: [...],
   <fact source id>: [...]}, legacy_rows: [...]}]` and draft again. Synthetic or anonymised rows only
   in the spec — never copy personal data into a repository.

`spec_validate` must then report `fixtures.failed == 0`. A failing characterization case is a
finding, not something to tune away. The usual causes, all of which go into the review note:

* **Late-arriving rows** the legacy code read although they arrived after `as_of` (legacy is not
  point-in-time correct; the spec is). Confirm by checking the availability column of the rows.
* **Entities without events** the legacy table omitted (the spec's population keeps them, with the
  `empty` value).
* **Duplicates/corrections** the legacy code double-counted (the spec rejects or collapses them).

Do not change goldens to match the spec; do not change the spec to reproduce a legacy bug unless a
human explicitly decides the bug is the intended behaviour.

## 4. Hand-off

Write `spec.yaml`, `drafting-report.json` and the review note (assumptions you made, questions a
human must answer, legacy divergences found, unsupported columns). Then, if asked, continue with
`featlas-split-wide-table` or `featlas-regenerate`.
