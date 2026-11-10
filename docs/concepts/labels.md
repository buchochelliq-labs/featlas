# Labels

**In plain terms:** a training label looks *forward* from the prediction moment (for example
"how much did this customer spend in the 7 days after *T*?"). featlas builds such labels only
when the whole future window has actually passed and the data for it is known to be complete;
otherwise the row is left out (or the run fails, if the consumer says so). A missing future is
never treated as a zero or a negative class.

## Contract (SEM-LABEL-001/002)

A consumer may declare a `label`: its `source`, `value_column`, `entity_keys`,
`event_time_column`, `available_time_column`, `horizon_seconds` with `lower`/`upper` bounds,
`availability_policy`, `aggregation` (`any_true`, `count`, `sum`, `max`, `latest`), `empty`,
`full_horizon_required` and `incomplete_horizon` (`exclude` or `error`); the consumer's
`time_inputs` must include a `dataset_cutoff`. featlas requires the label's
`available_time_column` to equal the source's declared availability column
(`LABEL_AVAILABILITY_COLUMN`, SPEC_FINDINGS #6).

## Two steps

`crates/featlas-core/src/label.rs` splits construction in two:

1. **`label_requests`** (no label data is read): for each request row it computes the horizon
   end `as_of + horizon_seconds`, requires maturity (horizon end not after the row's
   `dataset_cutoff`) and positive completeness evidence for the label source over the horizon.
   Rows that fail are `excluded` with a reason (immature, censored coverage, unavailable source);
   with `incomplete_horizon: error` the first incomplete request in request order fails. The
   rest are `eligible`.
2. **values** for eligible requests: the label source is reconstructed as known at each distinct
   dataset cutoff (availability ≤ cutoff, duplicates, revisions, tombstones), the horizon is
   applied to event time, and the aggregation computed. Only a known-complete empty horizon can
   use `label.empty`. Feature filters are never applied to labels.

`build_labels` (CLI `fe-spec label`, Python `Project.labels`) does both locally;
`Project.label_requests(consumer, rows, source_status)` exposes step 1 alone for hosts.

## On Spark

For each label consumer of a planned feature set, `fe-spec generate` also writes
`databricks/labels/<consumer>/artifacts.json` (`databricks.rs::lower_labels`). For
`projects/03_commerce_ratio.yaml` / `commerce_prod` that is the consumer
`spend_training_dataset` (sum of `amount_usd_cents` over `[as_of, as_of + 7 days)`):

* `requests_view` `fl_label_requests` with columns `fl_request_index`, the entity keys,
  `fl_as_of`, `fl_dataset_cutoff`, `fl_horizon_end`;
* `session`, `bind_sources`, reconstruction `stages` and zero-row `checks` for the label source,
  bound to `:as_of` = the current dataset cutoff;
* a `values` query; row-level failures (for example `NUMERIC_OVERFLOW`, `LABEL_TIE` for `latest`)
  are returned as data in `fl_error` so the host can report them in request order, as the core
  does;
* `notes`: "Maturity, completeness and exclusion are decided by the core (label_requests); this
  SQL computes values for eligible requests only." and "Feature filters are never applied to
  labels; the label source is reconstructed independently at each dataset cutoff."

Excerpt of the generated `values` query:

```sql
WITH req AS (SELECT * FROM fl_label_requests WHERE fl_dataset_cutoff = :as_of),
e AS (
  SELECT r.fl_request_index AS fl_ri, s.`amount_usd_cents` AS fl_v, 1 AS fl_present
  FROM req r JOIN fl_purchase_events_ai_selected s ON s.`tenant_id` = r.`tenant_id` AND s.`customer_id` = r.`customer_id`
   AND s.`purchased_at` >= r.fl_as_of AND s.`purchased_at` < r.fl_horizon_end
)
SELECT r.fl_request_index AS request_index, count(e.fl_present) AS eligible_rows,
CASE WHEN count(e.fl_present) = 0 THEN 'empty' ELSE 'ok' END AS fl_state,
...
```

Integer sums accumulate as `DECIMAL(38,0)` and overflow becomes an error, never a wrapped value.

The host `python/featlas_databricks/labels.py::LabelRun(spark, label_artifacts, requests,
source_relations=None)` takes the core's `label_requests` result, registers the eligible
requests, re-runs the reconstruction stages and checks per distinct cutoff, reads the values and
returns the core's `LabelDataset` shape (`rows` with value, state and `eligible_rows`, and the
core's `excluded`).

The capability is `core.label_construction` on the compute role; the Databricks adapter now lists
it among its lowered capabilities, so it reports `unverified`/`static` until a live run covers it
([Status](../status.md)).

## Goldens

`projects/labels/spend_training_dataset.json` holds hand-reasoned cases shared by
`crates/featlas-core/tests/labels.rs` and the Spark differential suite
(`test_generated_label_sql_matches_goldens_and_core`, which requires Spark to equal both the
goldens and the core):

| Case | Expected |
| --- | --- |
| `mature_horizon_sums_every_status` | value 70, `ok`, 2 eligible rows |
| `cutoffs_reconstruct_independently` | three rows (70, 1070, 7), each cutoff reconstructed separately |
| `immature_excluded_exact_maturity_labelled` | request 0 excluded "immature"; request 1 labelled 70 |
| `censored_coverage_excluded` | excluded "censored" |
| `known_complete_empty_horizon` | value 0, `empty` |
| `unavailable_source_is_not_negative` | excluded "unavailable" |
| `conflicting_revisions_fail` | `DUPLICATE_CONFLICT` |
