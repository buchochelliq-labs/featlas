# Stores and feature APIs

**In plain terms:** featlas computes a feature set once, the same way everywhere, and can then
put the results in different places: a Unity Catalog feature table (the default), Databricks
Feature Views (for the features that fit them exactly), a Databricks Online Feature Store for
low-latency serving, or Feast, an open-source feature store you can run on a laptop. Where a
store cannot keep a guarantee the specification makes, featlas says so in the capability report
instead of quietly doing something different.

| Surface | Adapter / configuration | Who computes | Publication evidence | Replay | Decision |
| --- | --- | --- | --- | --- | --- |
| Unity Catalog feature table | `dev.featlas:databricks`, `feature_api: uc_feature_tables`, target role `offline_store` | generated Spark SQL | Delta commit of the one MERGE (change data feed) | yes | D3, D9 |
| Feature Views | same adapter, `feature_api: feature_views` | Databricks, for mapped features only; featlas for the rest | not represented by Feature Views (featlas' table keeps it) | via the feature table | D16 |
| Online Feature Store + Feature Serving | same adapter, target role `online_store`, configuration `online_store` | generated Spark SQL | first observation of the publication at the serving endpoint | no (offline deployment) | D15 |
| Feast offline store | `dev.featlas:feast`, target role `offline_store` | Spark SQL lowering | first read-back through Feast | no | D17 |
| Feast online store | `dev.featlas:feast`, target role `online_store` | Spark SQL lowering | `get_online_features` after `materialize` | no | D17 |

The single-surface rule of the specification (SEM-PORT-005) applies: a deployment publishes one
surface. Serving offline history and online values means two deployments of the same logical
contract (see `projects/live/platform.yaml`: `fault_prod` offline, `fault_online` online).

## Unity Catalog feature tables (default)

Described in the [Databricks guide](databricks.md#publication-protocol): one MERGE per
publication into a table whose primary key is the entity keys, `as_of TIMESERIES` and the
publication sequence, so every publication is kept and historical serving replay is exact.

## Feature Views (`feature_api: feature_views`)

`fe-spec generate` (or `featlas.Project.generate`) writes
`databricks/<feature_set>/feature_views.json` (`featlas.databricks.feature-views/v1`). For each
member feature it contains either an exact definition or the reasons the feature stays on the
feature table:

```json
{"feature": "device.fault_severity_1h", "api": "feature_views", "reasons": [], "assumptions": [],
 "definition": {"name": "fault_severity_1h",
   "source": {"catalog_name": "featlas", "schema_name": "raw", "table_name": "device_events",
              "filter_condition": "(`is_fault` = true)"},
   "entity": ["tenant_id", "device_id"], "timeseries_column": "occurred_at",
   "function": {"operator": "Sum", "input": "severity"},
   "time_window": {"type": "rolling", "window_duration_seconds": 3600},
   "empty_window": {"spec": "null"}}}
```

Mapping rules (all must hold; each failure is listed in `reasons`):

| Rule | Why |
| --- | --- |
| One input, no joins | Feature Views do not join tables |
| Source `append_only`, no tombstones | Feature Views read rows as stored; featlas reconstructs revisions |
| Source availability time = event time | Feature Views select rows by event time only; a separate availability column means late rows would be counted at `as_of` |
| Window `[as_of - lookback, as_of)` | `RollingWindow` is lower-inclusive, upper-exclusive |
| `count` (of a non-nullable primary-key column), `sum`, `mean`, `min`, `max` over a plain column, nulls ignored | the Feature View functions with identical semantics; `count_distinct` has only `ApproxCountDistinct` |
| Lookup ordered by the event time, one key | `Last` / `First`; ties: the spec fails, Feature Views pick one row (recorded assumption) |
| Filter lowerable to SQL on the same input | `filter_condition` |
| Entity keys not DATE/TIMESTAMP | Feature View entity restriction |

Recency, derived expressions and custom/learned features never map. Feature states
(`ok`/`empty`/...), quality policies and publication evidence are not represented by Feature
Views; they stay with featlas. `featlas_databricks.feature_views` builds the mapped features
(`build`), evaluates them at an `as_of` with `create_training_set` (`training_values`) and
compares them with featlas' snapshot (`compare`); registering them in Unity Catalog
(`register`) is resource creation and needs authorization. The live platform suite runs this
comparison (`tests/test_databricks_platform.py::test_feature_views_parity`).

## Online Feature Store and Feature Serving (role `online_store`)

Deployment shape (from `projects/live/platform.yaml`, deployment `fault_online`):

```yaml
adapters:
- id: databricks_platform
  implementation: dev.featlas:databricks
  version: 0.1.0
  roles: [compute, catalog, online_store]
  configuration:
    connection_profile: example_databricks
    feature_api: uc_feature_tables
    online_store: {name: featlas-free, capacity: CU_1, serving_endpoint: featlas-device-reliability}
output_targets:
- feature_set: device_reliability
  role: online_store
  location: {kind: table, namespace: [featlas, synthetic_features], name: device_reliability_online_src, format: delta}
publication: {surface: online_store, ...}
consumer_bindings:
- {consumer: reliability_batch, version: 1.0.0, adapter: databricks_platform, role: online_store}
```

Publication, step by step:

1. `SnapshotRun.publish` stages the snapshot with one MERGE into the staging table (primary key
   entity keys + `as_of TIMESERIES`): newer rows update, new entities insert, entities missing
   from a newer complete snapshot are deleted. A retry changes nothing; an older snapshot is
   refused before computing (`SNAPSHOT_SUPERSEDED`). The evidence it returns says
   `visibility: staged_for_online_store`; `published_at` stays NULL.
2. `OnlinePublisher.ensure_store` — the online store exists and is `AVAILABLE` (created only with
   `OnlineAuthorization(allow_create=True)`).
3. `OnlinePublisher.sync` — `publish_table(..., publish_mode="TRIGGERED")` creates the online
   table `<staging table>_online` on first use and syncs incrementally.
4. `OnlinePublisher.ensure_endpoint` — a FeatureSpec over the staging table (features, `as_of`,
   `publication_id`) and a Feature Serving endpoint (scale to zero).
5. `OnlinePublisher.observe` — query the endpoint for the published keys until every key returns
   this `publication_id`; that instant is written to the staged rows' `published_at`.

`featlas-databricks <project> --deployment fault_online --feature-set device_reliability
--publish ...` runs all five steps (`--online-sample` keys are observed). The job, the endpoint
and the store cost compute: delete test resources with `OnlinePublisher.teardown`.

## Feast (`dev.featlas:feast`)

Feast stores and retrieves feature values; featlas computes them (Spark SQL lowering). Install
the `feast` extra (`feast==0.67.0`).

```python
from featlas_databricks.core import Core
from featlas_databricks.runner import SnapshotRun
from featlas_feast import FeastStore

gen = Core().generate("projects/live/platform.yaml", "fault_feast_offline")
store = FeastStore(gen, "device_reliability", "./feast-repo")
store.apply()                                   # entities + feature view (TTL = consumer staleness)
rows = SnapshotRun(spark, gen["artifacts"]["device_reliability"], as_of, coverage, set(), relations,
                   population_source="device_registry").run_to_snapshot(publication)
store.publish_offline(rows, publication)        # atomic file + observed published_at
store.historical([{"tenant_id": "tenant_a", "device_id": "device_1", "as_of": as_of}])
```

| featlas guarantee | Feast mapping | Status |
| --- | --- | --- |
| Atomic snapshot publication (offline) | one parquet file per publication, `os.replace` into the source directory | supported (local evidence) |
| Observed `published_at` | first complete read through `get_historical_features`; `_publications.jsonl` | supported (local evidence) |
| Source-as-known reads | point-in-time join: latest `as_of` <= request, within TTL = `max_staleness_seconds` | equal to the core consumer API (tested) |
| Tie-break between equal `as_of` | not possible without a non-null created timestamp | prevented: one publication per `as_of` (`SAME_AS_OF_REPUBLICATION`) |
| Historical serving replay | point-in-time join cannot filter by visibility | `unsupported` (generation refuses a replay consumer) |
| Online publication | `materialize`, observed with `get_online_features` | supported (local evidence) |
| Atomic online publication | Feast writes entity rows in batches | `unverified` |

Evidence: `python/tests/test_local_feast.py` (local Spark, Feast file offline store, SQLite
online store).
