# Live platform run 42993010899 (Databricks Free Edition)

`workflow_dispatch` run of `.github/workflows/databricks-live.yml` (job `platform`) at
`af13da7`; artifact `databricks-platform-evidence` downloaded unchanged. Tested sources
(`sources.sha256`) are those of pinned run 42984352013. No credentials, hosts or workspace
identifiers are included (the evidence holds one Databricks request id).

| Test | Result | Observation |
| --- | --- | --- |
| `test_feature_views_parity` | **passed** | The five features the adapter maps to Feature Views (`Count`, filtered `Sum`, `Max`, `Avg`, `Last`, all `RollingWindow` 1 h) were computed by `create_training_set` at `as_of` 2026-06-01T12:00:00Z and equal featlas' own snapshot on the same tables for all 3 entities: 0 mismatches. A device with an empty window got `count = 0` from the Feature View (the spec's declared empty value); a latest event with a null value gave `null` for `Last`, as the spec's lookup does. Distinct count, recency and the derived ratio stayed on the feature table with their reasons. |
| `test_online_publication` | failed — grant | Staging MERGE, `create_online_store`, `publish_table` (online table) and their deletion succeeded; `create_feature_spec` was refused: `Principal 'featlas' does not have CREATE FUNCTION on Schema 'featlas.synthetic_features'.` Online visibility was therefore not observed. |
| `test_scheduled_job` | failed — grant | `PERMISSION_DENIED: Principal 'featlas' does not have CREATE VOLUME on Schema 'featlas.synthetic_raw'.` The job was not created. |

Client: `databricks-connect==17.3.15`, `databricks-sdk==0.165.0`,
`databricks-feature-engineering==0.18.3`, `mlflow-skinny==3.18.0`. OAuth scopes requested:
`databricks-connect jobs files unity-catalog pipelines model-serving model-serving-inference
mlflow`; the Feature Engineering client received a bearer token minted from those scoped
credentials (`mlflow_creds_patched` in the report).
