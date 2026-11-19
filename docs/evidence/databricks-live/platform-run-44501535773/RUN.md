# Live platform run 44501535773 (Databricks Free Edition)

`workflow_dispatch` run of `.github/workflows/databricks-live.yml` (job `platform`) at `394bb06`;
artifact `databricks-platform-evidence` downloaded unchanged (zip SHA-256
`d65ebb5ec16d8ffef7b3fa4d0821c975202c4e1bbff1c097a04e7934e6dafded`). Same tested sources as the run's main
suite (pinned as run 44501535773) and as run 44488112120. **3 passed** in 1011.1 s. Platform tables carry the suffix
`44501535773p`, separate from the main suite's.

| Test | Result | Observation |
| --- | --- | --- |
| `test_scheduled_job` | **passed** | The generated job (`databricks/job.yml`, serverless environment client 3, wheel and project in a Unity Catalog volume) was created paused through the Jobs API (job timeout 900 s from the SLO). An on-demand run (`ONE_TIME`) and a run fired by the job's own 5-minute schedule (`PERIODIC`) both succeeded and published `fault_job/device_reliability/2026-11-19T10:00:00Z` and `…T10:10:00Z` (2 rows each, every row with observed `published_at`). The job's last slot equals an interactive recomputation of that slot (0 diffs). Job deleted. |
| `test_online_publication` | **passed** | `fault_online` staged 2 rows with one MERGE (commit 10:12:08Z); `publish_table` synced them to the Online Feature Store `featlas-free` (CU_1, AVAILABLE); a FeatureSpec and the Feature Serving endpoint `featlas-dr-44501535773p` were created; the endpoint returned publication `online-44501535773p` for both keys on the first poll and `published_at` = 2026-11-19T10:19:33.557561Z was recorded. Served values equal the staged ones. Endpoint, FeatureSpec, online table and store were removed. |
| `test_feature_views_parity` | **passed** | The five mapped features equal featlas' snapshot (0 mismatches), as in platform runs 42993010899 and 44488112120. |

Earlier on the same sources: run 44494933029 (at `3abb651`) also passed the main suite, the scheduled
job (slots 09:20Z and 09:25Z, 0 diffs against the interactive run) and Feature Views; its online test
failed when the new serving endpoint reached `UPDATE_FAILED` without a recorded reason (the snapshot
stayed staged, `published_at` NULL). That run is not pinned; the test now records the endpoint state
and served-entity logs on failure.

Client: `databricks-connect==17.3.16`, `databricks-sdk==0.169.0`, `databricks-feature-engineering==0.18.4`; runtime `19.15.x-aarch64-photon-scala2.13`.
