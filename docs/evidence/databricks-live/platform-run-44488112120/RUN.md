# Live platform run 44488112120 (Databricks Free Edition)

`workflow_dispatch` run of `.github/workflows/databricks-live.yml` (job `platform`) at `87bbfa4`;
artifact `databricks-platform-evidence` downloaded unchanged. Same tested sources as the run's main
suite (pinned as run 44488112120). Grants added for this run: `CREATE VOLUME`, `READ VOLUME`,
`WRITE VOLUME` on `featlas.synthetic_raw`, `CREATE FUNCTION` on `featlas.synthetic_features`.

| Test | Result | Observation |
| --- | --- | --- |
| `test_online_publication` | **passed** | `fault_online` staged 2 rows with one MERGE; `publish_table` synced them to the Online Feature Store `featlas-free` (CU_1, AVAILABLE); a FeatureSpec and the Feature Serving endpoint `featlas-dr-44488112120` were created; the endpoint returned publication `online-44488112120` for both keys on the first poll, and `published_at` = 2026-11-19T08:49:11.104146Z was recorded (staging commit 08:42:07Z — never used as online visibility). Served values equal the staged ones. Endpoint, FeatureSpec, online table and store were removed. |
| `test_feature_views_parity` | **passed** | As in platform run 42993010899: the five mapped features equal featlas' snapshot (0 mismatches). |
| `test_scheduled_job` | failed — test harness | The generated job was created through the Jobs API with the wheel and project in a Unity Catalog volume; the on-demand run (`ONE_TIME`) and a run fired by the job's own 5-minute schedule (`PERIODIC`) both **succeeded** and published `fault_job/device_reliability/2026-11-19T08:30:00Z` and `…T08:40:00Z` (2 rows each, every row with observed `published_at`). The final cross-check — an interactive recomputation of the 08:40 slot — returned 0 where the job had published 2: the platform test and the main suite of the same workflow run used the same run-suffixed source tables, and the main suite re-seeded and dropped them between the scheduled run and the recomputation. Fixed in the test (platform tables carry their own suffix); `core.scheduling` is not pinned from this run. |

Client: `databricks-connect==17.3.16`, `databricks-sdk==0.169.0`; runtime `19.15.x-aarch64-photon-scala2.13`.
