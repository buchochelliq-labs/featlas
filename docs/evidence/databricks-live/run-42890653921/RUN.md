# Live Databricks acceptance run 42890653921

`pull_request` run of `.github/workflows/databricks-live.yml` for PR #5 at head `2421f76`
(GitHub tested the merge commit `ac72f96a7645a040e922dca95839d1aef116bfa8`). Artifact
downloaded unchanged (zip SHA-256 `1b593fda60fa98c810ecfc03b6993f724d77144ac8d4492d5e1e06e2ddaf47dd`);
`pytest-summary.txt` holds the PASSED lines and totals from the job log. No credentials, hosts or
workspace identifiers are included. The `platform` job was skipped (it runs on manual dispatch
only), so this run covers the same scope as run 42847828392.

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested sources (`sources.sha256`) | `databricks.rs` `c47b23bf…c9aa`, `runner.py` `a024c552…f61b`, `cli.py` `0993793f…7d`, `online.py` `6be70a8d…ca1`, `feature_views.py` `c83cc6fa…566` |
| Runtime / client | Databricks Runtime `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0`; `databricks-connect==17.3.15`, `databricks-sdk==0.164.0` |
| Differential suite | **60 passed** in 564.2 s (46 reference cases, 3 quality projects, quarantine/warn host test, 7 label cases, precedence and rejection tests) |
| Unity Catalog publication suite | **8 passed** in 88.6 s |

Difference from run 42847828392: the adapter gained online-store targets, the Feature Views
mapping, Feast targets and the job runtime session; the offline lowering and publication
protocol exercised here are unchanged in behaviour.

Not exercised: Azure Databricks, classic clusters, scheduling (job deployment), online store and
Feature Serving, Feature Views — those are the `platform` job's scope.
