# Live Databricks acceptance run 42984352013

`pull_request` run of `.github/workflows/databricks-live.yml` for PR #5 at head `b9548e2`
(GitHub tested the merge commit `1c42b92731810e8674c6e499293549a929c4c67c`). Artifact downloaded unchanged (zip SHA-256
`070957e38875c1c996aa8ede5e174f8df98e591464977d04e7ba63661c0a669e`); `pytest-summary.txt` holds the PASSED lines and totals from the job log. No
credentials, hosts or workspace identifiers are included. The `platform` job runs on manual
dispatch only, so this run covers the same scope as run 42847828392.

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested sources (`sources.sha256`) | `databricks.rs` `4a9f8da7…e629`, `runner.py` `a024c552…c61f`, `cli.py` `0993793f…aa7d`, `online.py` `eb7f2913…4819`, `feature_views.py` `c83cc6fa…c566` |
| Runtime / client | Databricks Runtime `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0`; `databricks-connect==17.3.15`, `databricks-sdk==0.164.0` |
| Differential suite | **60 passed** in 561.6 s (46 reference cases, 3 quality projects, quarantine/warn host test, 7 label cases, precedence and rejection tests) |
| Unity Catalog publication suite | **8 passed** in 86.2 s |

Not exercised: Azure Databricks, classic clusters, scheduling (job deployment), online store and
Feature Serving, Feature Views — those are the `platform` job's scope.
