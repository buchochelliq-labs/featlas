# Live Databricks acceptance run 42907049080

`pull_request` run of `.github/workflows/databricks-live.yml` for PR #5 at head `3590aed`
(GitHub tested the merge commit `03f6960acaefa224619abc0a1af038e6919a70b4`). Artifact
downloaded unchanged (zip SHA-256 `188d5f11d151b6df9b83673ca9028db835d16b422a56055a380b4746c5078e69`);
`pytest-summary.txt` holds the PASSED lines and totals from the job log. No credentials, hosts or
workspace identifiers are included. The `platform` job was skipped (it runs on manual dispatch
only), so this run covers the same scope as run 42847828392.

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested sources (`sources.sha256`) | `databricks.rs` `4a9f8da7…e629`, `runner.py` `a024c552…c61f`, `cli.py` `0993793f…aa7d`, `online.py` `6be70a8d…6ca1`, `feature_views.py` `c83cc6fa…c566` |
| Runtime / client | Databricks Runtime `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0`; `databricks-connect==17.3.15`, `databricks-sdk==0.164.0` |
| Differential suite | **60 passed** in 665.2 s (46 reference cases, 3 quality projects, quarantine/warn host test, 7 label cases, precedence and rejection tests) |
| Unity Catalog publication suite | **8 passed** in 109.0 s |

Difference from run 42890653921: the generated primary-key constraint is named after the physical
table (`<table>_pk`) because Unity Catalog constraint names are unique per schema.

Not exercised: Azure Databricks, classic clusters, scheduling (job deployment), online store and
Feature Serving, Feature Views — those are the `platform` job's scope.
