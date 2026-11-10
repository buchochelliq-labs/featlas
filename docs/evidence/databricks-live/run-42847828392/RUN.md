# Live Databricks acceptance run 42847828392

`pull_request` run of `.github/workflows/databricks-live.yml` for PR #4 at head `eecb717`
(GitHub tested the merge commit `c3fa3016f686b7d5af64974c63eeb003a2aed683`). Artifact
downloaded unchanged (zip SHA-256 `74b17487a392b593eb05f1c2a835ba2c157db55458c8611123b2c7f53a9de16a`);
`pytest-summary.txt` holds the PASSED/FAILED lines and totals from the job log. No credentials,
hosts or workspace identifiers are included.

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested sources (`sources.sha256`) | `databricks.rs` `95a63302…10b3`, `runner.py` `b0deb914…f72b` |
| Runtime / client | Databricks Runtime `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0`; `databricks-connect==17.3.15`, `databricks-sdk==0.164.0` |
| Differential suite | **60 passed** in 665.6 s (same scope as run 42816483568: 46 reference cases, 3 quality projects, quarantine/warn host test, 7 label cases, precedence and rejection tests) |
| Unity Catalog publication suite | **8 passed** in 105.5 s |

Difference from run 42816483568: the review fixes in `runner.py` (1932da9). Retention preflight
now reports the seeded `fault_events` history as `RETENTION_UNPROVEN` with `severity: warning`
(a later first row does not prove deletion) and nothing blocks.

Not exercised: Azure Databricks, classic clusters, VACUUM/log-retention behaviour, scheduling (the
generated job was not deployed), online serving.
