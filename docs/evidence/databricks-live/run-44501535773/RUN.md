# Live Databricks acceptance run 44501535773

`workflow_dispatch` run of `.github/workflows/databricks-live.yml` on `claude/relaxed-rubin-vrjeag` at
`394bb06031ad36ab78ae58f0c66367ad41368870` (main suite and platform job on the same sources). Artifact downloaded unchanged (zip SHA-256
`264864375729347362bf48f0501ff15917cc4be041f42848073a9b79bd2d3704`); `pytest-summary.txt` holds the PASSED lines and totals from the job log. No
credentials, hosts or workspace identifiers are included. The main suite covers the same scope as run 42847828392; the platform job's results are in
[platform-run-44501535773](../platform-run-44501535773/RUN.md).

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested sources (`sources.sha256`) | `databricks.rs` `a0d64250…4eb7`, `runner.py` `a024c552…c61f`, `cli.py` `51ffb65a…56ef`, `online.py` `4b922774…a4d9`, `feature_views.py` `c83cc6fa…c566` (identical to run 44488112120) |
| Runtime / client | Databricks Runtime `19.15.x-aarch64-photon-scala2.13`, Spark `4.2.0`; `databricks-connect==17.3.16`, `databricks-sdk==0.169.0` |
| Differential suite | **60 passed** in 523.8 s (46 reference cases, 3 quality projects, quarantine/warn host test, 7 label cases, precedence and rejection tests) |
| Unity Catalog publication suite | **8 passed** in 89.1 s |

Not exercised by the main suite: Azure Databricks, classic clusters; scheduling, online serving and
Feature Views are the platform job's scope.
