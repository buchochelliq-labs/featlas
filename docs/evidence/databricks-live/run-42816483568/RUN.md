# Live Databricks acceptance run 42816483568

Workflow-dispatch run of `.github/workflows/databricks-live.yml` on branch
`claude/relaxed-rubin-vrjeag` at `ef5877b9b965c8e1fd035a17ae721ce3bf1e89b3` (the branch head
itself, not a merge commit). Artifact `databricks-live-evidence` downloaded unchanged (zip
SHA-256 `707d1dba1023f3c2b08b0595533d2c65d203fb330f25f20d21378330d35923de`);
`pytest-summary.txt` holds the PASSED/FAILED lines and totals copied from the job log. No
credentials, hosts or workspace identifiers are included.

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested sources (`sources.sha256`) | `databricks.rs` `95a63302…10b3`, `runner.py` `3f0cb046…a4a4` |
| `fe-spec` (release build) SHA-256 | `3df2e9083808802d070ee90030e9ea1382d97b1a4695afb36b14c17b46bfcd27` |
| Runtime | Databricks Runtime `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0` |
| Client | `databricks-connect==17.3.15`, `databricks-sdk==0.164.0`, Python 3.12.16; OAuth M2M, scope `databricks-connect` |
| Differential suite | **60 passed** in 497.9 s: 46 reference cases and the 3 quality-policy projects vs goldens **and** the core (`databricks-differential.json`), the quarantine/warn host test, 7 label-dataset cases vs goldens and the core, 2 precedence tests, 1 rejection test |
| Unity Catalog publication suite | **8 passed** in 93.5 s (`databricks-live.json`) |

New since run 42771706619 and observed here:

* **Quality policies** (DECISIONS D12): warn, quarantine and error projects on serverless; the
  quarantine test checks the violating row is attached and nothing is published.
* **Label datasets** (`lower_labels`): every case of `projects/labels/spend_training_dataset.json`.
* **Budget** (D13): the publication recorded `scan_bytes_upper_bound` 3735 bytes (sum of
  `DESCRIBE DETAIL` sizes of the two pinned sources) against `max_scan_bytes` 1073741824.
* **Retention preflight** (D13) on Unity Catalog tables: reported
  `RETENTION_CONTRACT_VIOLATED` for `fault_events` (declared history from 2026-01-01, oldest
  seeded row 2026-06-07) and nothing for `device_registry`, as the fixture data implies.
* **Evidence lookup over retained commits** (one MERGE commit read through `table_changes` with a
  bounded version range), retry, identity conflict, crash recovery and table identity as before.

Not exercised: Azure Databricks, classic clusters, VACUUM/log-retention behaviour, scheduling (the
generated job was not deployed: the service principal has only the `databricks-connect` scope),
online serving.
