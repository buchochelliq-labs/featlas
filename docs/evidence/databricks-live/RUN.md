# Live Databricks acceptance run 42771706619 (attempt 3)

Downloaded unchanged from the `databricks-live-evidence` artifact of
`.github/workflows/databricks-live.yml` (artifact zip SHA-256
`3c76096038fa6672a7947801097751a0768d8baecf614b89c1e2cbde9d2957de`). No credentials, hosts or
workspace identifiers are included.

| Item | Value |
| --- | --- |
| Target | **Databricks Free Edition (AWS), serverless compute**, Unity Catalog catalog `featlas` — not Azure Databricks |
| Tested commit | `5e9d94e8f258e38206a294ac0ee4fd653a78c6d9` (GitHub's merge of PR #3 head `4d48ae2` into `main` `b25c09d`) |
| `fe-spec` (release build) SHA-256 | `b41417617ee6e4955737f5cd733bca0c7732dbf2add3c8cd88249a50e7f81423` |
| Runtime | Databricks Runtime `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0` |
| Client | `databricks-connect==17.3.15`, `databricks-sdk==0.164.0`, Python 3.12.16; OAuth M2M, scope `databricks-connect` |
| Differential suite | 49 passed in 318.8 s: 46 reference cases (projects 01, 02, 03, 05, 06) through generated Spark SQL compared with goldens **and** the core, 2 multi-violation precedence tests, 1 rejection test (`databricks-differential.json`) |
| Unity Catalog publication suite | 7 passed in 80.6 s (`databricks-live.json`) |

Publication suite, observed on run-unique tables (all dropped at the end):

1. Physical-schema preflight of the generated read bindings: no problems.
2. Feature table created from the generated DDL; `SHOW CREATE TABLE` shows the
   `PRIMARY KEY ... TIMESERIES` constraint. One MERGE (table version 1) published 2 rows with
   sources pinned `VERSION AS OF 0`; `published_at` equals the inserting commit's
   `_commit_timestamp` (2026-11-10T07:50:43Z) read from the change data feed.
3. Generated consumer read + core consumer API: batch read returns both tenants' values with
   provenance; replay one microsecond before the commit returns nothing, at the commit returns
   the values.
4. Retry of the same publication writes no second commit and reports the original identities.
5. Reusing a `publication_id` with another sequence is rejected (`PUBLICATION_IDENTITY_CONFLICT`).
6. Crash between commit and evidence: live reads serve the pending snapshot, replay excludes it,
   a retry records the evidence and replay then serves it.
7. A feature table whose fingerprint property differs is refused (`TARGET_TABLE_MISMATCH`).

Not exercised: Azure Databricks, classic (non-serverless) clusters, retention/VACUUM behaviour,
grants beyond the test principal's, scheduling, online serving.
