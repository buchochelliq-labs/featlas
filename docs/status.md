# Status and roadmap

**In plain terms:** this is the single page that says what works today, what has been proven
where, and what is not built yet. Other pages describe current behaviour and link here instead
of repeating status claims. If you change what featlas can do, update this page (and the records
it cites).

Statements are taken from `README.md`, `docs/EVIDENCE.md`, `docs/DECISIONS.md`, the code and
the tests they cite. Counts are those recorded in the evidence files unless a row says
otherwise; building this site does not re-measure them.

!!! note "Records written before the latest features"
    `docs/EVIDENCE.md` (milestone 3) and `docs/TRACEABILITY.md` (SEM-QUAL-001, SEM-LABEL-*)
    were written before quality policies and Spark label datasets were added, and still say
    "warn/quarantine unsupported" and "labels not lowered to Spark". The code, DECISIONS D12 and
    the tests below supersede those lines; the evidence records themselves are not edited here.

## Current state

| Area | State | Evidence |
| --- | --- | --- |
| Canonical core, fixtures, negatives | Done. 50/50 reference cases of the vendored examples pass; 3 adapter-only cases (example 04) are `not_executed`, never passes. | [EVIDENCE.md](EVIDENCE.md), `docs/evidence/fixtures-*.json` |
| Quality policies `error` / `warn` / `quarantine` | Done in the core and in generated Spark SQL ([Quality policies](concepts/quality.md), DECISIONS D12). | `projects/quality/*.yaml`; local Spark differential (quality projects + `test_quarantine_reports_the_violating_rows_and_warn_keeps_values`) |
| Label datasets | Done in the core (`fe-spec label`) and lowered to Spark (`core.label_construction`, `featlas_databricks.labels.LabelRun`) ([Labels](concepts/labels.md)). | `projects/labels/spend_training_dataset.json` goldens; `crates/featlas-core/tests/labels.rs`; `test_generated_label_sql_matches_goldens_and_core` |
| Databricks code generation | Done; differential-tested on local Apache Spark 4.0.1 (46 reference cases recorded in `docs/evidence/local-spark-differential.json`; the suite now also covers the quality projects and label cases). | `python/tests/test_local_spark_differential.py` |
| Live Databricks acceptance | **Passed on Databricks Free Edition (AWS, serverless)**, run 42771706619: differential 49/49 and Unity Catalog publication 7/7. That run predates quality policies and Spark labels. **Azure Databricks: not run.** | [RUN.md](evidence/databricks-live/RUN.md) |
| Databricks capability reports | Lock-driven ([Capabilities](concepts/capabilities.md#the-databricks-adapter-lock)). For this commit every check is `unverified` and `execution_ready: false`, because `databricks.rs` and `runner.py` have changed since the pinned run. | `crates/featlas-adapters/src/databricks_lock.json` |
| Publication protocol on local Delta Lake | Exercised (one MERGE per publication, commit-instant evidence, idempotent retry, identity conflict, crash recovery, table identity). Unity Catalog constraints, three-part names and in-commit timestamps are not exercised locally. | [EVIDENCE.md](EVIDENCE.md) |
| Reverse engineering (`featlas_reverse`) | Done: SQL analysis, drafting with an assumptions report, characterization goldens, wide-table split, parity SQL, MCP server, three agent skills ([Reverse engineering](reverse/index.md)). | `python/tests/test_reverse.py` (local Spark), `python/tests/test_reverse_mcp.py` |
| Storage rebinding | Boundary proof with an in-memory test double (example 06); not a second production store and not a migration. | `crates/featlas-adapters/tests/rebinding.rs` |
| Migration | Plan only (`fe-spec migration-plan`); no transfer, backfill or cutover. | `crates/featlas-core/src/migration.rs` |
| CLI / MCP / Python bindings | Done locally; transports share `featlas-api`. | `python/tests/test_native.py::test_transport_parity_with_cli` |

While writing these pages, `tests/test_reverse.py` (3 tests), `tests/test_reverse_mcp.py` and
`tests/test_native.py` (16 tests together) and the quality/label subset of
`tests/test_local_spark_differential.py` (11 tests) were run on local Spark 4.0.1 / OpenJDK 21
and passed. Those runs are not recorded under `docs/evidence/`.

## Not implemented in this version

These are reported as `unsupported` or `unverified` by the planner and capability reports; none
is silently emulated.

| Item | How it shows up today | Source |
| --- | --- | --- |
| Databricks job scheduling (`core.scheduling`) | `unverified`, scope `declaration`: "Databricks job scheduling is not implemented in this version; manual runs only". Derived for interval schedules; gates only the schedule operation. | `databricks_caps.rs`, `capability.rs` |
| Budget and retention preflight | **In progress** (being added). Today: the local evaluator and the MCP server have work/time/size limits; Spark budgets are not enforced; output retention is only checked as a declaration (`OUTPUT_RETENTION_SHORT` warning). | TRACEABILITY SEM-BUDGET-001, SEM-DEPLOY-002 |
| Online store / Feature Serving, online publication | `unsupported` ("This adapter version provides no online_store role"; "Online publication is not implemented") | `databricks_caps.rs` |
| Databricks Feature Views | Not used; `feature_api` must be `uc_feature_tables` | `databricks.rs` configuration schema |
| Learned / custom transform execution | Unsupported by design: contracts are validated, never executed | DECISIONS D7, SEM-CUSTOM-001/002 |
| Live failure/retry tests beyond the publication suite | Open | EVIDENCE milestone 5 |
| ODCS import/validation | Not implemented; a prospective mapping only | [ODCS.md](ODCS.md) |

## Not observed on a live target

From [RUN.md](evidence/databricks-live/RUN.md): Azure Databricks, classic (non-serverless)
clusters, retention/VACUUM behaviour, grants beyond the test principal's, scheduling, online
serving. Quality policies and label datasets have not been run on Databricks.

## Open review items

* **R7 (low):** generated SQL grows quickly for deeply nested number expressions (finite guards
  repeat sub-expressions); bounded by the nesting limit of 24. ([EVIDENCE.md](EVIDENCE.md))
* **Tenant-column residual limit:** a relationship pairing the tenant key with a non-tenant
  column of the same type cannot be detected without column-level tenant metadata
  ([SPEC_FINDINGS.md](SPEC_FINDINGS.md) #7).

## Releases

No release has been made. `rab.yml` declares the packages; see [Releases](releases.md).

## Roadmap (from the records)

The records name these as the next gates; they are not commitments with dates:

1. Re-run `.github/workflows/databricks-live.yml` on the current sources and pin the run in
   `databricks_lock.json` so the Databricks checks can report `supported_native` with
   `live_target` evidence ([Databricks guide](guides/databricks.md#re-pinning-the-adapter-lock)).
2. Run the Azure Databricks gate of the [runbook](DATABRICKS_RUNBOOK.md).
3. Finish budget/retention preflight, then the remaining "Not implemented" items, each with its
   own evidence before a capability is reported as supported.
