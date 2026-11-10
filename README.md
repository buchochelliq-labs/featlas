# featlas

An implementation of the **Feature Engineering Specification 0.2.0** (vendored unchanged in
`spec/feature-engineering-spec-v0.2.0/`), with Azure Databricks as the first external target.

* **`crates/featlas-core`** — the single authoritative semantic implementation (Rust): restricted
  YAML/JSON parser, local Draft 2020-12 schema registry, typed model, reference/type/lineage/binding
  validation, canonical identity hashing, target-neutral planner, exact local evaluator, labels,
  target-neutral consumer API, migration planner, capability reports. No vendor SDKs or vendor
  dispatch.
* **`crates/featlas-adapters`** — `dev.featlas:local_reference`, `dev.featlas:memory_store`
  (test double) and `dev.featlas:databricks` (Spark SQL lowering, Unity Catalog statements).
* **`crates/featlas-api`** — transport-neutral JSON API; the CLI, the MCP server and the Python
  bindings all call it, so every transport returns identical results.
* **`crates/featlas-cli`** — the `fe-spec` CLI and a read-only MCP stdio server.
* **`crates/featlas-py`** — PyO3 bindings, built by maturin into the `featlas._native` extension
  (abi3, CPython ≥ 3.12).
* **`python/featlas`** — the Python package: `Project` wrapper over the native core.
* **`python/featlas_databricks`** — thin host that runs core-generated statements in a Spark
  session, calling the core in-process through `featlas`; no semantics of its own.

## Status (read before relying on it)

| Area | State |
| --- | --- |
| Canonical core, fixtures (50/50 reference cases; 3 adapter-only cases not executed), negatives | Done, local evidence |
| Databricks code generation | Done; differential-tested on **local Apache Spark 4.0.1** (46 cases) |
| **Azure Databricks live acceptance** | **OPEN — not run.** No workspace/credentials/authorization were available. Every Databricks capability reports `unverified`; `execution_ready: false`. See `docs/DATABRICKS_RUNBOOK.md`. |
| Online store/serving, Feature Views, job scheduling | Not implemented (reported unsupported/unverified) |
| Learned/custom execution | Unsupported by design (contracts validated, never executed) |
| Storage rebinding | Boundary proof with an in-memory test double (example 06); not a second production store or a migration |

Details: `docs/EVIDENCE.md` (results, review resolutions), `docs/TRACEABILITY.md` (SEM-* matrix),
`docs/DECISIONS.md` (architecture and interpretations), `docs/SPEC_FINDINGS.md`, `docs/ODCS.md`.

## Quick start

```sh
cargo build --release
B=target/release/fe-spec
E=spec/feature-engineering-spec-v0.2.0/examples
$B validate $E/01_fault_count.yaml
$B test $E/01_fault_count.yaml
$B explain $E/01_fault_count.yaml --feature device.fault_count_7d
$B evaluate $E/01_fault_count.yaml --fixture event_window_edges --format text
$B plan projects/01_fault_count.yaml --deployment fault_prod --format text
$B capabilities projects/01_fault_count.yaml --deployment fault_prod
$B generate projects/01_fault_count.yaml --deployment fault_prod --out generated/fault_prod
$B migration-plan projects/06_store_migration.yaml --from fault_prod --to fault_alternative_store --format text
$B mcp serve      # JSON-RPC over stdio; documents passed inline, read-only tools
```

`projects/*.yaml` are produced by `projects/derive.py` from the examples, changing only adapter
pins (the examples' `org.example.fe_spec:*@0.0.0-example` are synthetic and deliberately fail
planning with `UNRESOLVED_ADAPTER`) and local output formats; logical fingerprints are asserted
identical.

No command deploys, publishes or creates resources. Publication to Databricks requires the host
CLI's explicit `--publish` flags and an authorized environment.

## Python bindings

```sh
cd python
pip install maturin
maturin develop --release        # or: maturin build --release && pip install target/wheels/*.whl
python -c "
from featlas import Project
p = Project.from_path('../spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml')
print(p.run_fixtures()['summary']['passed'], p.file_sha256)
"
```

`Project` exposes `hash`, `explain`, `plan`, `capabilities`, `check_report`, `run_fixtures`,
`evaluate_case`, `coverage`, `generate` (in memory; returns file contents), `consume`,
`migration_plan` and `labels`. Documents are passed as exact bytes, so `file_sha256` matches the
CLI. Failures raise `featlas.FeatlasError` with `code`, `message` and structured `detail`. The GIL
is released while the core runs.

## Tests and evidence

```sh
cargo test
# Bindings + local Spark differential (needs maturin; Java 17+ and pyspark==4.0.1 for Spark):
cd python && maturin develop --release && PYSPARK_PYTHON=$(which python) pytest -q tests
# Regenerate docs/evidence (KIT_PYTHON / SPARK_PYTHON optional):
./tools/make_evidence.sh
```
