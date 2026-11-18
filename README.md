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
* **`crates/featlas-cli`** — the `fe-spec` CLI and a read-only MCP stdio server. Text output is
  rendered with [rs-rich](https://crates.io/crates/rs-rich) (colour only on a terminal; `NO_COLOR`
  honoured); `--format json` and MCP output are unaffected. Needs Rust ≥ 1.90.
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
| **Live Databricks acceptance** | **Passed on Databricks Free Edition (AWS, serverless)**, run 42771706619: differential 49/49 and Unity Catalog publication 7/7 (`docs/evidence/databricks-live/`). **Azure Databricks: not run.** Run 42984352013 on the current adapter (60/60 differential incl. quality and labels, 8/8 publication) is pinned in the adapter lock: those capabilities report `supported_native` with live evidence; scheduling is generated but unobserved, so `execution_ready: false`. |
| Online store/serving, Feature Views | Not implemented (reported unsupported) |
| Job scheduling | Paused Databricks job generated (DECISIONS D14); not deployed or run (`unverified`) |
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

## Reverse engineering existing feature tables

`python/featlas_reverse` (extra `reverse`: `pip install 'featlas[reverse]'`) turns an existing
feature table and the SQL that builds it into a spec, splits a very wide feature table into smaller
ones, and proves that tables regenerated from the spec reproduce the legacy table:

```sh
featlas-reverse draft --table wide.table.json --sql wide.sql --source orders.table.json \
  --source customers.table.json --out spec.yaml --report drafting-report.json
featlas-reverse split-analyze spec.yaml --feature-set customer_wide --table wide.table.json > split.json
featlas-reverse split-apply spec.yaml --feature-set customer_wide --assignment split.json \
  --strategy tag:domain --out split.yaml --mapping mapping.json
featlas-reverse parity split.yaml --legacy-table main.features.customer_wide \
  --legacy-as-of-column feature_ts --key tenant_id --key customer_id --as-of 2026-06-01T00:00:00Z
```

* The SQL analyzer (sqlglot, Databricks dialect) maps aggregates, filters (`FILTER`, `CASE WHEN`,
  WHERE), windows and derived ratios to spec transforms and lists everything else as unsupported.
* The drafter records every assumption and open question (population table, availability time,
  empty semantics) in a report; drafts validate and pass their fixtures.
* Characterization cases use the legacy table's own outputs as goldens. A late-arriving row the
  legacy code read after `as_of` shows up as a reported divergence, not as silently changed values.
* The split analysis scores tag (domain, business unit, owner...), filter-family, value-family,
  window and source strategies; derived features may depend on another split table (their inputs
  are recomputed from the same sources). The agent decides; see the skill.
* Agents use it through the read-only MCP server `python -m featlas_reverse.mcp_server`
  (registered in `.mcp.json`) and the skills in `.claude/skills/`: `featlas-reverse-engineer`,
  `featlas-split-wide-table` and `featlas-regenerate`.
* Worked example: `examples/reverse/` (inputs, `regenerate.sh`, `generated/` outputs). Evidence:
  `python/tests/test_reverse.py` runs the legacy SQL on local Spark, regenerates the six split
  tables through the generated Spark SQL and runs the parity SQL (no mismatches).

## Releases

Releases use [rab, the Release Asset Bundle](https://github.com/buchochelliq-labs/release-asset-bundle)
(installed under `.github/rab/`; reinstall to update, never edit). `rab.yml` declares two packages:
the `fe-spec` binaries on a GitHub release and the `featlas` wheels on PyPI (the workspace crates
are `publish = false`). CI runs `release.py check` and `release.py vendored`. Nothing is
published until someone runs `python3 .github/rab/scripts/release.py tag` on `main`, after the
`pypi` environment's trusted publisher is set up.

## License

Apache License 2.0; see [`LICENSE`](LICENSE). The vendored specification under `spec/` keeps
its own terms.
