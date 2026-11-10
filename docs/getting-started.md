# Getting started

**In plain terms:** build the `fe-spec` command, point it at one of the example projects and
run a handful of commands. Each command answers one question: is the file valid, do its test
cases pass, what does a feature mean, what would run on a target, and what code would be
generated. Nothing here needs a cloud account.

## Requirements

| Component | Requirement | Where it is declared |
| --- | --- | --- |
| Rust libraries and Python bindings | Rust ≥ 1.85 | `Cargo.toml` (`workspace.package.rust-version`) |
| `fe-spec` CLI | Rust ≥ 1.90 (needed by `rs-rich`, the terminal renderer) | `crates/featlas-cli/Cargo.toml` |
| Python package `featlas` | CPython ≥ 3.12 (abi3 wheel), built with `maturin>=1.16,<2` | `python/pyproject.toml` |
| Local Spark differential tests (optional) | Java 17+, `pyspark==4.0.1`, `delta-spark==4.0.0` | `python/pyproject.toml` (`local` extra), `README.md` |

The recorded evidence was produced with rustc 1.97.0 (`docs/evidence/candidate.txt`).

## Build the CLI

```sh
cargo build --release -p featlas-cli
target/release/fe-spec --version
```

```text
fe-spec 0.1.0
```

`cargo build --release` builds the default workspace members (core, adapters, API, CLI). The
Python extension crate `crates/featlas-py` is not a default member; maturin builds it
(`Cargo.toml` comment).

## First commands

The examples below use the vendored example `01_fault_count.yaml` (one feature: a seven-day
fault count per device) and its derived copy under `projects/`, which differs only in adapter
pins and local output formats (see [Writing feature specs](guides/authoring.md#examples-and-derived-projects)).

```sh
B=target/release/fe-spec
E=spec/feature-engineering-spec-v0.2.0/examples
```

### 1. Validate

```sh
$B validate $E/01_fault_count.yaml
```

```text
PASS: spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml (parse=passed, schema=passed, references=passed); planning, data and runtime levels not evaluated by validate
```

`validate` runs the parse, schema and reference/type stages only, and says which conformance
levels it did not evaluate (SEM-CONF-001).

### 2. Run the golden test cases

```sh
$B test $E/01_fault_count.yaml
```

```text
Acceptance cases: spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml
┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ status ┃ suite/case                                     ┃ feature                     ┃ reason ┃
┡━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ PASS   │ fault_count_conformance/event_window_edges     │ device.fault_count_7d@1.0.0 │        │
│ PASS   │ fault_count_conformance/availability_edges     │ device.fault_count_7d@1.0.0 │        │
│ PASS   │ fault_count_conformance/identical_duplicates   │ device.fault_count_7d@1.0.0 │        │
│ PASS   │ fault_count_conformance/conflicting_duplicates │ device.fault_count_7d@1.0.0 │        │
│ ...    │ (9 more rows)                                  │                             │        │
└────────┴────────────────────────────────────────────────┴─────────────────────────────┴────────┘
executed=13 passed=13 failed=0 not_executed=0 (not-executed cases are not passes)
```

(Trimmed; captured with `NO_COLOR=1 COLUMNS=110`. Text output is coloured only on a terminal.)

### 3. Explain a feature

```sh
$B explain $E/01_fault_count.yaml --feature device.fault_count_7d
```

```text
device.fault_count_7d@1.0.0 — Fault count in seven days
  fingerprint: dbe41c1433f9f79616aa225f88ff483eaa0101e8abb9c94924d87aedcaf73a00
  meaning: Number of classified fault observations for a registered device in [as_of - 7 days, as_of), using only observations available at as_of.
  entity: device (keys: tenant_id [tenant], device_id)
  output: integer? unit=Some("count")
  transform: aggregate
  event window: [as_of − 604800s, as_of) (publication delay 60s never shifts it)
  availability: available_time ≤ as_of
  history: 604800s contiguous, earliest_as_of 2026-02-01T00:00:00Z
  missingness: empty=Value { value: Number(0) } null_input=Ignore insufficient_history=Null source_unavailable=Null
  stage order: validate rows → availability cutoff → PK duplicates → logical-key revision selection → tie rejection → tombstones → population → window → joins → predicate → grouping → transform → coverage/missingness/quality/output
  input faults = fault_events columns ["event_id", "tenant_id", "device_id", "occurred_at", "available_at", "is_fault"]
  lineage fault_events.event_id roles ["ordering"]
  ...
```

### 4. Plan for a deployment

Planning the **vendored** example against `fault_prod` fails on purpose: its adapter pins are
synthetic (`org.example.fe_spec:*@0.0.0-example`) and no installed adapter implements them.

```sh
$B plan $E/01_fault_count.yaml --deployment fault_prod --format text
```

```text
error   UNRESOLVED_ADAPTER [planning:SEM-PORT-002] /deployments/1/adapters/0: No installed adapter implements org.example.fe_spec:databricks@0.0.0-example; registered: [("dev.featlas:local_reference", "0.1.0"), ("dev.featlas:memory_store", "0.1.0"), ("dev.featlas:databricks", "0.1.0")]. Example/synthetic adapter pins are not installed packages.
...
plannable=false execution_ready=false
```

The derived project uses real pins and plans:

```sh
$B plan projects/01_fault_count.yaml --deployment fault_prod --format text
```

```text
warning CAPABILITY_REQUIREMENT_DERIVED [planning:SEM-PORT-006] /deployments/1/runtime/required_capabilities: Planner derived databricks_platform/orchestrator/core.scheduling from the actual operations; it is required even though not declared
Capability checks: fault_prod
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ adapter             ┃ role          ┃ capability                   ┃ status     ┃ evidence    ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ databricks_platform │ compute       │ core.aggregate               │ unverified │ static      │
│ databricks_platform │ compute       │ core.as_of_join              │ unverified │ static      │
│ databricks_platform │ compute       │ core.expression              │ unverified │ static      │
│ databricks_platform │ compute       │ core.revision_reconstruction │ unverified │ static      │
│ databricks_platform │ compute       │ core.source_as_known         │ unverified │ static      │
│ databricks_platform │ catalog       │ core.source_read             │ unverified │ static      │
│ databricks_platform │ offline_store │ core.atomic_publication      │ unverified │ static      │
│ databricks_platform │ offline_store │ core.serving_replay          │ unverified │ static      │
│ databricks_platform │ orchestrator  │ core.scheduling              │ unverified │ declaration │
└─────────────────────┴───────────────┴──────────────────────────────┴────────────┴─────────────┘
plannable=true execution_ready=false
```

`plannable=true` means a named implementation can preserve the semantics; `execution_ready=false`
means the evidence does not yet allow running it on the external target (see
[Capabilities and readiness](concepts/capabilities.md)).

### 5. Generate code

```sh
$B generate projects/01_fault_count.yaml --deployment fault_prod --out generated/fault_prod
```

```text
Generated 6 files under generated/fault_prod; execution_ready=false
```

The six files are `manifest.json`, `plan.json`, `planning-diagnostics.json`,
`capability-report.json`, `databricks/device_reliability/script.sql` (a review artifact) and
`databricks/device_reliability/artifacts.json` (the statements the host executes). Generated
code is not executed. A committed copy lives in `docs/evidence/generated-fault_prod/`.

## Python bindings

```sh
cd python
pip install "maturin>=1.16,<2"
maturin develop --release      # or: maturin build --release && pip install target/wheels/*.whl
```

```python
from featlas import Project
p = Project.from_path("spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml")
s = p.run_fixtures()["summary"]
print(s["executed"], s["passed"], s["failed"], s["not_executed"], p.file_sha256)
```

```text
13 13 0 0 e1969d08f15b068831b35b920e1fb618bec09380acc74157bf35e0a8cd7cb5ad
```

See the [Python API](reference/python.md) for every method.

## Run the tests

```sh
cargo test --release
cd python && maturin develop --release && PYSPARK_PYTHON=$(which python) pytest -q tests
```

[Testing and evidence](testing.md) explains each suite and what it proves.
