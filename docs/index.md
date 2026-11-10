# featlas

**In plain terms:** featlas checks and runs *feature definitions* for machine learning. You
describe a feature once (for example "number of faults a device reported in the last seven
days, counting only data we actually had at that moment") in a YAML file that follows the
Feature Engineering Specification 0.2.0.
featlas tells you whether that definition is well-formed, computes it exactly on small test
data, explains every step, and generates reviewable Spark SQL for Databricks. It never deploys
or publishes anything on its own.

!!! note "Where the specification lives"
    The specification is vendored byte-for-byte under
    `spec/feature-engineering-spec-v0.2.0/` (its `$id` URLs under `example.org` are
    identifiers, not a hosted registry; see its `SPECIFICATION.md` §1). featlas implements that
    vendored copy. Rule identifiers such as `SEM-TIME-001` used throughout these pages are the
    specification's stable rule IDs.

## What it is made of

| Part | Path | What it does |
| --- | --- | --- |
| Semantic core | `crates/featlas-core` | Restricted YAML/JSON parser, embedded JSON Schema registry, typed model, validation, canonical hashing, planner, exact local evaluator, consumer API, labels, migration planning, capability reports. No vendor code. |
| Adapters | `crates/featlas-adapters` | `dev.featlas:local_reference`, `dev.featlas:memory_store` (test double) and `dev.featlas:databricks` (Spark SQL lowering, Unity Catalog statements, lock-driven capability assessment). |
| Shared API | `crates/featlas-api` | JSON-in/JSON-out functions used by every transport. |
| CLI and MCP | `crates/featlas-cli` | The `fe-spec` command and a read-only MCP stdio server. |
| Python | `crates/featlas-py`, `python/featlas` | PyO3 bindings (`featlas._native`) and the `featlas.Project` wrapper. |
| Databricks host | `python/featlas_databricks` | Runs core-generated statements (feature snapshots, label datasets) in a Spark session; adds no semantics. |
| Reverse engineering | `python/featlas_reverse`, `.claude/skills/` | Turns an existing feature table and its SQL into a spec, splits wide tables, and generates parity SQL; CLI, MCP server and agent skills. |

## Two-minute tour

```sh
cargo build --release -p featlas-cli
B=target/release/fe-spec
E=spec/feature-engineering-spec-v0.2.0/examples

$B validate $E/01_fault_count.yaml          # is the document well-formed and consistent?
$B test $E/01_fault_count.yaml              # do the reviewed golden cases pass?
$B explain $E/01_fault_count.yaml --feature device.fault_count_7d
$B plan projects/01_fault_count.yaml --deployment fault_prod --format text
$B generate projects/01_fault_count.yaml --deployment fault_prod --out generated/fault_prod
```

Real output of the first two commands on this checkout:

```text
PASS: spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml (parse=passed, schema=passed, references=passed); planning, data and runtime levels not evaluated by validate
```

```text
executed=13 passed=13 failed=0 not_executed=0 (not-executed cases are not passes)
```

The [Getting started](getting-started.md) page walks through each command with its output.

## Start here, by role

<div class="grid cards" markdown>

- **Data / ML engineers writing feature specs** — read
  [The specification model](concepts/spec-model.md), [Time semantics](concepts/time.md),
  [Missingness](concepts/missingness.md), then [Writing feature specs](guides/authoring.md) and
  the [CLI reference](reference/cli.md).
- **Platform engineers deploying to Databricks** — read
  [Capabilities and readiness](concepts/capabilities.md),
  [Publication and replay](concepts/publication.md) and the
  [Databricks guide](guides/databricks.md).
- **Reviewers and auditors** — read [Reviewing evidence](guides/reviewing.md),
  [Identity and hashing](concepts/identity.md), [Testing and evidence](testing.md) and the
  records ([Evidence](EVIDENCE.md), [Traceability](TRACEABILITY.md),
  [Decisions](DECISIONS.md)).
- **Teams with existing feature tables** — read
  [Reverse engineering](reverse/index.md) and its [worked example](reverse/example.md).
- **Contributors to the Rust/Python code** — read [Architecture](architecture.md),
  [Contributing](contributing.md), the [Python API](reference/python.md) and
  [Releases](releases.md).

</div>

## What featlas does not do

featlas validates, plans, evaluates bounded fixtures and generates code. No `fe-spec` command
deploys, publishes or creates resources (`crates/featlas-cli/src/main.rs` module docs). Writing to
Databricks happens only through the separate host CLI `featlas-databricks` with explicit
`--publish` flags. What is and is not implemented is kept on one page:
**[Status and roadmap](status.md)**.
