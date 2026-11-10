# CLI reference: `fe-spec`

**In plain terms:** `fe-spec` is the command-line tool. Every subcommand reads a project file,
answers one question about it and prints the answer as a readable table or as JSON. None of
them deploys, publishes, sends anything or creates cloud resources; `generate` only writes files
into the directory you name.

```text
Feature engineering specification 0.2.0 toolchain (featlas)

Usage: fe-spec <COMMAND>

Commands:
  validate        Parse, schema-validate and reference/type-check a project
  explain         Explain a feature: identity, stages, lineage and requirements
  hash            Print the logical and logical-semantic identities (hashes)
  plan            Build the target-neutral plan for a deployment, with adapter preflight
  capabilities    Produce an AdapterCapabilityReport for a deployment
  check-report    Check a capability report's consistency against exact project bytes
  test            Run reference acceptance suites through the exact local evaluator
  evaluate        Evaluate one acceptance case and show values, states and the stage trace
  coverage        Compute per-feature coverage states from source-status evidence (used by hosts)
  generate        Generate plan, capability report and adapter code into a directory (no execution)
  consume         Serve a target-neutral consumer request (local stores, or physical records supplied by a host)
  migration-plan  Produce a store-migration plan between two deployments of the same project
  label           Build forward-horizon labels for a consumer from supplied rows (bounded, local)
  mcp             Model Context Protocol server over stdio
  help            Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

## Conventions

* **Input format** is chosen from the file extension: `.json` is read as JSON, anything else as
  YAML (`Format::from_path`). Both go through the same restricted profile.
* **Output.** `--format text` renders with rs-rich: colour only on a terminal, none when
  `NO_COLOR` is set, and `COLUMNS` sets the width when piped
  (`crates/featlas-cli/src/render.rs`). `--format json` and the MCP server never pass through
  the renderer. CLI JSON envelopes carry `"schema": "featlas.cli/v1"` and a `command`; API
  results carry `"schema": "featlas.api/v1"`.
* **Exit codes.** `0` success; `1` when the answer is negative (validation failed, not
  plannable, a test case failed, report inconsistent, an API error); `2` for usage or I/O errors
  (`fe-spec: …` on stderr, and clap's own usage errors).
* **Load stage.** `explain`, `plan`, `capabilities`, `test`, `evaluate` and `migration-plan`
  first load through the reference stage and print the staged report (exit 1) if it fails.
  `hash`, `coverage`, `generate`, `consume`, `label` and `check-report` call the API, which
  returns a `DOCUMENT_INVALID` error (exit 1) instead.
* **Time limit.** Local evaluation uses a 60 s deadline (`commands.rs::limits`).

Examples below were captured on this commit with `NO_COLOR=1 COLUMNS=110` and
`B=target/release/fe-spec`, `E=spec/feature-engineering-spec-v0.2.0/examples`.

## validate

```text
Usage: fe-spec validate [OPTIONS] <PROJECT>
      --format <FORMAT>  [default: text] [possible values: text, json]
```

Runs parse → schema → references/types. Text output prints diagnostics and one summary line;
JSON adds the staged report, `logical_identities` and `levels_not_evaluated`.

```text
$ $B validate $E/01_fault_count.yaml
PASS: spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml (parse=passed, schema=passed, references=passed); planning, data and runtime levels not evaluated by validate
```

A failing document (example 01 with `status: draft` changed to `status: yes`; file path
shortened):

```text
error   PARSE_AMBIGUOUS_SCALAR [parse:SEM-PARSE-001] /metadata/status (line 13, column 11): Plain scalar "yes" is ambiguous across YAML versions; quote it or use JSON-compatible true/false/null/number syntax
FAILED: bad.yaml (parse=failed, schema=not_evaluated, references=not_evaluated); planning, data and runtime levels not evaluated by validate
```

Exit code 1.

## explain

```text
Usage: fe-spec explain [OPTIONS] --feature <FEATURE> <PROJECT>
      --feature <FEATURE>
      --format <FORMAT>    [default: text] [possible values: text, json]
```

Prints the feature's fingerprint, meaning, entity and keys, output, transform, event window,
availability rule, history, missingness, the canonical stage order, inputs and lineage roles. See
[Getting started](../getting-started.md#3-explain-a-feature) for full output.

## hash

```text
Usage: fe-spec hash <PROJECT>
```

Prints JSON: `project_file_sha256`, `logical` (canon id, per-object fingerprints,
`logical_project`) and `deployments` (deployment identities). Excerpt for example 01:

```json
{
  "project_file_sha256": "e1969d08f15b068831b35b920e1fb618bec09380acc74157bf35e0a8cd7cb5ad",
  "logical": {
    "canon": "featlas-canon/1",
    "features": {
      "device.fault_count_7d@1.0.0": "dbe41c1433f9f79616aa225f88ff483eaa0101e8abb9c94924d87aedcaf73a00"
    },
    "logical_project": "6d512fd95cd2abe19da68d7622d659caad853f3eb39bb790918c622840c72cdb"
  },
  "deployments": {
    "fault_dev@1.0.0": "fbba7d7613062498632d08cfedd9bf99806e3b01e31108107d4b69e33d71f6f6",
    "fault_prod@1.0.0": "bd1cc41f7d905aec2522cafd72b7ce9e19fcc5c4356684eea14eeaff79dcc029"
  },
  "schema": "featlas.api/v1",
  "operation": "hash",
  "command": "hash"
}
```

(Other fingerprint maps — `feature_sets`, `consumers`, `entities`, `sources`, `relationships` —
omitted.) See [Identity and hashing](../concepts/identity.md).

## plan

```text
Usage: fe-spec plan [OPTIONS] --deployment <DEPLOYMENT> <PROJECT>
      --deployment <DEPLOYMENT>
      --format <FORMAT>          [default: json] [possible values: text, json]
```

Builds the target-neutral plan with adapter preflight. JSON contains `plan`, `checks`,
`diagnostics`, `plannable`, `execution_ready` and `execution_scope`. Text prints diagnostics, a
capability table and `plannable=… execution_ready=…`. Exit code 1 if not plannable. Output for
both the synthetic-pin example and the derived project is in
[Getting started](../getting-started.md#4-plan-for-a-deployment).

## capabilities

```text
Usage: fe-spec capabilities [OPTIONS] --deployment <DEPLOYMENT> <PROJECT>
      --deployment <DEPLOYMENT>
      --evidence-dir <EVIDENCE_DIR>  Directory receiving local acceptance evidence used by local adapters
      --assessed-at <ASSESSED_AT>
```

Prints an `AdapterCapabilityReport` (schema
`spec/feature-engineering-spec-v0.2.0/schemas/capability-report.schema.json`): project and
deployment identity, `assessed_at` (default: now), `execution_scope`, `adapters` (implementation,
version, `implementation_sha256`, `runtime_versions`), `checks`, `execution_ready` and
`limitations`. The report is checked for consistency before it is printed.

`--evidence-dir DIR` first runs all local suites; if none fails it writes
`DIR/local-acceptance.json` and passes its SHA-256 as local evidence to local adapters (this is
how `docs/evidence/capability-report-positive-local.json` reaches `execution_ready: true`).
Report-level `limitations` for `fault_prod` on this commit:

```json
[
  "Requirements include planner-derived operations as well as declared ones; declared support strings are never treated as evidence.",
  "Evidence references are recorded, not fetched or authenticated by this report generator.",
  "Execution is not ready: at least one requirement is unsupported, unverified, or lacks evidence adequate for the execution scope."
]
```

A full check entry is shown in [Capabilities and readiness](../concepts/capabilities.md#what-this-commit-reports).

## check-report

```text
Usage: fe-spec check-report --project <PROJECT> <REPORT>
```

Checks a report against exact project bytes; prints `consistent`, `diagnostics` and a `scope`
statement; exit code 1 if inconsistent. Example in
[Capabilities](../concepts/capabilities.md#checking-a-report).

## test

```text
Usage: fe-spec test [OPTIONS] <PROJECT>
      --suite <SUITE>
      --format <FORMAT>  [default: text] [possible values: text, json]
```

Runs every acceptance suite (or one) through the exact local evaluator. In JSON, each case may
carry `quality_warnings` ([Quality policies](../concepts/quality.md)). Exit code 1 if any case
failed; unknown suite also exits 1. Cases that need an adapter are `NOT EXECUTED` and are not
passes. Example 04:

```text
$ $B test $E/04_fitted_and_custom_contracts.yaml
...
executed=3 passed=3 failed=0 not_executed=3 (not-executed cases are not passes)
```

## evaluate

```text
Usage: fe-spec evaluate [OPTIONS] --fixture <FIXTURE> <PROJECT>
      --fixture <FIXTURE>
      --format <FORMAT>    [default: json] [possible values: text, json]
```

Evaluates one case (by case id) with a stage trace. Text output for example 01:

```text
$ $B evaluate $E/01_fault_count.yaml --fixture event_window_edges --format text
Passed: 
Trace: event_window_edges
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ stage         ┃ feature               ┃ detail                                                             ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ validate_rows │                       │ device_registry: 1 rows type-checked                               │
│ validate_rows │                       │ fault_events: 4 rows type-checked                                  │
│ reconstruct   │                       │ device_registry: 1 supplied → 1 available (Inclusive cutoff) → 1   │
│               │                       │ selected, 0 retracted                                              │
│ population    │                       │ device: 1 eligible entities                                        │
│ reconstruct   │ device.fault_count_7d │ fault_events: 4 supplied → 4 available (Inclusive cutoff) → 4      │
│               │                       │ selected, 0 retracted                                              │
│ coverage      │ device.fault_count_7d │ complete for the required interval                                 │
│ event_window  │ device.fault_count_7d │ fault_events: 4 → 2 rows in [2026-06-01T00:00:00Z,                 │
│               │                       │ 2026-06-08T00:00:00Z)                                              │
│ predicate     │ device.fault_count_7d │ 2 → 2 rows (only true passes)                                      │
│ group         │ device.fault_count_7d │ 1 entity groups on complete keys (tenant_id, device_id)            │
└───────────────┴───────────────────────┴────────────────────────────────────────────────────────────────────┘
{
  "rows": [
    {
      "entity_keys": {
        "tenant_id": "tenant_a",
        "device_id": "device_1"
      },
      "value": 2,
      "state": "ok"
    }
  ],
  "diagnostic": null
}
```

The first line is `<status>: <reason>`; the reason is empty for a passing case. Exit code 1 only
if the case failed; an unknown case id also exits 1.

## coverage

```text
Usage: fe-spec coverage --deployment <DEPLOYMENT> --feature-set <FEATURE_SET> --as-of <AS_OF> --status <STATUS> <PROJECT>
      --status <STATUS>            JSON array of source-status records
```

Computes each member feature's coverage state (`established`, `insufficient_history`,
`source_unavailable`), the applicable policy and the view variant (`computed` / `degraded`) for
hosts. Example and record format: [Missingness and coverage](../concepts/missingness.md).

## generate

```text
Usage: fe-spec generate --deployment <DEPLOYMENT> --out <OUT> <PROJECT>
```

Writes `plan.json`, `planning-diagnostics.json`, `capability-report.json`, adapter code (for
Databricks `databricks/<feature_set>/script.sql` and `artifacts.json`, plus
`databricks/labels/<consumer>/artifacts.json` for label consumers) and `manifest.json`
(generator versions, `fe-spec` executable SHA-256, project digest, deployment identity, plan
digests, `execution_ready`, SHA-256 and size per file, and the statement "Generated code has not
been executed by this operation. No resource was created and nothing was published."). Planning
errors print diagnostics and `Not generated: …` with exit code 1. See
[Databricks guide](../guides/databricks.md#how-lowering-maps-to-spark-sql).

```text
$ $B generate projects/01_fault_count.yaml --deployment fault_prod --out generated/fault_prod
Generated 6 files under generated/fault_prod; execution_ready=false
```

## consume

```text
Usage: fe-spec consume [OPTIONS] --request <REQUEST> <PROJECT>
      --request <REQUEST>
      --records <RECORDS>  Physical records read by an external host for the bound target (JSON array)
```

Serves a consumer request (`api_version: featlas.consumer/v1`, `deployment`, `consumer`,
`consumer_version`, `rows` holding exactly the consumer's entity input fields and time inputs;
at most 100 000 rows, `featlas-api::MAX_REQUEST_ROWS`). Without `--records` it reads from the
deployment's local stores; with `--records` it uses the physical rows a host read. Worked
example: [Publication and replay](../concepts/publication.md#example-replay-excludes-a-later-publication).

## migration-plan

```text
Usage: fe-spec migration-plan [OPTIONS] --from <FROM> --to <TO> <PROJECT>
      --format <FORMAT>  [default: json] [possible values: text, json]
```

Produces a review artifact mapping every logical field (entity keys, values, states and the five
publication metadata fields) from one deployment's target to another's, with blockers and
semantic-review items. It performs no transfer, backfill or cutover
(`crates/featlas-core/src/migration.rs`). Text output for example 06 (abridged):

```text
$ $B migration-plan projects/06_store_migration.yaml --from fault_prod --to fault_alternative_store --format text
Migration plan fault_prod -> fault_alternative_store
  BLOCKER: Destination alternative_store/offline_store/core.atomic_publication is unverified (local_mock)
  BLOCKER: Destination alternative_store/offline_store/core.serving_replay is unverified (local_mock)
  BLOCKER: Destination databricks_platform/compute/core.aggregate is unverified (static)
  ...
  BLOCKER: Destination databricks_platform/orchestrator/core.scheduling is unverified (declaration)
[
  {
    "feature_set": "device_reliability",
    "version": "1.0.0",
    "role": "offline_store",
    ...
    "lossless_rules": "Rename only: entity keys, values, states and all five publication metadata fields map to distinct fields in both directions; precision (microsecond UTC, BIGINT, binary64) and explicit null/state encodings must be verified on the destination backend."
  }
]
```

The command exits 0 even when blockers exist; read the blockers.

## label

```text
Usage: fe-spec label --consumer <CONSUMER> --input <INPUT> <PROJECT>
      --input <INPUT>        JSON object: {"rows": [...], "source_rows": [...], "source_status": {...}}
```

Builds forward-horizon labels for a consumer that declares a `label`, locally and bounded. `rows`
are consumer request rows (entity inputs, `as_of`, `requested_at`, `dataset_cutoff`),
`source_rows` are rows of the label source and `source_status` is its completeness evidence at the
cutoff (`crates/featlas-core/src/label.rs::LabelInput`). See [Labels](../concepts/labels.md); for
Databricks, `generate` additionally writes `databricks/labels/<consumer>/artifacts.json`.

## mcp serve

```text
Usage: fe-spec mcp <COMMAND>
  serve  Serve MCP over stdin/stdout (read-only tools; documents passed inline)
```

See [MCP server](mcp.md).
