# Capabilities and readiness

**In plain terms:** before anything runs on a real platform, featlas asks each adapter "can you
preserve these exact semantics, and how do you know?". The answer has two parts: a *status*
(supported, unsupported, unverified) and the *kind of evidence* behind it (a declaration, code
inspection, a local run, a live run). Only live evidence can make an external target
"execution ready". Writing a capability name in the YAML never counts as proof.

## Requirements are derived, not just declared (SEM-PORT-006)

`crates/featlas-core/src/plan.rs::derive_requirements` derives `(adapter, role, capability)`
requirements from what the deployment actually does and merges them with
`runtime.required_capabilities`:

| Derived from | Capability | Role |
| --- | --- | --- |
| Each output target | `core.atomic_publication` (+ `core.online_publication` for `online_store`) | target role |
| `aggregate` / `recency` / `lookup` / `expression` transforms | `core.aggregate` / `core.recency` / `core.lookup` / `core.expression` | compute |
| Learned / custom transforms | their declared `required_capabilities` | compute |
| Any row-shaped transform | `core.revision_reconstruction`, `core.source_as_known` (+ `core.expression` for a filter) | compute |
| Joins | `core.as_of_join` (temporal `as_of`) or `core.relationship_join` (temporal `none`) | compute |
| Each bound source a feature reads | `core.source_read` | catalog |
| Replay consumers | `core.serving_replay` | consumer's role |
| Consumers with a label | `core.label_construction` | compute |
| Native bindings | their `required_capabilities` | binding's role |
| Interval schedule with an orchestrator | `core.scheduling` | orchestrator |

A derived-but-undeclared requirement produces the warning `CAPABILITY_REQUIREMENT_DERIVED` and is
still required. This is why the kit's sample report for example 06 is flagged
`REPORT_REQUIRED_CAPABILITY_MISSING` for `core.scheduling` (SPEC_FINDINGS #2).

## Status and evidence scope

`crates/featlas-core/src/capability.rs` keeps them apart:

| Status | Meaning |
| --- | --- |
| `supported_native` | The adapter implements it natively |
| `supported_emulated` | The adapter implements it by emulation |
| `unverified` | Possibly implemented, not established for this execution scope |
| `unsupported` | Not provided |

| Evidence scope | Meaning |
| --- | --- |
| `declaration` | Only stated (for example in configuration) |
| `static` | Code generation exists and was inspected/tested offline |
| `local_reference` | Established by a local reference run |
| `local_mock` | Established against a test double |
| `live_target` | Established by a run on the real target |

## Plannable versus execution-ready

* **`plannable`**: no error diagnostics and a plan exists. An `unsupported` requirement is an
  error (`UNSUPPORTED_CAPABILITY`) that blocks planning, except when every operation it gates is
  `label_dataset` or `schedule`; then it is a warning and gates only that operation.
* **`execution_ready`**: plannable, **and** every check is supported, **and** every check has
  evidence, **and** the evidence scope fits: for an external target (`execution_scope:
  external_target`) every scope must be `live_target`; for local targets it must not be
  `declaration`.

Example: the positive local report `docs/evidence/capability-report-positive-local.json` is
`execution_ready: true` with scope `local_reference`; the Databricks report
`docs/evidence/capability-report-databricks.json` is `execution_ready: false`.

## The Databricks adapter lock

`crates/featlas-adapters/src/databricks_caps.rs` assesses `dev.featlas:databricks` from
`crates/featlas-adapters/src/databricks_lock.json`, a file written only from observed live runs.
Each run entry records the run id and URL, the evidence artifact SHA-256, the tested commit, the
target (product, cloud, compute, Databricks Runtime, Spark version, Unity Catalog), the client
versions, the evidence files with their SHA-256, the list of verified `(role, capability)` pairs
and `sources_sha256`: the SHA-256 of `crates/featlas-adapters/src/databricks.rs` and
`python/featlas_databricks/runner.py` that the run tested.

Assessment logic, in order:

1. **Operator-supplied live evidence** is recorded but stays `unverified` (scope `live_target`)
   until a reviewed run is pinned in the lock.
2. **A pinned run that tested exactly the compiled sources** (both digests equal; the binary
   embeds both files with `include_bytes!`) and lists the pair: `supported_native`, scope
   `live_target`, with the evidence URI, digest and a limitation naming the observed target.
3. **A pinned run whose sources differ** from the compiled ones: `unverified`, scope `static`,
   with a limitation naming the run, the tested and compiled digests and the instruction to
   re-run the live workflow.
4. Otherwise a static fallback: lowered compute capabilities (`COMPUTE_LOWERED`:
   `core.aggregate`, `core.recency`, `core.lookup`, `core.expression`,
   `core.revision_reconstruction`, `core.as_of_join`, `core.relationship_join`,
   `core.source_as_known`, `core.label_construction`), `core.source_read`,
   `core.atomic_publication` and `core.serving_replay` are `unverified`/`static`;
   `core.scheduling` is `unverified`/`static` (a paused job is generated, DECISIONS D14); online
   roles, learned/custom and online publication are `unsupported`.

So `supported_native` is only ever reported while the compiled `databricks.rs` and `runner.py`
hash exactly as in a pinned live run, and only for the pairs that run verified. Run 42771706619
does not list `core.label_construction`; run 42816483568 does.

The adapter's `implementation_sha256` is the SHA-256 over `path:sha256` lines of the vouched-for
sources, and `runtime_versions` lists the target and client versions only when a matching run
exists. Keeping assessment in `databricks_caps.rs` (not `databricks.rs`) means changing assessment
logic does not invalidate the lowering pin.

### What this commit reports

The lock pins five runs. Run 42771706619 tested `databricks.rs` `1847f7c2…` and `runner.py`
`44be9ac4…`; run 42816483568 tested `95a63302…` and `3f0cb046…`; run 42847828392 tested
`95a63302…` and `b0deb914…`; runs 42890653921 and 42907049080 tested the five files vouched for
since the online, Feature Views and job changes. Run 42907049080 (`databricks.rs` `4a9f8da7…`,
`runner.py` `a024c552…`, `cli.py` `0993793f…`, `online.py` `6be70a8d…`, `feature_views.py`
`c83cc6fa…`) tested the compiled sources of this commit. So step 2 applies with run
42907049080: the eight core checks of `projects/01_fault_count.yaml` (`fault_prod`) and
`core.label_construction` (project 03) are `supported_native` with `live_target` evidence, while
`core.scheduling` and the `online_store` checks stay `unverified`/`static` until a run of the
platform job is pinned. Real output of
`fe-spec capabilities projects/01_fault_count.yaml --deployment fault_prod`, first check:

```json
{
  "adapter": "databricks_platform",
  "role": "compute",
  "capability": "core.aggregate",
  "status": "supported_native",
  "evidence_scope": "live_target",
  "evidence": [
    {
      "uri": "https://github.com/buchochelliq-labs/featlas/actions/runs/42907049080#docs/evidence/databricks-live/run-42907049080/databricks-differential.json",
      "sha256": "e4c1c109d12a338654c5e53e95f53395063d0f9f61998dd945ed5a7b57f5d437",
      "description": "Live run 42907049080 on Databricks Free Edition (aws, serverless compute), Databricks Runtime 19.13.x-aarch64-photon-scala2.13, Spark 4.2.0 (tested commit 03f6960acaef, artifact sha256 188d5f11d151b6df9b83673ca9028db835d16b422a56055a380b4746c5078e69): 49 differential cases (46 reference cases of projects 01, 02, 03, 05, 06 and the 3 quality-policy projects) vs goldens and core; fixture sources as session temp views"
    }
  ],
  "limitations": [
    "Observed only on Databricks Free Edition (aws, serverless compute), Databricks Runtime 19.13.x-aarch64-photon-scala2.13, Spark 4.2.0; Azure Databricks and classic clusters were not observed. Evidence is pinned to adapter source sha256 4a9f8da7b58163bfb434d4e58c23e54831b2aeac25caa88c8c6d28c855a4e629 and host runner sha256 a024c552e832f2fdbedddcd679300069372dd3d1c03b318875da069773eec61f."
  ]
}
```

and the report ends with `"execution_ready": false` because of `core.scheduling`. The compiled
digests depend on the exact source bytes, so any edit to those files sends the checks back to
step 3 until a new run is pinned. How to re-pin is in the
[Databricks guide](../guides/databricks.md#re-pinning-the-adapter-lock).

## Checking a report

`fe-spec check-report REPORT --project PROJECT` checks a report's consistency against the exact
project bytes. The checks in `capability.rs::check_report_core` include the project hash and
identity, the deployment, the adapter roster and identities, duplicate or unknown checks, role
mismatches, support claims without evidence or with only declaration scope, missing
implementation digests, missing (including planner-derived) required capabilities, and whether an
`execution_ready: true` claim is backed by live evidence and runtime versions for an external
target (codes `REPORT_*`). It does **not** fetch or
authenticate evidence and does not certify readiness (the output says so in `scope`). On the kit's
sample report:

```json
{
  "consistent": false,
  "diagnostics": [
    {
      "code": "REPORT_REQUIRED_CAPABILITY_MISSING",
      "severity": "error",
      "stage": "planning",
      "rule": "SEM-PORT-006",
      "path": "/checks",
      "message": "Missing assessment for databricks_platform/orchestrator/core.scheduling (planner-derived)"
    }
  ],
  "scope": "Record consistency only; evidence is not fetched or authenticated and readiness is not certified.",
  "schema": "featlas.api/v1",
  "operation": "check_report"
}
```

Every report `fe-spec capabilities` produces is first checked with the same function and refused
(`REPORT_INCONSISTENT`) if it fails.
