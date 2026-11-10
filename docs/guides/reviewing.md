# Reviewing evidence

**In plain terms:** a reviewer's job is to check that a claim ("this feature is computed
correctly on Databricks") is backed by something concrete — a named test, a run, a file with a
known digest — for exactly the code that will run. featlas is built so those links are explicit.
This page lists what to check and where to find it.

## 1. Is this the code that was tested?

* `docs/evidence/candidate.txt` names the commit, whether the tree was dirty, the rustc version,
  the `fe-spec` SHA-256 and the `Cargo.lock` SHA-256 of the evidence run.
* `docs/evidence/SHA256SUMS` lists the digest of every evidence file.
* A generated `manifest.json` names the generator versions, the `fe-spec` executable SHA-256,
  the project file SHA-256, the deployment identity, the plan digests and a SHA-256 per generated
  file. It states "Generated code has not been executed by this operation."
* The live run's `commit.txt` names the tested commit and `fe-spec` SHA-256; `databricks_lock.json`
  pins the artifact zip SHA-256 and the SHA-256 of each evidence file
  (`evidence_files_match_the_lock` re-hashes them in `cargo test`).

SEM-TEST-003 requires that the tested candidate is the shipped candidate. Note that the evidence
in `docs/evidence/` was produced at an earlier commit than the current sources; see
[Status](../status.md).

## 2. Does the claim match the evidence scope?

Read the capability report, not the YAML. For each `(adapter, role, capability)`:

* `status` (`supported_native`, `supported_emulated`, `unverified`, `unsupported`) and
  `evidence_scope` (`declaration` < `static` < `local_mock`/`local_reference` < `live_target`)
  are separate;
* an external target is `execution_ready` only with `live_target` evidence for every check;
* `limitations` say what was not observed (for Databricks: Azure, classic clusters).

`fe-spec check-report REPORT --project PROJECT` checks a report's internal consistency against
exact project bytes, but "evidence is not fetched or authenticated and readiness is not
certified" ([Capabilities](../concepts/capabilities.md#checking-a-report)).

## 3. Are the goldens independent?

* Example goldens come from the vendored specification and are read, never regenerated.
* Added goldens are hand-reasoned (`projects/quality/derive.py`,
  `projects/labels/spend_training_dataset.json`) or come from an independent implementation (the
  legacy SQL in [characterization](../reverse/characterization.md)).
* `not_executed` is never a pass; check the `not_executed` counts.

## 4. Do engines agree?

The differential harness compares Spark results with the goldens **and** with the core: values
(numbers within the declared tolerance), states, the first diagnostic code, quality warnings and
label rows (`python/featlas_databricks/differential.py`). Known engine notes are listed in each
generated `artifacts.json` (`notes`).

## 5. Is every rule covered?

[TRACEABILITY.md](../TRACEABILITY.md) maps each SEM rule to implementation and tests with a
status (`impl+test`, `impl`, `partial`, `unsupported`, `open`). Rules marked `open` need live
evidence. [EVIDENCE.md](../EVIDENCE.md) records the independent reviews (C1–C10, D1–D13, R1–R7)
and their resolutions; R7 is open.

## 6. Publication claims

For a published snapshot, check the host's evidence: one inserting MERGE commit, `published_at`
equal to that commit's `_commit_timestamp` from the change data feed, source tables pinned with
`VERSION AS OF`, approver and environment recorded, and (since D12) quality warnings. See
[Publication and replay](../concepts/publication.md) and the
[Databricks guide](databricks.md#publication-protocol).

## Checklist

- [ ] Commit and binary digests of the evidence match the candidate.
- [ ] Every required capability for the target is supported with adequate evidence scope.
- [ ] Unverified/unsupported items are listed as limitations, not hidden.
- [ ] Goldens are independent and unchanged; no `not_executed` counted as pass.
- [ ] Differential results agree with goldens and the core.
- [ ] No credentials, hosts or personal data in evidence or specs.
