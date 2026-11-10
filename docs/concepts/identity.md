# Identity and hashing

**In plain terms:** featlas computes several different fingerprints, each answering a
different question. "Did the *meaning* of this feature change?" is a logical fingerprint. "Did
anything at all in the file change?" is the file SHA-256. "Will exactly the same plan run on the
same bindings and engine?" is the plan digest. Moving a table to another catalog changes the
file hash and the deployment identity, but not the feature's logical fingerprint.

## The scopes (DECISIONS D2, `crates/featlas-core/src/canon.rs`)

| Scope | Computed from | Changes when |
| --- | --- | --- |
| Logical fingerprint (feature, feature set, consumer, entity, source, relationship, logical project) | `featlas-canon/1` JSON of the semantic projection: documentation keys removed; referenced entity, population source, input sources, relationships and dependency fingerprints included | The meaning changes |
| Deployment identity | Deployment projection + member feature-set fingerprints | Any binding, adapter pin, schedule, budget… |
| Logical plan digest | Logical part of the plan only | The meaning changes |
| Plan digest | Logical plan digest + bindings + requirements + core name/version (`plan.rs`) | Bindings or implementation change |
| Project file SHA-256 | Exact bytes of the file | Any byte |
| Run / publication identity | `publication_id`, `publication_sequence`, `materialization_id` | Every run or import |

Documentation keys removed from logical fingerprints are `description` and `extensions` at every
depth, and `name`, `owner(s)`, `tags`, `status`, `acceptance` at object top level. The file
SHA-256 is binding-sensitive and is never a logical fingerprint (SEM-PORT-008).

## Canonical JSON: `featlas-canon/1`

* object keys sorted by Unicode code point, no whitespace;
* arrays keep their order (order is meaningful in the language);
* strings use JSON escapes only for `"`, `\` and U+0000–U+001F; everything else is literal UTF-8;
* integers in shortest decimal; binary64 numbers in Rust's shortest round-trip form, always
  containing `.` or `e`, so integer `1` and number `1.0` stay distinct; `-0.0` is written `-0.0`.

The form is frozen by `canon::tests::canonical_form_is_frozen`. Number-typed contract fields
(tolerances, budgets) are binary64 in the model, so authored `0` and `0.0` hash identically
(SPEC_FINDINGS #5).

## Seeing the scopes in practice

`projects/01_fault_count.yaml` is the vendored example with real adapter pins and a different
local output format. Selected fields of `fe-spec hash` on both files (`project_file_sha256`,
`logical.logical_project` and `deployments["fault_prod@1.0.0"]`):

```text
spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml
  file            e1969d08f15b068831b35b920e1fb618bec09380acc74157bf35e0a8cd7cb5ad
  logical_project 6d512fd95cd2abe19da68d7622d659caad853f3eb39bb790918c622840c72cdb
  fault_prod@1.0.0 bd1cc41f7d905aec2522cafd72b7ce9e19fcc5c4356684eea14eeaff79dcc029
projects/01_fault_count.yaml
  file            a09bc543bee57bba411f6e2a30676d0eefe223a7b747e4c2156db3a137ae7400
  logical_project 6d512fd95cd2abe19da68d7622d659caad853f3eb39bb790918c622840c72cdb
  fault_prod@1.0.0 f19fc51466f7868d8776477d5eea63cd37c3ad7ca51fc2fc45a4bd27f1fa0827
```

The logical project fingerprint is identical; the file digest and the deployment identity differ.
`crates/featlas-adapters/tests/rebinding.rs::logical_fingerprints_survive_rebinding_and_adapter_repinning`
asserts this for examples 01 and 06 and their derived projects (all four file digests differ;
logical fingerprints are equal), and `python/tests/test_native.py::test_logical_identities_survive_rebinding`
checks it through the bindings.

## Where each identity appears

* `fe-spec hash` and `fe-spec validate --format json` (`logical_identities`).
* `fe-spec explain` prints the feature fingerprint.
* Generated `manifest.json` records `project_file_sha256`, deployment `identity`, `plan_digest`,
  `logical_plan_digest`, the generator versions and the `fe-spec` executable SHA-256, plus a
  SHA-256 per generated file.
* Generated `script.sql` names the plan and logical plan digests in its header.
* Databricks feature tables carry `featlas.feature_set` and `featlas.feature_set_fingerprint`
  table properties; the host refuses to publish into a table whose properties differ
  (`TARGET_TABLE_MISMATCH`).
* Capability reports carry `project_file_sha256`, the deployment id/version and each adapter's
  `implementation_sha256`.

## Versions are separate evidence (SEM-ID-004)

`spec_version` versions the language; object `version` fields version meaning; compiler, runtime,
SDK and adapter versions are separate evidence. Changing an adapter version changes the
deployment identity and plan digest, not the feature's meaning.
