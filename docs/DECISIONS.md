# Architecture and decision record

Status: accepted for featlas 0.1.0 (implements specification 0.2.0). Each decision lists the
spec clause it serves and, where the spec left a choice open, the interpretation adopted.

## D1. Language and boundaries

* **Rust canonical core** (`crates/featlas-core`): parser, schema registry, model, semantic
  validation, canonical hashing, planner, exact evaluator, consumer API, labels, migration
  planning, capability reports. It has no vendor dependency, physical-coordinate assumption or
  vendor-name dispatch (enforced by `crates/featlas-adapters/tests/rebinding.rs::core_crate_has_no_vendor_dependencies`).
* **Rust adapters** (`crates/featlas-adapters`): `dev.featlas:local_reference`,
  `dev.featlas:memory_store` (test double), `dev.featlas:databricks` (Spark SQL lowering and
  Unity Catalog statements). The core resolves adapters only through the
  `AdapterRegistry`/`StoreRegistry` traits by exact `(implementation, version)`.
* **Shared API** (`crates/featlas-api`): JSON-in/JSON-out functions used by the CLI, the MCP
  server and the Python bindings, so transports cannot drift (parity is tested in
  `python/tests/test_native.py::test_transport_parity_with_cli` and the CLI/MCP tests).
* **Python bindings** (`crates/featlas-py`, PyO3 0.30, abi3-py312, built by maturin into
  `featlas._native`; ergonomic wrapper `python/featlas`). Bindings pass exact document bytes and
  JSON text across the boundary and return the API's JSON, release the GIL during core work and
  map failures to `featlas.FeatlasError(code, message, detail)`. They add no semantics.
* **Thin Python host** (`python/featlas_databricks`): executes generated statements in a
  caller-provided Spark session (Databricks Connect or a job). It calls the core in-process via
  the bindings for plans, code, coverage states and consumer selection; it contains no
  temporal, typing, null or selection rules.
* Repository was empty (greenfield), so the prompt's recommendation was followed.

## D2. Identity scopes (SEM-ID-004, SEM-PORT-008)

| Scope | Computed from | Changes when |
| --- | --- | --- |
| Logical fingerprint (feature/set/consumer/entity/source/relationship, logical project) | `featlas-canon/1` JSON of the semantic projection: documentation keys (`description`, `extensions` at every depth; `name`, `owner(s)`, `tags`, `status`, `acceptance` at object top level) removed; referenced entity/population source/input sources/relationships and dependency fingerprints included | Meaning changes |
| Deployment identity | Deployment projection + member feature-set fingerprints | Any binding, adapter pin, schedule, budget… |
| Logical plan digest | Logical part of the plan only | Meaning changes |
| Plan digest | Logical plan digest + bindings + requirements + core version | Bindings or implementation change |
| Project file SHA-256 | Exact bytes | Any byte |
| Run/publication identity | `publication_id`, `publication_sequence`, `materialization_id` assigned by the host | Every run/import |

`featlas-canon/1`: keys sorted by code point, arrays in order, minimal JSON escapes, integers
in decimal, binary64 numbers in shortest round-trip form always containing `.` or `e`
(so integer `1` ≠ number `1.0`). Frozen by `canon::tests::canonical_form_is_frozen`.
Number-typed contract fields (e.g. tolerances, budgets) are binary64 in the model, so authored
`0` and `0.0` have the same fingerprint (they are the same JSON Schema number).

## D3. Parser profile (SEM-PARSE-001)

The YAML reader builds JSON directly from parser events and rejects: multiple documents,
anchors, aliases, merge keys, any explicit tag, duplicate keys, non-string or non-scalar keys,
plain scalars that resolve differently across YAML 1.1/1.2 (`yes`, `~`, `.inf`, `0x1F`,
`1_000`, `NULL`…), plain timestamps or dates, non-finite numbers, integers outside i64, `${…}`
interpolation markers (in any string), BOMs and NULs; 4 MiB, depth 128 and 2 M node limits.
JSON input must parse as strict JSON first and then goes through the same event builder, so
both formats share one rule set and source locations. This is stricter than the bundled kit
(which accepts e.g. `yes` as a string); all shipped examples pass.

## D4. Evaluation interpretations

| Topic | Interpretation | Basis |
| --- | --- | --- |
| Row validity | Every supplied row of every direct/transitive/population source is type- and nullability-checked before anything else (`invalid_row_policy: reject`); unavailable sources are not read. | Prompt step 1; SEM-SRC-001 |
| Duplicates | Physical PK duplicates: `reject` fails on any; `collapse_identical` collapses only fully identical rows. Append-only logical key with differing rows fails. Versioned: any two eligible revisions sharing the full revision-order key with different payload fail (not only the winner). | SEM-REV-001/002 |
| Invalid intervals | `valid_to <= valid_from` on **any** reconstructed dimension row fails (`INVALID_INTERVAL`), not only on rows matched by key. | SEM-REV-001 "invalid selected revision" |
| Coverage precedence | `source_unavailable` (any transitive source) takes precedence over `insufficient_history`. `earliest_as_of`, retention `earliest_reproducible_at` and contiguous envelopes all yield `insufficient_history`. | SEM-MISS-001/003 |
| Coverage vs data errors | Available sources are reconstructed even when history is insufficient, so contract violations in supplied rows still fail rather than being masked by a null. | Prompt order (reconstruct before step 9) |
| Derived features | A dependency's `source_unavailable`/`insufficient_history` state propagates to the derived feature (with the derived feature's own missingness policy) rather than becoming an `ok` null. | SEM-MISS-001 |
| Row-level feature references | A feature reference inside a filter/value expression is rejected (`UNSUPPORTED_ROW_LEVEL_DEPENDENCY`); entity-level expressions cannot read source columns (`EXPRESSION_SOURCE_INPUT`). | SEM-ID-003, prompt |
| Sums | Integer sums are exact (i128 accumulate, checked i64 result); number sums are correctly rounded (Shewchuk/`fsum`); means divide the exact sum. | SEM-TRANS-004 |
| Quality `warn`/`quarantine` | Reported `UNSUPPORTED_CAPABILITY` (no artifact sink), as in the kit. | SEM-QUAL-001 |
| Source-as-known consumer reads | Latest evidence-complete publication with `as_of <= request.as_of`, ordered `(as_of, published_at, sequence)`, staleness checked after selection; replay additionally requires `published_at <= requested_at`. Publications whose `published_at` evidence is still pending are never served. | SEM-REPLAY-002, SEM-FRESH-002 |
| Labels | Source reconstructed at `availability ≤ dataset_cutoff`; horizon `[T, T+H]` with declared bounds; requires maturity `T+H ≤ cutoff` **and** status coverage of the horizon (end rule mirrors the window rule); otherwise `incomplete_horizon`. | SEM-LABEL-001/002 |

## D5. Planning and capabilities (SEM-PORT-002/006)

* Requirements are **derived** from operations (transform kinds, predicates, joins by temporal
  mode, source reads per binding adapter, atomic publication per target, replay per consumer,
  labels, native bindings, schedules) and merged with declared ones; derived-but-undeclared
  requirements produce `CAPABILITY_REQUIREMENT_DERIVED` warnings and are still required.
* Adapters assess each (role, capability). The Databricks adapter reports `unverified` with
  scope `static` for lowered operations, `declaration` for scheduling, and `unsupported` for
  online, learned, custom and label construction. It never reports support without live
  evidence. `execution_ready` for an external target requires every check supported with
  live-target evidence.
* Synthetic example pins (`org.example.fe_spec:*@0.0.0-example`) do not resolve
  (`UNRESOLVED_ADAPTER`). `projects/*.yaml` are generated by `projects/derive.py` with real
  pins; logical fingerprints are asserted equal to the examples'.

## D6. Databricks publication protocol (SEM-PORT-009, SEM-REPLAY-003)

1. Compute the complete snapshot into a temporary view (published_at NULL).
2. One Delta `MERGE … WHEN NOT MATCHED THEN INSERT *` keyed on (entity keys, as_of,
   publication_id) — a single commit makes the whole set visible; retries are idempotent.
   The commit carries `userMetadata = materialization_id`.
3. Read `DESCRIBE HISTORY`; the unique commit with that userMetadata gives the observed
   visibility instant; `UPDATE … SET published_at = <commit timestamp> WHERE publication_id = …
   AND published_at IS NULL` records it. Readers (consumer API) never serve rows whose
   evidence is pending, so a crash between steps 2 and 3 cannot fabricate visibility; step 3 is
   re-runnable.
4. `published_at` is never derived from schedules, job time or `as_of + delay`.

Open verification items for the live gate: commit timestamp semantics (in-commit timestamps vs
file modification time), concurrent-writer behaviour, informational PK constraints, UTF8_BINARY
collation, ANSI mode and `raise_error` behaviour on the pinned runtime.

## D7. Native and custom code

`portable` mode rejects native bindings; `allow_native` requires adapter/role, digest-pinned
implementation, declared capabilities, a suite with cases for the feature and migration notes.
No adapter in this release attests a custom/learned implementation, so such deployments are not
plannable (`UNSUPPORTED_CAPABILITY`). `extensions` are never read by the planner or evaluator.
