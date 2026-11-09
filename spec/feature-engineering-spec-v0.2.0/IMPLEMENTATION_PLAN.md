# Implementation plan — Databricks first, portable contracts

This plan guides a future product implementation. The archive supplies schemas, normative rules, examples, and a bounded Python verification kit. It does not supply a production CLI, canonical Rust engine, live Databricks adapter, or MCP server. [VERIFICATION.md](VERIFICATION.md) records the evidence actually obtained for the bundle.

**Azure Databricks is the mandatory first external target and first production implementation.** Build a small complete path into Unity Catalog early, then extend its supported semantics. Keep the specification, canonical model, and consumer contract independent of the feature store so later changes of storage do not force new feature definitions.

## Architecture to preserve

The current greenfield recommendation is a Rust canonical core with thin Python for Databricks SDK or host I/O. Existing repository constraints may justify another language; document that decision. The invariant is one authoritative parser, model, semantic/type validator, planner, exact evaluator, diagnostics layer, and manifest model. No Databricks SDK import or vendor-name dispatch belongs in this core.

Adapters implement independent `compute`, `catalog`, `offline_store`, `online_store`, and `orchestrator` roles. One adapter may supply several roles, but the interfaces and deployment selectors must allow each role to change independently. An offline deployment may omit online storage. Orchestration may be omitted for manual execution; a declared interval or cron schedule requires an orchestrator. Deployment bindings own physical coordinates, adapter configuration, complete mappings, and runtime requirements. Transports and target-neutral consumers invoke the same core.

See [PORTABILITY.md](docs/PORTABILITY.md) for role boundaries, [DATABRICKS_MAPPING.md](docs/DATABRICKS_MAPPING.md) for the first target, and [MIGRATION.md](docs/MIGRATION.md) for specification/store migration obligations.

## Milestones and acceptance gates

| Milestone | Concrete result | Required acceptance evidence |
| --- | --- | --- |
| 0. Repository and contract baseline | Repository inventory; language/architecture decision; dependency policy; requirement/test matrix; unchanged reference-fixture baseline. | Actual kit commands and outcomes, schema/prose conflict resolution, exact starting candidate, and declared limits. |
| 1. Minimum canonical fault-count path | Restricted parser, local schema resolution, canonical model/hash, diagnostics, necessary semantic/type checks, exact local fault-count evaluation, reviewable target-neutral plan. | Reviewed goldens; meaningful parser/reference/time/coverage negative cases; deterministic output; unsupported operations fail explicitly. |
| 2. First Azure Databricks vertical slice | Generated Spark SQL/DataFrame execution over mapped sources into a Unity Catalog feature table; offline consumer retrieval through the neutral API. | Authorized live target run; exact runtime/SDK/adapter locks; physical preflight; ordering inspection; differential fixture outputs; observed offline publication and atomic-set evidence for claimed behavior. Without access, deliver code/plans and leave this gate unverified. |
| 3. Baseline semantic completion | Remaining aggregates, recency, lookup, historical joins, derived expressions, population/coverage, null/numeric policies, and supported replay/label behavior. | `SEM-*` coverage; independent temporal goldens and negatives; local/Databricks differential evidence for every advertised operation; explicit unsupported custom/learned capabilities. |
| 4. Portability and consumer proof | Role-scoped adapter registry; complete bidirectional mappings; target-neutral consumer API; explicit native contracts; storage rebinding using the local test double. | `examples/06_store_migration.yaml` preserves logical semantic identities/hashes, requests, values/states, and publication metadata after rebinding; role/mapping/native failures reject correctly. This is separation evidence, not a second production integration. |
| 5. Migration and offline operational readiness | Concrete migration/backfill/replay/cutover plan; recovery, idempotence, retention, access, bounded resources, and provenance. | Destination gap analysis; parity and history evidence appropriate to the available target; rollback/cutover criteria; truthful replay claims; tested retry/failure behavior. No live-store migration claim without live destination evidence. |
| 6. CLI and MCP | Strict, versioned, bounded validation/explanation/planning/fixture/code-generation interfaces over the same core. | Transport parity, preserved diagnostics, path/URI and resource limits, and no external mutation from read/plan/code-generation operations. |
| 7. Candidate acceptance | Independent review; complete docs, locks, reproducible examples, manifests, and exact-candidate evidence. | Material findings resolved; shipped bytes verified; mandatory Databricks gate passed before production support is claimed; limitations visible. Release/deployment requires applicable authorization. |
| Separate capability: online serving | Online publication and retrieval, observed visibility, freshness, replay, and feature-set consistency through the selected online role. | Separate live target evidence for `publication.surface: online_store`; offline results do not satisfy this gate. |

The milestone sequence establishes dependencies, not a requirement to finish every possible operator before testing the first target. The Databricks slice must follow the minimum trustworthy core path. Expand only once that path exposes mapping and runtime differences. Additional production stores and native/custom/learned execution are later capabilities with their own evidence gates.

## First complete slice

Use [01_fault_count.yaml](examples/01_fault_count.yaml), preserving its logical definitions and acceptance expectations. Implement source and population reconstruction, availability-first revision handling, exact event bounds, tenant/entity keys, coverage/outage behavior, and count output in the canonical evaluator. Produce a target-neutral plan that exposes these stages before generating Spark SQL or DataFrame operations.

Bind sources through a catalog-capable adapter and materialize the feature set through the offline-store role into a Unity Catalog feature table. Keep `as_of`, `published_at`, `publication_sequence`, `publication_id`, and `materialization_id` distinct. Read the result through a logical consumer request and deployment `consumer_bindings`. Demonstrate complete keys, value/state encoding, and observed offline publication behavior.

Feature Views may be introduced as a validated lowering when they preserve the same contract on the verified target. They do not replace the generated Spark execution acceptance path or excuse semantic differences. Online publication and serving remain separately demonstrated capabilities.

Example adapter implementations, versions, configuration, coordinates, and operational values are synthetic. Establish real dependency/runtime locks from the intended target and repository; record their exact successful versions. A configuration string never establishes installed software, credentials, permissions, or support.

## Semantic expansion sequence

After the first slice, add the remaining aggregates and explicit null/numeric behavior. Introduce historical dimensions only after revision and availability ordering have decisive tests. The historical-temperature example exposes late dimension corrections and valid-interval errors that a simple count does not. Commerce adds integer units, dependency alignment, safe ratios, and separate label obligations. Lookup/recency adds ordering, ties, and subsecond flooring.

Preserve availability cutoff → revision selection → conflicting/invalid selected-row rejection → tombstones → population → event/valid-time/business filtering → transforms → coverage/quality/output contracts. Every optimizer transformation needs an equivalence argument. Retain exact fixture semantics, independently reasoned expectations, full tenant keys, complete source-status evidence, and distinct empty/null/outage/unknown states.

Implement observed publication replay as its own path, with original publication evidence, latest-null behavior, staleness, and atomic-set consistency. Source-as-known recomputation cannot establish historical serving. Labels and learned preprocessing require their separate temporal/split boundaries; artifact digest verification cannot establish executable or leakage-free behavior.

## Portability and capability gates

Before code generation or execution, validate each adapter instance and selected role, all exact logical references, complete source/output mappings, compatible physical types, and the requested publication surface. Output mappings must include full keys, values, states, and required publication metadata. Reject missing, ambiguous, lossy, or incompatible mappings. Scope every runtime capability requirement to its adapter and role with a reason.

Capability reports separate requested requirements, support status, evidence scope, adapter identity, and execution readiness. Local checks, a mock, generated code, and live target execution are different evidence. Verify installed mechanisms and physical access; do not derive support solely from declarations. Generic location handling must support namespace sequences without assuming Databricks naming rules in the core.

The neutral consumer interface accepts declared consumer identity/version, entity keys, feature-set references, and time requests. It returns values/states, diagnostics, and provenance. Its callers do not receive SDK types or need physical table names. The local rebinding test must keep these requests and results stable while independently changing storage bindings; the demonstration certifies only architectural separation. Logical feature hashes stay stable, while deployment/execution identities and the full-project digest reflect binding changes.

In `portable` mode, require canonical semantics and reject `native_bindings`. In `allow_native`, require explicit `native_bindings` with exact feature/adapter/role identity, immutable artifact pins, required capabilities, a conformance suite, and migration requirements. These implementations still preserve the referenced logical contract. Treat ordinary `extensions` as metadata. Generated Spark SQL implements the canonical plan; arbitrary SQL or SDK objects cannot become an alternative hidden semantic language.

## Migration, backfill, replay, and cutover

A migration plan must inventory feature/consumer pins, mappings, source and publication histories, artifacts, retention, access, and destination capability gaps. Compare keys, time precision, value/state encoding, null behavior, atomicity, publication visibility, and recovery guarantees. Prove semantic equivalence before retaining logical feature versions; review semantic changes explicitly.

Backfill only retained, reproducible ranges with adequate coverage and split/artifact provenance. Respect `earliest_reproducible_at`, use the contracted separate output namespace, and never fabricate historical publication times. A faithfully copied observed ledger preserves its original evidence; destination visibility is a separate observation. State which replay questions can actually be answered after migration.

Define shadow/dual-read parity criteria, acceptable differences, rollback triggers, cutover checkpoints, consumer routing changes, idempotent retry behavior, and old-store retirement/retention requirements before a real cutover. Rebinding a fixture or generating a migration plan is not evidence that these operations occurred on a second live store.

For future ODCS integration, reuse existing source contract references and upstream/column lineage. Document proposed importer/validator mappings and gaps in [PRIOR_ART.md](docs/PRIOR_ART.md). The bundle does not claim a real ODCS import, execution path, or certification.

## Evidence and independent review

Each milestone report binds its claims to an exact candidate commit or code/specification digest and includes the relevant `SEM-*` IDs, test commands, dependency locks, adapter identities/configuration digests, source snapshot identities, generated plan/code digests, capability report, observed outputs/times, and review resolutions. Identify unsupported and skipped checks explicitly with reasons. Keep credentials and private raw logs out of distributed reports.

Review after semantic validation, temporal execution, and before candidate acceptance. Seek filters pushed below revision selection, omitted tenant keys, unsupported cardinality assumptions, coverage inferred from row absence, null/outage converted to zero, fabricated publication history, unstable artifacts, native behavior hidden in extensions, adapter logic leaking into consumers, and claims based on a different candidate.

Run only relevant additional checks after a change, then regenerate final manifests for the actual shipped bytes. A release-blocking finding must be fixed or the advertised capability narrowed. Local checks cannot replace the required live Azure Databricks acceptance gate; unavailable access leaves that gate open while useful verified local work is delivered.
