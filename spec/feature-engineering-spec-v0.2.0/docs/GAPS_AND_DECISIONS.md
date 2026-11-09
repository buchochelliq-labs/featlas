# Scope, decisions, and remaining implementation work

Version 0.2.0 is a proposed scalar, tabular feature contract with runnable document and fixture verification. A complete production implementation is future work. The supplied schema describes more than the reference evaluator executes.

## Decisions already made

| Topic | Decision | Reason and consequence |
| --- | --- | --- |
| Interchange format | JSON Schema Draft 2020-12; restricted, JSON-compatible YAML authoring. | Human-readable definitions with a deterministic parse and explicit structural validation. |
| Schema packaging | One authoritative project schema with local definitions and optional wrappers. | Local resolution; no hosted registry dependency. |
| IDs and versions | Unique ID per collection, one version per ID per project; exact feature/version pins. | References are reviewable and reproducible. Multiple simultaneous versions require separate project contexts or a future registry design. |
| Time | UTC instants, integer elapsed-second durations, explicit bounds. | Avoid implicit timezone and calendar interpretations. |
| Source histories | Append-only or explicit versioned records with availability and revision ordering. | Snapshot-only mutable tables cannot claim historical reconstruction. |
| Dimensions | Revision reconstruction plus availability cutoff, then half-open valid-time matching at the left event time. | Prevents both future dimension corrections and wrong business-time matches. |
| Relationships | One-to-one or many-to-one; explicit use; fail on fan-out. | Prevents accidental aggregate multiplication. |
| Expressions | Typed AST with a fixed vocabulary and explicit null/numeric behavior. | Portable semantics without executable strings. |
| Aggregation | Exact count, distinct tuple count, sum, mean, min, max. | Approximate algorithms require a separate error contract. |
| Money | Prefer integer minor units plus an explicit currency/scale unit. | Exact sums; no accidental dollar/cent or cross-currency mixing. |
| Missingness | Distinct coverage, empty, null, outage, and failure behavior. | Zero is a fact that requires evidence, not a fallback for unknown data. |
| Historical selection | Source-as-known reconstruction and observed publication replay are different modes. | A correct recomputation does not prove the value was served historically. |
| Learned/custom code | Typed immutable contracts with required capabilities and acceptance suites. | Schema validity does not verify arbitrary code or fit leakage. |
| Deployment | Logical/physical separation; declared intent and operational constraints. | A deployment object is not authorization to create resources. |
| Product implementation | Recommend a canonical Rust core for a greenfield project; use a thin Python integration only where the host requires it. | A proposal for maintainability and shared semantics, not a language requirement from the user. Existing repository constraints take precedence. |

## Three different meanings of “supported”

1. **Representable:** the schema can encode the contract.
2. **Validated:** a checker can establish the relevant structural or semantic invariant.
3. **Executable:** a target can perform the operation while preserving the contract and producing evidence.

A learned transform may be representable, partially validated, and unsupported for execution. A Databricks deployment may be structurally valid without a working adapter or credentials. Reports must keep these distinctions.

## Work required for a complete product

| Area | What this archive supplies | Additional acceptance before claiming production support |
| --- | --- | --- |
| Parser and document model | Schemas, examples, restricted YAML rules, reference verification. | Robust diagnostics, all profile edge cases, canonicalization, versioned normalized model. |
| Reference/type validation | Normative rule IDs and limited executable checks. | Complete expression arity/type analysis, reference checks, lineage role checks, capability graph, migration policy. |
| Population/coverage | Explicit population histories, source status fixture contract, selected tests. | Production adapters must prove the population and coverage envelope at the requested scope and retained cutoff. |
| Temporal execution | Exact semantic algorithms and synthetic acceptance cases. | All baseline transforms, invalid-row cases, all null policies, timestamp precision, partitioned histories, deterministic replay. |
| Labels | Independent source, entity/time mapping, forward horizon and dataset availability cutoff. | Complete label construction, maturity/censoring behavior, split isolation, and tests for unavailable or late outcomes. |
| Fitted artifacts | Immutable artifact metadata, split identity, training boundaries, typed apply contract. | Actual artifact retrieval/verification, fit provenance, per-fold recipes, training-row identity, serialization safety, and adapter conformance. |
| Custom operations | Digest-pinned implementation contract; no side effects/network; resource ceilings. | Sandboxed execution, truthful determinism, supported formats, review of code and independent golden outputs. |
| Databricks | Intent mapping and capability checklist. | Exact SDK/runtime pins, accessible test workspace, differential conformance, publication/permission/budget/retention checks. |
| Serving replay | Publication record and selection contract. | Durable publication ledger, atomic set commits, observed visibility, freshness policies, no hidden null fallback. |
| Operational governance | Classification, ownership, source retention, deployment contract fields. | Target enforcement and evidence; declarations alone do not establish permissions or compliance. |
| MCP | Proposed tool boundary in the implementation prompt. | Stable structured contracts, time/size limits, capability checks, read-only default, no side effects from validation. |

## Deliberate exclusions from the portable baseline

These are extensions to design, not silent variants of the existing operators:

- Calendar/timezone-aware windows, daylight-saving transitions, business calendars, and calendar-month arithmetic.
- Arrays, vectors, embeddings, nested records, and maps as feature values. A scalar custom contract does not imply support for vector-valued ML features.
- Streaming state machines, out-of-order watermarks, unbounded state, and event-time timer behavior.
- General one-to-many or many-to-many joins and fan-out-safe aggregation semantics.
- Arbitrary SQL/Python as a cross-engine semantic language.
- Approximate distinct counts, sketches, quantiles, probabilistic error bounds, or silent sampling.
- Cross-entity and offset-time feature dependencies.
- Automated fitting, model selection, hyperparameter search, or arbitrary artifact deserialization.
- Automatic cloud provisioning, paid resource creation, production publication, releases, or repository merges.

## Decisions an implementation must record before expansion

For each extension, record the use case, exact field syntax, type behavior, time/availability behavior, null/failure semantics, capability name, target mappings, independently reasoned fixtures, and migration/version impact.

Examples:

- A calendar-month window needs a named timezone and rules for ambiguous/nonexistent local instants.
- A one-to-many relationship needs a declared output grain and a proof that aggregation cannot duplicate upstream facts unexpectedly.
- A vector feature needs element type, dimension, missingness, storage/serving encoding, and comparison rules.
- An approximate aggregate needs a named algorithm/version, seed if relevant, accuracy guarantees, and tests independent of the implementation.
- A fitted artifact registry needs immutable identifiers, split/fold membership, fit data hashes, retention, availability evidence, and authorization controls.

Ordinary `extensions` are metadata-only and never modify baseline meaning. Future language additions need typed syntax and versioning; target-specific implementations use explicit `native_bindings`, preserve the referenced contract, and fail when required capabilities are unavailable.

## Remaining epistemic limits

A passing fixture suite proves its cases, not all possible datasets. Source availability cannot prove historical state without retained revisions. A cost estimate cannot prove a future bill. A target annotation cannot prove enforced uniqueness. A function digest identifies code bytes but does not prove the function is correct. A green report that omits unsupported checks is not a conformance report.

The implementation plan turns these limits into explicit acceptance gates rather than treating them as implied successes.


## 0.2.0 implementation priority and portability limits

Azure Databricks is the required first external production target. Begin with the canonical fault-count plan lowered to Spark SQL/DataFrames and a Unity Catalog feature table, then expand the verified baseline. Feature Views are an optional equivalent lowering; online publication and serving have separate acceptance gates. No second real store is required for the initial release.

The schema now supports independent adapter roles, generic resources, complete source/output/consumer mappings, explicit publication surfaces, native bindings, and a capability-report envelope. Example adapter identities/pins are synthetic. The local alternate-store mapping exercise is not a store implementation or real migration. The report checker establishes document consistency only; it cannot authenticate external evidence or derive the complete production capability plan. Logical canonical hashing, provider configuration schemas, ODCS importing, actual SDK locks, target compilation/execution, consumer routing, and operational migration are future implementation tasks.

See [PORTABILITY.md](PORTABILITY.md), [MIGRATION.md](MIGRATION.md), and [PRIOR_ART.md](PRIOR_ART.md).
