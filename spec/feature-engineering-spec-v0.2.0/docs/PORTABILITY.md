# Portability and adapter contract

**Azure Databricks is the required first production implementation.** The authoring language and canonical semantic core remain independent of the feature store. This document explains the 0.2.0 deployment structure; the normative requirements are `SEM-PORT-*` in [SPECIFICATION.md](../SPECIFICATION.md).

## 1. What is portable

Entities, populations, sources, relationships, feature calculations, feature-set membership, consumer requests, time semantics, missingness, units, types, and golden expectations are logical contracts. A physical store change does not rewrite them when their meaning is unchanged. Source provenance may cite an upstream system or repository, but those citations are not execution bindings.

Deployments contain adapter selection, physical locations, field mappings, SDK integration choices, schedules, authorization intent, and operational constraints. Backend-specific objects, SQL dialects, partitioning, clustering, workspace coordinates, and online endpoint configuration stay here or in generated adapter artifacts. A generated Databricks query is an implementation of the logical plan, not the authoritative feature definition.

Portability is conditional on the destination preserving the required semantics and having the required data/history. A valid contract does not establish that a destination adapter exists or supports it. A change of storage engine can require data transfer, reconstruction, permission changes, online synchronization, and operational cutover; it is not necessarily a one-line operational migration.

## 2. Independent roles

| Role | Responsibility | Initial Databricks path |
| --- | --- | --- |
| `compute` | Lower and execute the canonical plan; preserve evaluation order, types, and resource limits. | Generated Spark SQL/DataFrame computation. |
| `catalog` | Resolve bound datasets, inspect schemas/history, read source snapshots, and manage target metadata/permissions. | Unity Catalog and supported source interfaces. |
| `offline_store` | Write and retrieve feature snapshots; preserve keys, states, history, retention, and publication evidence. | Unity Catalog feature tables backed by Delta. |
| `online_store` | Materialize and retrieve online values through a specified integration, including its serving endpoint when needed. | Optional online/Feature Serving integration, only after separate evidence. |
| `orchestrator` | Scheduling, retries, run tracking, and bounded execution. | Databricks job integration when implemented. |

Roles describe interfaces, not a requirement for five processes. An adapter instance may provide several roles. A package can expose separate instances when configuration or evidence differs. Online support is optional. A null orchestrator is valid for manual execution; an interval/cron schedule requires a selected orchestrator.

The canonical core must neither import vendor SDKs nor dispatch on vendor names. A registry resolves a namespaced adapter implementation and its exact version. Unknown implementations can be structurally represented, but an unresolved implementation or configuration schema blocks target planning/execution. Configuration schemas belong to the adapter version; arbitrary keys accepted by the core envelope are not automatically valid adapter settings.

## 3. Deployment fields

| Field | Meaning |
| --- | --- |
| `adapters[]` | Local `id`, namespaced `implementation`, exact `version`, requested `roles`, and credential-free `configuration`. |
| `execution.compute` | Selected compute adapter ID. |
| `execution.catalog` | Selected catalog adapter ID for deployment metadata operations. Source bindings may select another declared catalog-capable adapter. |
| `execution.orchestrator` | Selected scheduler ID, or null for manual execution. |
| `source_bindings[]` | Exact logical source/version, catalog adapter ID, read-only physical location, and complete `column_mapping`. |
| `output_targets[]` | Exact feature set/version, storage adapter/role, physical destination, write intent, complete key/value/state/metadata mappings. |
| `consumer_bindings[]` | Exact logical consumer/version and storage adapter/role with a matching output target. |
| `runtime.required_capabilities[]` | Adapter ID, role, capability name, and requirement reason. The planner also derives requirements from the actual operations. |
| `publication.surface` | One exposed guarantee: `offline_snapshot` or `online_store`. Consumer bindings must use that surface. |
| `portability` | Portable mode or explicit native implementations with replacement/conformance obligations. |

Names such as `org.example.fe_spec:databricks` and versions such as `0.0.0-example` in the examples describe a proposed adapter interface. They are not installed packages, official Databricks package identifiers, or verified SDK versions. The implementation must resolve real packages and record exact successful runtime/build identities.

## 4. Physical mappings must preserve meaning

A source mapping covers exactly every declared logical column. A mapping renames fields only. It cannot cast a string to an integer, convert units, change a timestamp interpretation, invent a default, or drop a tenant key. An upstream normalization can perform such work only through its own reviewed, versioned contract and provenance.

An output target maps every entity key, every pinned feature value, every feature state, and these metadata fields:

- `as_of`: feature evaluation instant.
- `published_at`: actual visibility evidence for the publication's specified origin and surface.
- `publication_sequence`: deterministic original publication ordering.
- `publication_id`: stable identity of the original publication.
- `materialization_id`: identity of the current write or import operation.

All mapped physical fields must be distinct. States remain explicit: null, zero, empty/default, and unavailable outcomes must not collapse into one representation. Adapters must additionally check case folding, reserved words, encodings, timestamp precision, numeric range, and physical aliases against the real backend.

`kind: table` uses an arbitrary ordered `namespace`, a `name`, and a `format`. Databricks can interpret a two-component namespace as catalog/schema. `kind: resource` uses an opaque URI and a format identifier for stores without SQL table coordinates. `kind: file` and read-only `kind: fixture` are also defined. A format label does not distinguish two resources at the same address. Repeated physical output destinations are prohibited in this profile; fixture locations cannot be output destinations. Source and output aliases that require backend resolution must be checked by the adapter before execution.

Within one deployment there is one target per feature-set version and storage role. Multiple surfaces can have staging targets, but the deployment guarantees one exposed publication surface and binds consumers only to that surface. Use separate deployments for independently guaranteed offline and online consumption. Offline commit evidence never satisfies an online visibility requirement.

## 5. Capability and readiness evidence

The schema [capability-report.schema.json](../schemas/capability-report.schema.json) records the exact project-file SHA-256, project/deployment identity, adapter roster, runtime/build identities, checks, evidence scope, and readiness.

| Result | Interpretation |
| --- | --- |
| `supported_native` | The selected implementation preserves the contract through a native mechanism, with scoped evidence. |
| `supported_emulated` | Additional implementation preserves the same contract, with scoped evidence and limitations. |
| `unsupported` | The target cannot satisfy the requirement in the assessed configuration. |
| `unverified` | Support has not been established. |

Evidence scopes distinguish a declaration, static inspection, local reference execution, a local mock, and a live target. Local support does not make an external target execution-ready. External readiness requires live evidence for every mandatory capability, exact implementation digests/runtime identities, all prerequisite checks, and evidence for guarantees composed across roles. Required operations may not disappear simply because the author omitted a capability string. The production planner must derive them.

The bundled checker verifies report structure and internal consistency against the project, not the truth or authenticity of remote evidence. It does not fetch evidence URIs, probe adapters, or independently attest cloud execution. The example Databricks report deliberately records unverified support and `execution_ready: false`.

## 6. Native implementation boundary

`extensions` is metadata-only. It cannot contain an executable override or change baseline meaning. `portability.mode: portable` prohibits `native_bindings`.

`allow_native` permits a reviewed binding that names a feature/version, adapter/role, immutable implementation reference and digest, required capabilities, conformance suite, and migration notes. Each named capability must be declared for the same adapter/role. The suite must contain cases for the pinned feature. A native implementation must still satisfy the logical contract. If the intended behavior changes, create an explicit semantic version or a future language construct; do not reinterpret an existing operator.

Compiler-produced native SQL is ordinary adapter lowering when it is generated from the canonical plan and proven equivalent. A user-supplied SQL/Python artifact is an explicit native/custom boundary. Unsupported bindings fail planning. Native mode is a portability warning, not permission to execute arbitrary code.

## 7. Consumer API and hash scopes

The future public API accepts logical consumer ID/version, entity keys, and the declared time inputs. It resolves feature-set pins and target bindings internally, returning values, states, diagnostics, and provenance. Applications do not supply Unity Catalog table names or SDK objects to this API. Native SDK exports can coexist as explicitly nonportable integrations.

Keep four identities separate: logical contract identity; full project/deployment identity; implementation identity; and run/publication identity. The report's `project_file_sha256` hashes the exact source-file bytes. It is intentionally binding-sensitive. A logical semantic fingerprint excludes deployments and physical configuration and must use the core's specified, versioned canonicalization. The production implementation must freeze and test its numeric and ordering behavior before hashes are used as compatibility evidence. This kit compares the logical objects directly and does not claim to implement that future canonical hash algorithm.

## 8. Evidence supplied by this archive

[Example 06](../examples/06_store_migration.yaml) keeps Databricks compute/catalog selected while replacing the offline store and renaming output fields. Logical entities, sources, relationships, features, feature sets, consumers, and golden expectations match example 01. Local mapping helpers exercise field mapping without I/O or another semantic engine.

This establishes schema/reference consistency and local boundary behavior. It does not establish a second real store adapter, a running Databricks deployment, source-data transfer, backfill correctness on production data, or successful cutover. See [MIGRATION.md](MIGRATION.md) for the additional obligations.
