# Store migration and 0.1.0-to-0.2.0 upgrade

This document describes two different changes: upgrading the specification syntax and changing an implementation's feature store. Neither operation authorizes a production deployment.

## 1. Upgrade existing documents

Version 0.2.0 intentionally changes deployment syntax. It retains the baseline feature calculations, time rules, logical source relationships, and acceptance expectations from 0.1.0. A 0.1.0 document must be migrated explicitly; changing only `spec_version` is insufficient.

| 0.1.0 field | 0.2.0 replacement | Review required |
| --- | --- | --- |
| `deployment.provider` and `runtime.engine` | Named adapter instances plus `execution` role selectors. | Actual implementation/version, provided roles, and valid configuration. |
| Table `catalog`, `schema`, `table` | `namespace: [catalog, schema]`, `name`, and `format`. | Namespace interpretation by the selected adapter. |
| Source ID/location | Source ID/version, catalog adapter, location, complete column mapping. | Every logical field, type, time role, tenant key, and physical schema. |
| Output location/write mode | Storage adapter/role plus entity, feature/state, and publication mappings. | Full feature-set membership and lossless physical representation. |
| Unscoped capability strings | Adapter/role/capability/reason requirements. | Derived requirements and actual evidence, not copied support claims. |
| Implicit consumer target | `consumer_bindings` to exact consumers and matching targets. | Selected publication surface, request semantics, freshness, and replay. |
| Implicit publication surface | Explicit `offline_snapshot` or `online_store`. | The surface for which visibility was actually observed. |
| Semantic use of metadata extensions | Metadata-only extensions; explicit native bindings where applicable. | Immutable code, capability support, equivalent behavior, and migration notes. |

All complete YAML projects in this archive are already migrated. Their source and feature semantic versions remain unchanged. The original 0.1.0 archive is the comparison source; the new kit does not claim to provide a general automated upgrader for arbitrary external projects.

## 2. Decide what the store change preserves

Changing the physical store should preserve these when meaning is unchanged:

- Entity, source, relationship, feature, feature-set, and consumer IDs/versions.
- Entity population, full tenant/key structure, output grain, types, units, and null/state rules.
- Source availability, revision/tombstone order, valid-time relationships, windows, and historical eligibility.
- Feature-set membership, dependency pins, fitted-artifact digests, and consumer request/freshness/replay policies.
- Independently reasoned golden expectations and their meaning.

Change the deployment identity/version, physical mappings, adapter configuration/versions, implementation locks, and execution evidence as appropriate. A performance improvement can retain a feature version when it preserves the contract. Different window approximation, timestamp precision, late-data policy, type range, default behavior, or publication semantics requires explicit semantic review.

## 3. Produce a migration plan before writes

The future migration planner must produce a concrete artifact containing:

| Section | Required content |
| --- | --- |
| Identity | Source/destination deployments and versions, logical object pins, exact spec/build/configuration identities. |
| Consumers | Every affected logical consumer, its selected surface, request/response contract, and current routing. |
| Capabilities | Required/derived guarantees by adapter/role, support status, evidence scope, blockers, and native replacements. |
| Data | Source snapshots, retained revisions/deletions, artifact hashes, coverage intervals, volumes, and earliest reproducible times. |
| Mapping | Old/new resources, keys, values, states, metadata, precision/encoding constraints, and physical alias checks. |
| Publication history | Immutable original publication identities, origin deployment/surface, observations, ordering, provenance, and available ledger coverage. |
| Execution | Separate transfer/backfill namespaces, idempotency keys, budgets, retries, concurrency, and failure recovery. |
| Verification | Golden cases, representative historical comparisons, value/state/coverage differences, performance/freshness objectives, and acceptance thresholds. |
| Cutover | Destination synchronization point, routing change, health checks, rollback criteria, preserved old resources, and retirement conditions. |

A binding change cannot recover overwritten source revisions or missing publication observations. If the source store cannot export them and retained raw data cannot reconstruct the requested mode, the planner must limit the migration range or reject the request explicitly.

## 4. Preserve two histories distinctly

**Feature recomputation history** answers what could be calculated from eligible retained sources. **Publication history** answers what a logical consumer could actually retrieve from a specific surface. Source availability, offline commits, online synchronization, and serving observations are separate evidence.

When original publication records are faithfully copied, preserve `published_at`, `publication_sequence`, and `publication_id`, with their original deployment/surface provenance in the migration manifest or immutable ledger. Assign a new `materialization_id` to the destination import. Record the destination's actual import/visibility observations separately. An old publication time must not be presented as evidence that the new destination existed or was serving then.

Historical replay across a store migration refers to the preserved logical service's observed history, with its origin retained. The migration must establish continuity of that logical service and of the publication surface. If that continuity or ledger authenticity cannot be established, historical-serving replay is unverified or unsupported. A fresh backfill provides recomputed values; it never manufactures a historical served-value ledger.

The single-surface deployment profile requires consumers to match `publication.surface`. Offline and online consumer migrations use separate deployments and acceptance evidence. A mixed pipeline may stage offline data before online publication, but the offline commit alone cannot satisfy the online guarantee.

## 5. Test and cut over

1. Validate source and destination contracts and reject incomplete or lossy mappings.
2. Probe the exact destination adapters/runtime and complete their capability report.
3. Run the shared goldens, including boundary, null/state, tenant, correction, deletion, dimension, and publication cases.
4. Transfer or backfill a bounded historical slice into a separate destination namespace; preserve immutable source and artifact evidence.
5. Compare complete keys, row counts, values, states, timestamps, coverage, and lineage. Numeric tolerances must not hide missing rows or altered units.
6. Exercise interrupted writes, retries, duplicate submissions, concurrency, late corrections, retention expiry, and CDF/history gaps where applicable.
7. Shadow or dual-read real consumer requests. Verify freshness, failures, and publication consistency through the actual selected surface.
8. Perform the reviewed routing change only after its acceptance conditions and authorization are satisfied. Observe end-to-end reads after switching.
9. Roll back to the preserved prior route if the stated conditions fail. Retire old resources only after the retention and recovery window has passed.

No new real store adapter is required for the first product release. The local alternate-store example proves the abstraction boundary. A real migration claim requires the destination implementation and the above evidence.

## 6. Example 06

The two declarations in [06_store_migration.yaml](../examples/06_store_migration.yaml) share one logical project:

- `fault_prod` selects the illustrative Databricks package for computation, catalog, scheduling, and offline feature storage.
- `fault_alternative_store` keeps Databricks computation/catalog/scheduling, but selects `org.example.fe_spec:mock_store` for offline storage and consumer resolution.

The destination fields `tenant_key`, `asset_key`, `fault_total`, and `fault_status` replace physical names without changing logical keys or feature identity. Original visibility/order/identity are mapped separately from `target_import_id`.

Both declarations are examples with synthetic adapter pins. The capability-report example explicitly remains unverified. The supplied code checks documents and in-memory field mappings; it creates no store, migrates no real records, and performs no cutover.
