# Azure Databricks first: mapping and adapter boundaries

**Azure Databricks is the required first production implementation target.** The specification and semantic core remain feature-store agnostic. The initial implementation owns the Spark SQL/DataFrame computation pipeline and writes Unity Catalog feature tables. Feature Views are an optional, verified lowering of the same logical plan.

This archive supplies a design and acceptance contract. It contains no production Databricks adapter, installed Databricks runtime, provisioned resource, or evidence of a live Databricks run. Public documentation was retrieved on **2026-11-09**. Exact SDK/API versions, workspace availability, and supported configurations must be probed during implementation; illustrative adapter versions are not installed-version evidence.

Databricks documents both declaratively computed Feature Views and application-populated feature tables. Its recommendation to use Feature Views for new cases does not establish equivalence with this specification's source reconstruction and publication semantics. The owned pipeline is the selected initial implementation because it keeps those stages reviewable. See [authoring strategies](REFERENCES.md#ref-databricks-authoring) and [Unity Catalog feature tables](REFERENCES.md#ref-databricks-uc-tables).

## 1. One semantic core, independently replaceable roles

For a greenfield implementation, prefer a Rust core for parsing, normalization, reference/type checks, temporal planning, exact local evaluation, diagnostics, and manifests. An established repository may justify another language while preserving one authoritative semantic model. The production core must contain no Databricks SDK imports or vendor-name dispatch. A thin Python integration may call Databricks APIs; it must not independently redefine temporal, numeric, or null behavior.

The deployment schema selects adapter instances and roles. One Databricks package may implement several roles, but each role has a separate contract and capability result.

| Role | Portable responsibility | Initial Azure Databricks implementation |
| --- | --- | --- |
| `compute` | Lower a logical plan, execute bounded work, and return validated results and execution evidence. | Generated Spark SQL/DataFrame computation. |
| `catalog` | Resolve bound resources, inspect physical schema and permissions, and preserve metadata and lineage. | Unity Catalog integration. |
| `offline_store` | Commit complete snapshots, retain historical values and publication evidence, and support offline retrieval. | Unity Catalog Delta feature tables. |
| `online_store` | Publish and retrieve feature values with explicit visibility, consistency, and freshness guarantees. | Separately verified Online Feature Store and Feature Serving capabilities; deferred from the initial offline acceptance path. |
| `orchestrator` | Schedule and observe runs, retries, dependencies, and operational limits. | A separately verified Databricks job integration when required. |

`execution` selects compute, catalog, and optional orchestration instances. Source bindings, output targets, and consumer bindings name the appropriate adapter/role explicitly. An offline write does not imply online serving support. This revision has no separate `serving` role: endpoint operations, if supported, must be described and evidenced as capabilities of the selected online integration.

Replacing a store must preserve logical feature IDs, semantic versions, dependencies, consumer references, and expected outputs when feature meaning is unchanged. Deployment bindings, physical plans, and their identities can change. A local alternate-store test double checks this separation; it is not a second production adapter.

## 2. Complete source, output, and consumer bindings

A source binding pins the logical source ID and version, selects a catalog-capable adapter, and supplies the complete logical-to-physical `column_mapping`. Resolve that map into the physical plan. Expressions, entities, relationships, temporal rules, and consumers retain logical references.

A generic table location consists of `namespace`, `name`, and `format`. For the first Databricks adapter, the namespace maps to supported Unity Catalog catalog/schema coordinates. That interpretation belongs to this adapter; the portable schema does not require every store to have a three-part namespace. Generic resource URIs remain opaque and credential-free.

An output target pins the feature-set version and identifies a storage adapter and role. Its mappings cover entity keys, feature values, output states, and publication metadata: `as_of`, `published_at`, `publication_sequence`, `publication_id`, and `materialization_id`. Preserve composite tenant keys, missingness reasons, and publication identity. Consumer bindings select the retrieval surface while retaining logical consumer IDs and time requests.

Preflight validates actual physical types, units, columns, keys, snapshots, revision history, access, and retention. Declared primary-key metadata is insufficient: Databricks documents primary, foreign, and unique constraints as informational rather than enforced. Check uniqueness and relationship cardinality explicitly. See [constraints](REFERENCES.md#ref-databricks-constraints).

## 3. Initial implementation path

First demonstrate the fault-count feature through the canonical plan, generated Spark execution, and a Unity Catalog feature table. Add other baseline operators and temporal cases only after differential acceptance. Start with batch computation and offline snapshots; online synchronization and arbitrary custom execution are not prerequisites for this milestone.

| Specification concept | Initial mapping | Required evidence |
| --- | --- | --- |
| Entity population | Explicit population query or retained entity table. | Zero-event entities, tenant identity, availability, and tombstones survive. |
| Source revisions | Ordered queries over retained source history. | Availability filtering precedes revision selection; feature filters follow selection. |
| Historical dimension | Availability-aware reconstruction plus valid-time join. | Both clocks, interval boundaries, and cardinality match the core. |
| Aggregate, recency, lookup | Generated Spark expressions and queries. | Exactness, ordering, null/empty behavior, numeric precision, and time boundaries agree. |
| Derived expression | Lowering from the portable expression AST. | Types, dependency alignment, lazy evaluation, and null behavior agree. |
| Feature set | Complete validated snapshot in a Delta feature table. | Atomic visibility on the declared surface and recoverable publication identity. |
| Training lookup | Verified feature-table lookup or generated historical query. | Timestamp meaning and source/publication constraints remain explicit. |
| Learned/custom transform | Explicit artifact capability outside the initial baseline. | Immutable artifact, split provenance, execution limits, and conformance evidence. |
| SLO, budget, retention | Jobs, storage policy, monitoring, and run manifest. | Actual enforcement and measurements; declarations alone prove nothing. |

Preserve this execution order:

1. Bind immutable source snapshots or another justified reproducible history.
2. Validate row types, keys, and ambiguity conditions.
3. Filter revisions by source availability at feature as-of.
4. Select revisions and apply retractions.
5. Determine the eligible population.
6. Apply fact windows and availability-aware valid-time relationships.
7. Apply business predicates and exact transforms.
8. Validate coverage, grain, values, states, and quality.
9. Commit the complete snapshot and persist required publication evidence.

Reordering requires proof of equivalence. Pushing a category, value, or event-window filter below revision selection can preserve an obsolete revision after a correction.

## 4. Feature Views and native mechanisms

Feature Views may implement portable operations when they preserve the same contract. Compare full behavior with the canonical evaluator before enabling that lowering. Current documentation describes materialization restrictions for batch rolling windows, column selections, derived/UDF features, and streaming features. Streaming correction/backfill behavior also differs by mechanism. These are capability boundaries, not reasons to weaken semantics. See [Feature Views](REFERENCES.md#ref-databricks-feature-views), [API reference](REFERENCES.md#ref-databricks-feature-views-api), and [materialization](REFERENCES.md#ref-databricks-materialization).

In `portability.mode: portable`, reject a feature that cannot preserve its contract. An explicitly native implementation must use `allow_native` and `native_bindings`, including the feature, adapter, immutable implementation artifact, required capabilities, and migration requirements. Ordinary `extensions` remain metadata and cannot bypass validation. A native lowering equivalent to the portable plan does not itself make the authored feature vendor-specific.

## 5. Publication surfaces and historical evidence

### Offline snapshots

For `publication.surface: offline_snapshot`, prove atomic visibility of the complete feature set in the offline store. Prefer a single-table commit for the first path. Preserve materialization/publication identities and recover from a failure between data commit and evidence recording. Readers must not accept a publication with an incomplete required snapshot/evidence relationship.

Do not assert that Databricks lacks multi-table atomic transactions. Current documentation supports them for eligible tables with catalog commits and supported compute. If an implementation uses a separate ledger or several output tables, probe those prerequisites or provide another verified visibility protocol. Offline atomic commits do not establish atomic offline-to-online synchronization. See [ACID guarantees](REFERENCES.md#ref-databricks-acid) and [transaction modes](REFERENCES.md#ref-databricks-transactions).

### Online stores and endpoints

For `publication.surface: online_store`, prove visibility on the actual consumer path. Keep offline commit evidence separate from online visibility evidence. Job completion, a successful `publish_table` call, or an endpoint reporting Ready does not alone establish that every contracted value is visible together.

The documented Online Feature Store offers several synchronization modes and imposes publication and routing limitations. An endpoint cannot choose an arbitrary online-table copy of the same offline table. Check these constraints before migration or publication. See [Online Feature Stores](REFERENCES.md#ref-databricks-online-store).

The reviewed documentation does not establish atomic feature-set visibility across online synchronization and endpoint lookups. Leave that capability unverified until a concrete protocol and live failure/concurrency tests establish it. If the requested contract cannot be met, reject the deployment; do not describe row-by-row synchronization as feature-set atomicity.

`FeatureSpec` is a reusable catalog definition of lookups/functions, not a publication transaction or snapshot certificate. For the initial feature-table path, it references offline tables whose precomputed values require online publication for real-time serving. FeatureSpec/UDF versioning and endpoint dependency updates require explicit lifecycle handling. See [concepts](REFERENCES.md#ref-databricks-concepts) and [serving](REFERENCES.md#ref-databricks-serving).

### Replay and freshness

Databricks point-in-time joins select by a designated timestamp. They do not automatically reconstruct all of this specification's source-availability and publication clocks, and they are distinct from Delta table-version time travel. Document each bound timestamp's meaning. The documented time-series online publication path retains latest values and does not provide online historical as-of lookup; a training `lookback_window` is not an online freshness guard. See [point-in-time joins](REFERENCES.md#ref-databricks-time-series).

Historical replay requires immutable publication values/evidence for the declared surface, semantic versions, set snapshot identity, states, and the specified ordering. A successful probe at time T establishes observed availability at T; it does not prove the exact earlier instant of global visibility. Do not fabricate `published_at` from a schedule, computation time, expected delay, feature timestamp, or first probe and describe it as a proven global commit instant. If required visibility history is absent, replay remains unsupported or not evaluable.

Backfills and corrections preserve earlier publication evidence. A corrected value published today must not become eligible for a historical request merely because its `as_of` is old. A newer eligible null supersedes an older numeric value under the portable replay rules; consumer freshness and missingness checks still apply.

## 6. Capability negotiation and version pinning

Before execution, produce the versioned capability report and record:

- Adapter implementation/version, role, exact runtime/packages, workspace configuration, and mechanisms actually observed.
- Required capabilities from the project and transitive dependencies, including each requirement's reason.
- Supported, unsupported, and unverified outcomes, evidence scope, and execution readiness.
- Complete physical bindings, access prerequisites, history/retention, intended environment, and operational limits.
- Native bindings, semantic differences, and rejected requirements.

`runtime.required_capabilities` expresses requirements. Repeating a capability name in an input or generating plausible code does not prove support. Documentation, local test-double results, generated code, and live target results are different evidence scopes. Initial offline acceptance must not advertise online publication, replay, or serving readiness.

Pin successful versions in the adapter lockfile and execution manifest. Resolve SDK APIs and workspace/runtime prerequisites during implementation, rather than treating documentation minimums or illustrative adapter declarations as installed packages.

## 7. Integration acceptance gates

For every advertised capability:

1. Run reviewed inputs through the canonical evaluator and live Databricks candidate. Compare complete keys, values, states, diagnostics, and selection results under declared numeric tolerances.
2. Exercise late arrivals, corrections moving events outside windows, retractions, late dimension corrections, overlapping intervals, null join keys, tenant collisions, all-null aggregates, and stale/new-null publications.
3. Inspect plans for availability/revision/filter ordering. Matching a small output alone is insufficient.
4. Verify complete source/output/consumer mappings, namespace handling, immutable source identities, key checks, schema changes, and retention/CDF-gap recovery where relevant.
5. Inject failures and concurrent retries around data commit, publication evidence, and online synchronization when supported. Demonstrate idempotence, recovery, and the declared atomicity surface.
6. For online capabilities, query the consumer path during updates and dependency migrations. Detect mixed snapshots, stale values, missing grants, obsolete UDF logic, and misleading Ready status.
7. Run the same logical contract through the local alternate-store test double with only deployment bindings changed. Compare logical identities, values, states, and publication meaning.
8. Record exact code/plan digests, commands, SDK/runtime/workspace identities, source snapshots, manifests, and output digests for the shipped candidate.

If live access is unavailable, finish supported local planning/code generation and produce an explicit unverified target report. Local fixtures and mocks are not a cloud integration pass. This archive itself authorizes no resource provisioning, paid services, access changes, or production publication.
