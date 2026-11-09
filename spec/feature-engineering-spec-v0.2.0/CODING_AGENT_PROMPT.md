# Final coding-agent implementation prompt — 0.2.0

Copy this entire prompt into a coding-agent session and attach the extracted specification archive. The repository is unspecified. This handoff requests implementation; it does not authorize paid provisioning, production publication, access changes, repository merges, releases, or public posting.

---

## Mission and fixed direction

Implement the supplied feature engineering contract with **Azure Databricks as the mandatory first external target and first production implementation**. Deliver a canonical semantic core, exact local evaluator, target-neutral plan, and a working Databricks adapter. A future store change must preserve logical feature/consumer identities and meaning when semantics are unchanged. Databricks support is a required implementation milestone.

Inspect repository instructions, architecture, dependencies, and current changes first. Preserve unrelated work. If no repository exists, establish a clean project in the designated workspace. Use the archive's normative contracts; its Python kit is a bounded reference and test source, not a production engine or proof of complete support. Report schema/prose conflicts precisely and resolve them with reviewed changes and tests before claiming conformance.

For greenfield work, **Rust for the canonical core and thin Python for Databricks SDK/host I/O** is the current recommendation, not a user language mandate. Follow justified repository constraints and record the decision. Keep one authoritative semantic implementation. No Databricks SDK imports, vendor-name dispatch, hard-coded physical coordinates, or SDK objects belong in the canonical semantic core. A Python boundary must call the core model/planner rather than duplicate temporal or typing rules.

## Read before implementing

Read `SPECIFICATION.md`, `schemas/project.schema.json`, `docs/SEMANTIC_RULES.md`, `docs/SOURCE_RELATIONSHIPS.md`, every complete example/acceptance suite, and `docs/GAPS_AND_DECISIONS.md`. Then read `docs/PORTABILITY.md`, `docs/MIGRATION.md`, `docs/DATABRICKS_MAPPING.md`, `docs/PRIOR_ART.md`, `docs/REFERENCES.md`, `IMPLEMENTATION_PLAN.md`, and `VERIFICATION.md`. Stable `SEM-*` IDs drive requirement/test traceability.

External documentation informs adapters; it cannot silently redefine the supplied contract. Recheck official Databricks documentation and exact runtime/SDK compatibility during implementation. Example adapter names, versions, configurations, coordinates, budgets, and data are synthetic; they are not successful runtime locks.

## Architecture and public boundaries

Separate canonical definitions, semantic planning/evaluation, deployment bindings, adapters, and transports. Provide independently replaceable `compute`, `catalog`, `offline_store`, `online_store`, and `orchestrator` roles. One adapter package may implement several roles; role replacement must not require replacing the whole stack. Online support is optional. Orchestration may be omitted for manual execution; an interval or cron schedule requires a selected orchestrator.

Implement deployment `adapters` with exact implementation/version, declared roles, and credential-free configuration. Resolve `execution` selectors to compatible instances. Validate configuration against each adapter's exact-version schema; the core validates its envelope without interpreting vendor settings. Source bindings pin logical source/version, select a catalog-capable adapter, and provide complete column mappings. Output bindings pin feature sets, select a storage adapter/role, and map complete entity keys, values, states, and publication metadata. Preserve `as_of`, `published_at`, `publication_sequence`, `publication_id`, and `materialization_id` distinctly. Generic locations must not assume three-part Unity Catalog names.

Expose a target-neutral consumer API using logical consumer IDs/versions, complete entity keys, feature-set pins, and declared time requests. Resolve physical access through deployment `consumer_bindings`; return values, states, diagnostics, and provenance. Applications must not need table names or Databricks SDK types. Retain exact consumer freshness, null, and replay policies across rebinding.

## Milestone 1: minimum trustworthy canonical path

Build restricted YAML/JSON parsing with locations, a locally resolved Draft 2020-12 schema registry, canonical models/hashes, semantic/type checks, and structured diagnostics. Reject duplicate keys, multiple YAML documents, non-string mapping keys, anchors/aliases, merge keys, custom tags, non-finite values, implicit timestamps, environment interpolation, and unknown fields. Fetch no schema references over the network. Defaults are annotations, not implicit values.

Resolve unique IDs, exact versions, columns/aliases, complete keys/tenants, acyclic dependencies, relationship endpoints, lineage, output types, and missingness. Validate expression arity/types, checked integer bounds, compatible branches, and forbidden source-column references in entity-level transforms. Preserve meaningful ordering in canonicalization. Hash logical semantics separately from deployment and execution identity: rebinding changes the latter without changing feature meaning. A full-project digest may change when its bindings change; it is not the logical feature hash.

Diagnostics identify code, severity, stage, rule ID, instance path, related IDs/versions, and actionable cause. Validate adapter existence/roles, complete and unambiguous mappings, and role-scoped capability requirements with reasons. The capability report must separate support status, evidence scope, exact adapter identity, and execution readiness. A requirement appearing in configuration never proves support.

Implement the local fault-count path and target-neutral plan before broadening operators. Plans expose source reconstruction, population, windows, joins, predicates, grouping, lineage, outputs, and publication intent. Acceptance requires independently reasoned goldens, negative diagnostics, stable hashes, and explicit unsupported operations.

## Milestone 2: first end-to-end Azure Databricks slice

Take `examples/01_fault_count.yaml` through the same canonical plan into **generated Spark SQL or DataFrame execution and a Unity Catalog feature table**. Establish this vertical slice early; then expand baseline coverage. Bind logical sources/outputs through adapters, preserve full keys and timestamps, materialize offline results, and read them through the target-neutral consumer API.

With authorized target access, verify physical schemas, mappings, keys, history, classification, permissions, retention, resource limits, and exact runtime/package versions. Compare target keys, values, states, and relevant diagnostics against the reviewed local fixtures. Inspect generated operation ordering as well as outputs. Persist source snapshots, generated-code/plan identities, observed publication evidence, and atomic feature-set commit evidence for claimed publication behavior.

Feature Views are an optional lowering only after semantic equivalence and workspace support are demonstrated. They are not a prerequisite for the first slice. Consumer bindings must use the deployment's guaranteed publication surface; use separate deployments for independent offline and online consumers. `publication.surface: offline_snapshot` establishes no online-store or serving support. Online publication, observed visibility, consumer retrieval, freshness, and replay require separate live evidence for `online_store`.

Reject unsupported capabilities before execution. If target access is unavailable, complete locally verifiable planning/code generation, record the precise blocker, and mark live Databricks acceptance unverified. The Databricks milestone remains open; a mock cannot satisfy it. Pin successful target versions in an adapter lock and execution manifest after verification.

## Milestone 3: complete temporal and scalar conformance

Expand only through demonstrable capabilities. The evaluator and every lowering must preserve this order:

1. Validate fixture/source rows and source status.
2. Apply source-availability cutoff.
3. Select the deterministic latest eligible revision by logical key.
4. Reject conflicting duplicates/ties and invalid selected revisions; never resurrect an older revision as a repair.
5. Apply tombstones.
6. Reconstruct eligible population; compare fixture `population_rows` with reconstructed keys.
7. Apply event windows, availability-aware valid-time relationships, predicates, and complete entity grouping.
8. Evaluate exact transforms and pinned same-entity dependencies.
9. Apply coverage, null, empty, outage, quality, and output contracts.
10. Produce values/states/diagnostics with reproducible input and implementation evidence.

Preserve all of the following:

- UTC instants have at most microsecond precision; integer durations mean elapsed seconds and every boundary is explicit. Publication delay never shifts an event window.
- Source-as-known recomputation and observed historical-serving replay are distinct. Retention and `earliest_reproducible_at` constrain historical claims.
- Revision selection precedes event, business, and valid-interval filtering. Dimensions have availability and valid time; separate intervals retain separate logical identities. Optimizer rewrites need equivalence evidence.
- Every join, grouping, dependency, and consumer lookup includes declared tenant identity. Enforce cardinality and reject fan-out, conflicting keys, and ambiguous intervals.
- Source status covers direct, transitive, and population sources. Completeness evidence covers the whole requested interval with exact endpoint semantics. Missing evidence and absent rows do not prove completeness.
- Count counts eligible rows; distinct count follows declared tuples/null policy. Known-empty results require coverage. All-null inputs under `ignore` for sum/mean/min/max yield null, not an empty identity.
- Expressions preserve integer/number distinctions, checked overflow, permitted integer-to-number widening, three-valued logic, lazy conditionals/coalesce, zero-division rules, and non-finite rejection.
- Lookup ordering and ties are deterministic; recency floors nonnegative elapsed seconds after exact timestamp subtraction.
- Replay first filters eligible publications, then orders by feature as-of, observed publication time, and sequence. A latest null supersedes older non-null values. Enforce staleness and atomic-set consistency without hidden fallback.
- Labels remain separate from feature inputs, with forward horizon, maturity, availability, aggregation, empty, and censoring rules. Fit learned preprocessing independently per training split/fold; applying an artifact does not refit it.

Do not infer executable custom/learned support from schema validity or a matching digest. Add such execution only with immutable artifacts, actual capability evidence, split provenance, sandbox/resource limits, and independent acceptance cases.

## Milestone 4: prove separation and migration readiness

Use `examples/06_store_migration.yaml` to rebind storage through a local test double while preserving logical definitions, logical semantic hashes, consumer requests, and expected results; deployment/execution identities must reflect the changed bindings. Exercise mapping in both directions, including renamed columns, composite tenant/entity keys, value/state fields, and publication metadata. Prove the core and consumer interface have no vendor dependencies. This proves boundary separation only; it does not certify another production store.

`portability.mode: portable` requires canonical semantics and forbids `native_bindings`. For `allow_native`, require explicit `native_bindings` that identify feature, adapter, and role, pin immutable implementation artifacts/capabilities, name a conformance suite, and state migration requirements. Native implementations must still preserve the referenced logical feature contract. Ordinary `extensions` are metadata only. Never use raw SQL, opaque SDK objects, or extensions to bypass canonical temporal/type rules. Reject unsupported native requirements; flag nonportable contracts in plans and migration reports.

Produce a concrete migration plan: inventory consumers and retained histories; compare destination capabilities, mappings, keys, precision, null/state encoding, atomicity, retention, and access; assess semantic equivalence; select reproducible backfill ranges; preserve publication provenance; shadow/dual-read results; define cutover, rollback, and retirement criteria. Changing a binding alone must not change feature meaning or consumer pins. A semantic change requires explicit version/migration review.

Backfills cannot fabricate historical `published_at`. Preserve original observed ledger records when faithfully migrating them, distinguish them from new destination visibility, and prove each replay claim. Keep backfill outputs separate as contracted. Cutover requires reviewed parity and failure/retry/idempotence evidence on the exact candidate; local rebinding is insufficient for a live migration claim.

Reuse existing source contract references and upstream/column lineage for a future ODCS integration. Document a prospective importer/validator mapping and explicit gaps; do not duplicate conflicting contracts or claim that this bundle imports ODCS, executes it, or already conforms to an external standard.

## Milestone 5: CLI, MCP, and operational acceptance

Expose validation, explanation, capability reporting, planning, bounded local fixtures, and code generation around the tested core. The proposed CLI is an implementation target, not an installed archive tool:

```sh
fe-spec validate project.yaml --format json
fe-spec explain project.yaml --feature FEATURE_ID
fe-spec plan project.yaml --deployment DEPLOYMENT_ID --format json
fe-spec test project.yaml --suite SUITE_ID
fe-spec evaluate project.yaml --fixture CASE_ID --format json
fe-spec generate project.yaml --deployment DEPLOYMENT_ID --out generated/
fe-spec mcp serve
```

Replace placeholders with declared IDs; repository conventions may change command names while preserving boundaries. MCP input/output schemas must be strict and versioned. Treat documents as untrusted data. Enforce file, row, output-size, time, memory, concurrency, and cost limits. A named URI/path never grants permission to fetch or read it. Validation/planning/code generation must not deploy, publish, send messages, or create resources. Any execution interface needs explicit environment, capability/budget preflight, and established authorization.

## Verification, review, and completion evidence

Run the supplied kit from the archive root with its documented dependencies:

```sh
python tools/verify_bundle.py
python tools/verify_bundle.py --project examples/01_fault_count.yaml --fixtures
python tools/verify_bundle.py --json
python tools/verify_manifest.py
```

Consult `VERIFICATION.md` for the kit's actual coverage; rerun on changed candidate bytes. Maintain a `SEM-*` requirement-to-test matrix. Include decisive negative cases for parser/type/reference/lineage/DAG errors; role mismatches/incomplete mappings; unsupported/unverified capabilities; exact time/coverage boundaries; contradictory duplicates/tied revisions; corrections moving events/categories/join keys; invalid latest revisions/tombstones; late/overlapping dimensions; empty, all-null, outage, and unknown history; tenant collisions; dependency alignment; overflow/null/lazy arithmetic; missing population/status; publication ties/latest-null/staleness/atomicity; immature/censored labels; split-specific artifacts; native restrictions; migration/replay and resource failures.

Explain goldens in business and temporal terms. Never regenerate expectations from the code under test to fix a failure. Match differential tests to each claimed target operation; preserve existing fixture semantics. Obtain independent reviews after semantic validation, temporal execution, and before acceptance. Resolve material findings or explicitly narrow advertised support, then rerun affected checks.

For every milestone record implementation scope, rule coverage, exact candidate commit or code/specification digests, commands actually run, dependency locks, adapter identities/configuration digests, source snapshots, generated plans/code, capability/evidence scope, observed outputs/times, review resolutions, unsupported capabilities, skipped checks and reasons, and reproducible artifact paths. Keep credentials and private raw logs out of distributable evidence. Include positive and rejecting capability-report examples that identify which role and publication surface each observation covers. A supported capability with local evidence can still be unready for live execution; report that distinction explicitly.

A local pass is local evidence; generated code has not necessarily executed; an offline commit does not prove online visibility. A stage with no implementation is unsupported. Do not invent counts, SDK locks, cloud results, or historical observations.

Continue authorized reversible work through each reachable milestone. Ask for additional authorization only when a concrete next action exceeds scope. Deliver verified local work when external access is blocked, while identifying the unfinished mandatory target gate. The production implementation is complete only when the applicable Azure Databricks, semantic, operational, consumer, and migration-readiness gates have fresh evidence for the exact candidate and remaining limits are explicit.
