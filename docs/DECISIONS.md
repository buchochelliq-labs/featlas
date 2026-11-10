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

## D8. Canonical diagnostic precedence (core and generated Spark SQL)

When several contract violations co-occur, both engines report the first of:

1. invalid source rows (sources in id order);
2. reconstruction of the population source (primary-key duplicates, logical-key conflicts or
   revision ties), then population duplicates;
3. reconstruction of every feature's available input sources in plan order (primary input,
   then join targets);
4. per feature in dependency order: for each join in order — invalid intervals, null join keys,
   one-to-one cardinality, multiple matches, unmatched rows (each scanned over all rows); then
   reduce assertions over population entities — null input, null ordering key, lookup tie;
   then runtime errors of that feature (overflow, division by zero, non-finite, empty error);
   then that feature's output and quality rules (D12);
5. quarantine (D12), after every feature has passed its error-level rules;
6. snapshot assembly.

The Python host executes assertions and materializes feature views in exactly this order, and
the differential harness requires the first code to match.

## D12. Quality failure handling (SEM-QUAL-001, SEM-OUT-001)

Every value of a feature is checked against its output type/nullability, `output.constraints`
(minimum, maximum, enumeration) and `quality.checks` in declared order. The outcome of a
violated rule:

| Rule | `on_violation: error` | `on_violation: quarantine` |
| --- | --- | --- |
| type/nullability, constraints | fail (`OUTPUT_TYPE_MISMATCH` / `QUALITY_VIOLATION`) | fail (same) |
| check with `severity: error` | fail (`QUALITY_VIOLATION`) | quarantine finding |
| check with `severity: warn` | warning finding | warning finding |

* **Warnings never change data.** The value, its state and publication are unchanged; the
  finding (feature, check, entity keys, value) is returned with the evaluation
  (`quality_warnings`) and in the host's publication evidence. This is the spec's "a
  monitoring alert threshold MUST NOT silently change feature values or delete failed rows".
* **Quarantine blocks the whole snapshot.** Once every member has passed its error-level
  rules, any quarantine finding stops the run with `QUALITY_QUARANTINED`; nothing is merged,
  so a quarantined value is never published as successful data. The Spark host attaches the
  violating rows to the error (`detail["quarantined"]`) for the operator to keep.
* **Rule-major precedence.** Across a feature's values the first failing *rule* wins (rank 0
  type/nullability, 1 minimum, 2 maximum, 3 enumeration, 4+i declared check i), then population
  order. The core's population order follows source revision order, which Spark does not
  reproduce; ranking by rule lets the generated SQL (one query per rule, in rank order) report
  the same first code.

Covered by `projects/quality/*.yaml` (generated by `projects/quality/derive.py`) in the core
fixtures and the local Spark differential suite (which also compares warnings); live runs 42816483568 and 42847828392 ran the same suite on Databricks serverless (the latter is pinned in the adapter lock).

## D13. Budgets and retention on the target (SEM-BUDGET-001, SEM-DEPLOY-002)

Generated artifacts carry the deployment `budget` and a `retention` contract; the host enforces
what it can measure and says what it cannot:

* `max_scan_bytes`: before anything runs, the summed `DESCRIBE DETAIL sizeInBytes` of every read
  source (an upper bound of the scan) must fit; otherwise `BUDGET_EXCEEDED`, never sampling.
* `max_output_rows`: the snapshot is counted before the MERGE; over the limit, nothing is published.
* A larger budget needs `PublicationAuthorization.budget_override` (explicit, recorded).
* `max_daily_cost`, `max_memory_mib`: no host signal; reported under `not_enforced_by_host`.
  `max_concurrent_runs` is enforced by the generated job (D14) or the workflow concurrency group.
* Retention preflight: a source with no row at or before its declared `earliest_reproducible_at`
  is a **warning** (`RETENTION_UNPROVEN`): a later first row is consistent with deleted history
  but also with a source that simply started later, so it is recorded, not blocking. An existing
  output table without its change data feed is an **error** (`CHANGE_DATA_FEED_DISABLED`) and
  stops the run, because publication evidence is read from it. Declared retention already
  turns `as_of`s before `earliest_reproducible_at` into `insufficient_history` in the core.

## D14. Scheduling (SEM-DEPLOY-003, core.scheduling)

An interval deployment with an orchestrator generates `databricks/job.yml`, a Databricks Asset
Bundle job: UTC Quartz cron on the `every_seconds`/`offset_seconds` grid (intervals that are not
whole minutes dividing an hour, whole hours dividing a day, or one day are refused), one task per
feature set, two retries, `max_concurrent_runs` from the budget, `timeout_seconds` from the SLO.
Each task floors the trigger time to the grid for its `as_of` and derives its publication identity
from it (`<deployment>/<feature set>/<as_of>`, sequence = epoch seconds), so a retried or
duplicated run of the same slot takes the host's retry path and writes nothing new. The job is
created **paused**: deploying the bundle authorizes nothing; unpausing it is the publication
approval. Deployment and runs have not been observed live (the capability stays `unverified`).


## D15. Online store targets on Databricks (SEM-PORT-005, SEM-PORT-009, core.online_publication)

An `online_store` output target is a single-surface deployment of its own (the offline replay
history stays on the `offline_snapshot` deployment). The snapshot is computed exactly as for the
offline table and staged by **one** Delta MERGE in a Unity Catalog feature table keyed by the
entity keys plus `as_of TIMESERIES` (change data feed on): newer rows update, new entities
insert, entities absent from a newer complete snapshot are deleted, a retry changes nothing, and
an older snapshot is refused before computing (`SNAPSHOT_SUPERSEDED`). `featlas_databricks.online`
then publishes the staging table to a Databricks Online Feature Store (`publish_table`,
TRIGGERED) and observes visibility through a Feature Serving endpoint over a FeatureSpec of the
staging table: `published_at` is the first instant at which every sampled key serves this
publication's `publication_id`. The staging commit, a sync-pipeline success or a job end is never
recorded as online visibility. Resource creation (store, online table, FeatureSpec, endpoint)
needs explicit authorization. `core.serving_replay` on the online role is `unsupported`.
Databricks documents that online stores need Lakebase without scale-to-zero while Free Edition's
single Lakebase project is scale-to-zero; whether Free Edition accepts an online store is
therefore an observation of the live platform run, not an assumption.

## D16. Feature Views or Unity Catalog feature tables (adapter configuration `feature_api`)

`feature_api: uc_feature_tables` computes and publishes every feature with generated Spark SQL.
`feature_api: feature_views` additionally emits `databricks/<feature_set>/feature_views.json`:
per member feature, an exact Feature View definition or the reasons it stays on the feature
table. A feature maps only when the Feature View computes the same value: one append-only source
without revisions or tombstones whose availability time **is** its event time (Feature Views
select rows by event time; with a separate availability column, late rows would be counted), a
`[as_of - lookback, as_of)` window (`RollingWindow`), no joins, `Count` (of a non-nullable key
column) / `Sum` / `Avg` / `Min` / `Max` over a plain column with nulls ignored, or a lookup ordered
by the event time (`Last` / `First`; ties are a recorded assumption because the spec fails on
them), a spec filter lowered to `filter_condition`, and non-temporal entity keys. Distinct counts
(only `ApproxCountDistinct` exists), recency, derived expressions, joins and custom/learned
features never map. Feature states, quality policies and publication evidence are not represented
by Feature Views and stay with featlas. Values are compared live (`create_training_set` against
featlas' own snapshot) before relying on them; an empty window served as null where the spec
declares a value is reported separately, never counted as equal.

## D17. Feast as a store (dev.featlas:feast@0.1.0)

Feast stores and retrieves; it does not compute. A Feast deployment computes snapshots with the
Spark SQL lowering and `featlas_feast` publishes them: one parquet file per publication renamed
into the feature view's source directory (a reader sees all of it or none), `published_at`
observed by reading the publication back through Feast and kept beside the data, one publication
per `as_of` (Feast cannot break ties between equal timestamps deterministically without a
non-null created timestamp), feature view TTL = the bound consumer's `max_staleness_seconds`.
Feast's point-in-time join and the featlas consumer API over the same records select the same
publication (tested at, between, before and beyond publications). Online: `materialize` and
`get_online_features`; Feast writes online rows in batches, so atomic online publication is
`unverified`. Historical serving replay is `unsupported` (the join cannot filter by visibility).
Evidence scope is `local_reference` (file offline store, SQLite online store).
