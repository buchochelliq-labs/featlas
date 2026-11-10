# Databricks guide

**In plain terms:** featlas turns a deployment into a list of Spark SQL statements and hands
them to a small Python runner that executes them, in a fixed order, in a Spark session you
provide (Databricks Connect or a job). The runner checks the data against the contract with
"must return zero rows" queries, builds one complete snapshot, writes it to a Unity Catalog Delta
table in **one** `MERGE`, and records the real commit time as `published_at`. Nothing is written
unless you pass explicit publication flags.

!!! warning "Status"
    Live acceptance passed on **Databricks Free Edition (AWS, serverless)**, run 42771706619.
    **Azure Databricks has not been run.** Capabilities report `supported_native` only while the
    adapter sources equal a run pinned in the adapter lock; otherwise `unverified`. Scheduling is
    generated (a paused job) but not observed, so `execution_ready` is `false`. Online serving and
    Feature Views are not implemented. Details: [Status](../status.md).

## Adapter and configuration

Deployments select `dev.featlas:databricks@0.1.0` with roles `compute`, `catalog`,
`offline_store`, `online_store` and `orchestrator`. Its configuration schema
(`crates/featlas-adapters/src/databricks.rs`) requires:

| Key | Value |
| --- | --- |
| `connection_profile` | Name of an externally managed Databricks unified-auth profile (pattern `^[A-Za-z0-9_.-]{1,64}$`); never a credential |
| `feature_api` | `uc_feature_tables` (every feature on a Unity Catalog feature table) or `feature_views` (additionally, exact Feature View definitions for the features that map; see [Stores and feature APIs](stores.md#feature-views-feature_api-feature_views)) |
| `online_store` (optional) | `{name, capacity, serving_endpoint}` of the Databricks Online Feature Store used by `online_store` targets ([Stores](stores.md#online-feature-store-and-feature-serving-role-online_store)) |
| `execution` (optional) | `databricks_connect` or `spark_session` |

Binding preflight (`check_bindings`) requires table locations with a two-part
`namespace: [catalog, schema]` plus `name`, Unity Catalog identifiers (letters, digits, underscore;
max 255) and `format: delta` (diagnostics `DATABRICKS_NAMESPACE`, `DATABRICKS_IDENTIFIER`,
`DATABRICKS_FORMAT`, `DATABRICKS_LOCATION_KIND`). Case-insensitive physical-name collisions are
rejected (TRACEABILITY SEM-PORT-003/004).

Logical types map to Spark as `integer → BIGINT`, `number → DOUBLE`, `boolean → BOOLEAN`,
`string → STRING`, `timestamp → TIMESTAMP`, `date → DATE` (`spark_type`).

## How lowering maps to Spark SQL

`fe-spec generate` (or `featlas.Project.generate`) produces, per feature set,
`databricks/<feature_set>/artifacts.json` (the statements the host executes) and `script.sql` (the
same statements as one reviewable file, with named parameters such as `:as_of`). The sections of
`artifacts.json`, in execution order, for `projects/01_fault_count.yaml` / `fault_prod`:

| Section | Statements (example 01) | Mirrors |
| --- | --- | --- |
| `session` | `SET TIME ZONE 'UTC'`; `SET spark.sql.ansi.enabled = true` (checked BIGINT overflow, strict casts) | SEM-TIME-003, SEM-TRANS-006 |
| `bind_sources` | `fl_raw_<source>` temp views selecting every mapped column `AS` its logical name (rename only) | SEM-PORT-003 |
| `stages` | per source: `*_available` (`available_at <= :as_of`), `*_deduped` (`SELECT DISTINCT` for `collapse_identical`), `*_selected` (immutable events, or `ROW_NUMBER() OVER (PARTITION BY logical key ORDER BY revision order)` then tombstones for versioned sources); `fl_population_<set>` | Stages 2–6, SEM-REV-001 |
| `features` | per feature a **computed** view, a **degraded** view (typed null with `:coverage_state`, no source reads) and a `quality` list of rule statements in rank order ([Quality policies](../concepts/quality.md)) | Stages 7–9, SEM-QUAL-001 |
| `checks` | zero-row assertions, each with a stable `code` and `rule`; any returned row is a violation | SEM-SRC-003 |
| `snapshot` | `fl_snapshot_<set>`: every population key once, every member value and state, `feature_as_of`, `published_at = NULL`, publication identities | SEM-PORT-009 |
| `create_table` | `CREATE TABLE IF NOT EXISTS … USING DELTA` with `PRIMARY KEY (… feature_as_of TIMESERIES, publication_sequence)` and table properties | — |
| `table_properties` / `expected_table_properties` | `SHOW TBLPROPERTIES`, expected `featlas.feature_set` and `featlas.feature_set_fingerprint` | SEM-PORT-008 |
| `publish` | one `MERGE INTO … WHEN NOT MATCHED THEN INSERT *` keyed on (entity keys, `feature_as_of`, `publication_id`) | SEM-PORT-009 |
| `evidence` | `commit_candidates` (`DESCRIBE HISTORY`), `commit_history` (`table_changes` over one version), `record_published_at` (`UPDATE … SET published_at`) | SEM-REPLAY-003 |
| `consumer_read` | `SELECT * … WHERE feature_as_of <= :max_as_of AND (keys) IN (SELECT … FROM fl_request_keys)` | SEM-PORT-005 |

Further lowering invariants (`databricks.rs` module docs):

* event-window filtering happens on selected revisions only, so a predicate is never pushed below
  revision selection;
* joins use complete key pairs (tenant keys included) and half-open valid intervals;
  cardinality, null-key, unmatched and interval checks are separate zero-row assertions;
* `and`/`or`/`if_else`/`coalesce` lower to ordered `CASE`/`COALESCE`, so errors in unselected
  branches are not raised; division guards zero explicitly; number results are checked finite;
  runtime violations use `raise_error('FEATLAS:<CODE>: …')`, which the host maps back to the code
  (`python/featlas_databricks/errors.py`);
* aggregates are left-joined to the population so zero-event entities survive.

A generated computed feature view (example 01):

```sql title="script.sql (excerpt)"
CREATE OR REPLACE TEMP VIEW fl_feature_device_fault_count_7d AS
WITH fl_agg_rows AS (
  SELECT `faults`.`tenant_id` AS `tenant_id`, `faults`.`device_id` AS `device_id`, count(*) AS fl_agg
  FROM fl_fault_events_ai_selected `faults`
WHERE unix_micros(`faults`.`occurred_at`) >= (unix_micros(:as_of) - 604800000000) AND unix_micros(`faults`.`occurred_at`) < unix_micros(:as_of)
  AND coalesce((`faults`.`is_fault` = true), false)
  AND `faults`.`tenant_id` IS NOT NULL AND `faults`.`device_id` IS NOT NULL
  GROUP BY `faults`.`tenant_id`, `faults`.`device_id`
)
SELECT p.`tenant_id`, p.`device_id`,
  CASE WHEN a.fl_present IS NULL THEN CAST(0 AS BIGINT) ELSE CAST(a.fl_agg AS BIGINT) END AS fl_value,
  CASE WHEN a.fl_present IS NULL THEN 'empty' ELSE 'ok' END AS fl_state
FROM fl_population_device_reliability p
LEFT JOIN (SELECT *, 1 AS fl_present FROM fl_agg_rows) a ON p.`tenant_id` <=> a.`tenant_id` AND p.`device_id` <=> a.`device_id`;
```

A zero-row assertion:

```sql
-- [fl_fault_events_ai_primary_key_duplicates] DUPLICATE_CONFLICT
SELECT `event_id`, count(*) AS n FROM (SELECT DISTINCT `event_id`, `tenant_id`, `device_id`, `occurred_at`, `available_at`, `is_fault` FROM fl_fault_events_ai_available) GROUP BY `event_id` HAVING count(*) > 1;
```

`artifacts.json` also carries `notes` stating known engine differences, quoted from the generated
file:

* "Number sums/means use Spark binary64 accumulation; the local evaluator is correctly rounded.
  Declared numeric tolerances govern comparison; integer results are exact (BIGINT, ANSI
  overflow)."
* "Integer/number comparisons follow Spark's widening to DOUBLE, which is inexact above 2^53."
* "String ordering assumes UTF8_BINARY collation (code-point order); preflight must confirm the
  table/session collation."
* "Declared primary keys, foreign keys and cardinalities are verified by the generated assertion
  queries; Unity Catalog constraints are informational."
* "Coverage/outage states are computed by the core from source-status evidence; this SQL never
  infers completeness from row counts."

## The host runner

`python/featlas_databricks/runner.py::SnapshotRun(spark, artifacts, as_of, coverage,
unavailable_sources, source_relations=None, population_source=None)` executes the artifacts:

1. **`prepare()`**
    * fails fast with `SOURCE_UNAVAILABLE` (SEM-ENT-001) if the population source is unavailable;
    * drops only this artifact's own `fl_*` temp views left from an earlier run in the session
      (review item R5);
    * runs the `session` statements;
    * binds each readable source. Against real tables it pins one immutable Delta snapshot per
      source: `DESCRIBE HISTORY <table> LIMIT 1`, then rewrites the binding to
      `FROM <table> VERSION AS OF <version>` and records `source_versions`, so assertions, the
      snapshot and the MERGE read the same rows (review item D3). With `source_relations`
      (used by the differential harness) it substitutes those relations instead;
    * creates the reconstruction and population views.
2. **`execute()`**: untagged assertions (row validity, reconstruction, population) in generated
   order; then per feature in plan order: its tagged assertions, its computed or degraded view
   (as decided by the core's coverage `variant`), a `count()` that materializes it so runtime
   errors surface before later features, then its quality rules in rank order: the first `error`
   rule with rows raises its code, `warning` rows are recorded in `quality_warnings`, and
   `quarantine` rows are collected and raise `QUALITY_QUARANTINED` (with the rows in
   `detail["quarantined"]`) after every feature has passed (DECISIONS D8, D12).
3. **`materialize(publication)`**: builds and collects the snapshot view.
4. **`publish(auth)`**: the only method that writes (below).

`preflight_sources()` compares `DESCRIBE TABLE` of each bound source with
`expected_source_schemas` (column present, exact Spark type) and returns the problems.
Every statement is logged by name and parameter names in `run.log`; parameters are passed as
named Spark SQL arguments and an unbound one raises `UNBOUND_PARAMETER`.

## Publication protocol

`SnapshotRun.publish(PublicationAuthorization(environment, approved_by, allow_create_table,
publication_id, publication_sequence, materialization_id))`:

1. Refuses with `PUBLICATION_NOT_AUTHORIZED` unless environment, approver, publication id and
   materialization id are all non-empty.
2. With `allow_create_table`, runs the generated `CREATE TABLE IF NOT EXISTS`.
3. Reads `SHOW TBLPROPERTIES` and refuses with `TARGET_TABLE_MISMATCH` unless
   `featlas.feature_set` and `featlas.feature_set_fingerprint` equal the expected values (D8 in
   EVIDENCE).
4. **Looks up an earlier attempt first**: groups existing rows of this `publication_id` by
   (materialization id, sequence, same as-of). More than one group, or a different as-of or
   sequence, is `PUBLICATION_IDENTITY_CONFLICT`. If one matching group exists, this is a retry:
   nothing is recomputed or written (review item R4).
5. Otherwise runs `prepare → execute → materialize`, records the table version before the write,
   best-effort sets `spark.databricks.delta.commitInfo.userMetadata` to the materialization id
   (serverless may reject this; evidence does not depend on it), and runs the single `MERGE`.
6. **Finds the inserting commit** from the retained history: `DESCRIBE HISTORY`, keep `MERGE`
   operations (after a fresh write, only versions newer than the pre-write version), newest first,
   and for each run `table_changes(table, v, v)` filtered to `_change_type = 'insert'` and this
   `publication_id`. Versions whose change data is unreadable are skipped and reported. No hit is
   `PUBLICATION_EVIDENCE_MISSING`; `published_at` stays NULL.
7. Requires the inserting commit to hold exactly the publication's row count (atomicity), else
   `PUBLICATION_EVIDENCE_MISSING`.
8. `UPDATE … SET published_at = <_commit_timestamp> WHERE publication_id = … AND published_at IS
   NULL` and returns evidence: table, version, commit timestamp (microseconds via `unix_micros`),
   stored identities (on a retry, the original write's), rows, `retry`, source versions and
   `quality_warnings` (only for the computing run), `evidence_source: "change data feed (table_changes)"`, approver,
   environment and the statement log.

Because readers never replay a row whose `published_at` is NULL, a crash between steps 5 and 8
cannot fabricate visibility, and re-running finishes step 8 ([Publication and replay](../concepts/publication.md)).

!!! note "Evidence source changed since D6 was written"
    [DECISIONS.md](../DECISIONS.md) D6 describes the commit lookup through `DESCRIBE HISTORY`
    and `userMetadata`. The current runner and generated statements read the inserting commit
    from the change data feed over retained `MERGE` versions, with `userMetadata` only as a
    best-effort label; the generated `CREATE TABLE` sets `delta.enableChangeDataFeed = true`.

## Running it: `featlas-databricks`

The package installs a console script (`python/pyproject.toml`):

```sh
# featlas is not on PyPI yet (no release has been made): build and install the wheel
# (see Getting started), then add the Databricks client.
pip install databricks-sdk databricks-connect
featlas-databricks projects/01_fault_count.yaml --deployment fault_prod \
  --feature-set device_reliability --as-of 2026-06-08T00:00:00Z --status status.json
```

| Flag | Meaning |
| --- | --- |
| `--deployment`, `--feature-set`, `--as-of` | What to compute; `--as-of` is a UTC instant ending in `Z` |
| `--status` | JSON file of source-status records ([Missingness](../concepts/missingness.md)) |
| `--publish` | Write; without it the run is a **dry run** that computes the snapshot and assertions and prints the row count, statement log and plan digest |
| `--environment`, `--approved-by`, `--publication-id`, `--publication-sequence`, `--materialization-id` | Publication authorization and identities |
| `--allow-create-table` | Also run the generated `CREATE TABLE` |
| `--evidence-out` | Write the returned evidence JSON to a file |

The CLI generates artifacts through the bindings (writing a copy to a temporary directory),
computes coverage with the core,
opens a session with `DatabricksSession.builder.profile(<connection_profile>)`, runs
`preflight_sources()` (exit code 1 with the problems if any) and then the dry run or
publication. Credentials are resolved by the Databricks SDK from the named profile; none are
accepted on the command line (`cli.py` docstring).

!!! note "Extras are not pinned"
    The `databricks` extra lists `databricks-sdk` and `databricks-connect` without versions
    ("Pinned only after a successful live target run", `pyproject.toml`). The live run used
    `databricks-connect==17.3.15` and `databricks-sdk==0.164.0`
    (`docs/evidence/databricks-live/pip-freeze.txt`); the live workflow installs
    `databricks-connect>=16.4,<18`.

## Label datasets

For every label consumer of a planned feature set, `generate` also writes
`databricks/labels/<consumer>/artifacts.json` (`lower_labels`), returned by the API and the
bindings as `label_artifacts`. The core decides which requests are mature and complete
(`label_requests`); `featlas_databricks.labels.LabelRun` registers the eligible requests as
`fl_label_requests`, reconstructs the label source once per distinct dataset cutoff and reads the
generated values query. See [Labels](../concepts/labels.md).

```python
from featlas_databricks import Core
from featlas_databricks.labels import LabelRun

core = Core()
gen = core.generate("projects/03_commerce_ratio.yaml", "commerce_prod")
art = gen["label_artifacts"]["spend_training_dataset"]
requests = core.label_requests("projects/03_commerce_ratio.yaml", "spend_training_dataset", rows, source_status)
dataset = LabelRun(spark, art, requests).run()   # same shape as Project.labels(...)
```

(`rows` and `source_status` are the consumer request rows and the label source's status record;
this mirrors `test_generated_label_sql_matches_goldens_and_core`.)

## Consumer reads

To serve a consumer from Databricks, the host registers the requested keys as the temp view
`fl_request_keys`, runs the generated `consumer_read` with `:max_as_of`, and passes the records to
the core (`fe-spec consume --records`, or `Project.consume(request, records)`). The core does all
selection, freshness, null and replay logic; `differential.py::_replay_case` is a working example.

## Unity Catalog requirements and grants

From `.github/workflows/databricks-live.yml` (live run setup):

* A catalog the run may use (the live variant uses `featlas`; `projects/derive.py` writes
  `projects/live/*.yaml` with `example_catalog` replaced by `featlas`, deployment bindings only).
* The principal needs `USE CATALOG` on the catalog and `USE SCHEMA`, `CREATE TABLE`, `SELECT`,
  `MODIFY` on `featlas.synthetic_raw` and `featlas.synthetic_features` (plus `CREATE SCHEMA` on
  the catalog if those schemas do not exist).
* Feature tables are Delta with change data feed enabled (generated DDL). The `PRIMARY KEY …
  TIMESERIES` constraint is informational; uniqueness is checked by the generated assertions.
* For other deployments, `access.required_permissions` in the project declares the intended
  read/write/create privileges; declaring them grants nothing and featlas does not probe grants
  (SEM-DEPLOY-002, TRACEABILITY).

## Authentication and OAuth scopes

The live tests authenticate a service principal with OAuth machine-to-machine through the
Databricks SDK's unified-auth environment variables `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET` (`python/tests/spark_session.py`); no credential is read by featlas
code. `DATABRICKS_SCOPES` (space or comma separated, non-secret) is passed to
`databricks.sdk.core.Config(scopes=…)`, because the SDK otherwise requests `all-apis`, which a
secret bound to narrower scopes rejects. The workflow defaults it to `databricks-connect`: the
suite talks only to serverless compute over Spark Connect, and table access is governed by Unity
Catalog grants. The scopes list is converted to a tuple because Databricks Connect caches sessions
keyed by a hash of the SDK config values. The session uses `serverless_compute_id="auto"`.

## The live workflow

`.github/workflows/databricks-live.yml` (`workflow_dispatch`, and pull requests touching the
Databricks lowering/caps, the core, the host, `python/tests/**`, `projects/live/**` or the
workflow), one run at a time, 45-minute timeout, GitHub environment `databricks-free`:

1. check that host, client id and secret are configured (names only);
2. build `fe-spec` and the wheel; install `databricks-connect>=16.4,<18` (which provides the
   pyspark client — pyspark is not installed alongside);
3. record client versions (`pip-freeze.txt`), the commit and `fe-spec` SHA-256 (`commit.txt`)
   and the SHA-256 of `databricks.rs` and `runner.py` (`sources.sha256`);
4. run `tests/test_local_spark_differential.py` against serverless
   (`FEATLAS_SPARK=databricks`), writing `databricks-differential.json`;
5. run `tests/test_databricks_live.py`, writing `databricks-live.json`;
6. upload `live-evidence/` as the artifact `databricks-live-evidence`.

### The platform job

On `workflow_dispatch` (input `platform`, default true) a second job, `platform`, runs
`tests/test_databricks_platform.py` against the same workspace with OAuth scopes from the
environment variable `DATABRICKS_PLATFORM_SCOPES` (default `databricks-connect jobs files
unity-catalog pipelines model-serving model-serving-inference`). The service principal's secret
must allow those scopes, and the principal needs `CREATE VOLUME`, `READ VOLUME`, `WRITE VOLUME`
and `CREATE FUNCTION` on `featlas.synthetic_raw` / `featlas.synthetic_features` in addition to
the grants above. It builds an aarch64 (manylinux 2_28) and an x86_64 wheel, then:

* **scheduled job** — creates the generated `databricks/job.yml` of `fault_job` through the Jobs
  API (`featlas_databricks.jobs.job_settings` substitutes only the bundle variables: wheel and
  project in a Unity Catalog volume, source-status table), runs it once on demand, unpauses it
  for one 5-minute slot to observe a `PERIODIC` run, pauses and deletes it, and checks the
  slot-derived publications and their equality with an interactive run;
* **online** — stages `fault_online`, publishes to the Online Feature Store `featlas-free`,
  observes the publication at a Feature Serving endpoint and removes the endpoint, FeatureSpec,
  online table and store;
* **Feature Views** — compares `create_training_set` values of the mapped features with featlas'
  snapshot on run-unique tables.

Each step and any platform refusal is recorded in `platform-evidence/databricks-platform.json`.

## Evidence of run 42771706619

Recorded in [RUN.md](../evidence/databricks-live/RUN.md) and the JSON files beside it (artifact
zip SHA-256 `3c76096038fa6672a7947801097751a0768d8baecf614b89c1e2cbde9d2957de`):

| Item | Value |
| --- | --- |
| Target | Databricks Free Edition (AWS), serverless, Unity Catalog catalog `featlas` — not Azure |
| Tested commit | `5e9d94e8f258e38206a294ac0ee4fd653a78c6d9` |
| Runtime | `19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0` |
| Client | `databricks-connect==17.3.15`, `databricks-sdk==0.164.0`, Python 3.12.16; OAuth M2M, scope `databricks-connect` |
| Differential | 49 passed: 46 reference cases (projects 01, 02, 03, 05, 06) vs goldens **and** the core, 2 precedence tests, 1 rejection test; fixture sources as session temp views |
| Publication | 7 passed on run-unique Unity Catalog tables (dropped at the end) |

The publication suite observed: schema preflight with no problems; DDL with the `PRIMARY KEY …
TIMESERIES` constraint; one MERGE (table version 1) of 2 rows with sources pinned `VERSION AS OF
0`; `published_at` equal to the inserting commit's `_commit_timestamp` from the change data feed;
consumer batch read and replay (nothing one microsecond before the commit, values at the commit);
idempotent retry; `PUBLICATION_IDENTITY_CONFLICT`; crash recovery; `TARGET_TABLE_MISMATCH`. The
recorded `logical_plan_digest` (`667733be…`) is the same as the one generated for
`projects/01_fault_count.yaml`, because the live variant changes deployment bindings only.

Not exercised: Azure Databricks, classic clusters, retention/VACUUM, grants beyond the test
principal's, scheduling, online serving.

## Re-pinning the adapter lock

A live run vouches for the adapter only while `databricks.rs` and `runner.py` are byte-identical to
what it tested (see [Capabilities](../concepts/capabilities.md#the-databricks-adapter-lock)). After
changing either file:

1. Run `.github/workflows/databricks-live.yml` on the change and confirm both suites pass.
2. Download the `databricks-live-evidence` artifact unchanged, record its zip SHA-256, and commit
   the files under `docs/evidence/databricks-live/` with a `RUN.md` (no credentials, hosts or
   workspace identifiers).
3. Append a `verified_runs` entry to `crates/featlas-adapters/src/databricks_lock.json`: run id and
   URL, artifact SHA-256, tested commit, `observed_at`, target and client versions from the
   evidence, `sources_sha256` from the artifact's `sources.sha256`, each evidence file's path,
   SHA-256 and summary, and the `verified` `(role, capability, evidence)` list — only pairs the run
   actually exercised.
4. Run `cargo test`: `databricks_caps::tests::lock_is_consistent` checks that each run pins exactly
   the vouched-for sources and names existing evidence, and `evidence_files_match_the_lock`
   re-hashes the committed evidence files.
5. `fe-spec capabilities … --deployment fault_prod` should then report `supported_native` /
   `live_target` for the verified pairs. `core.scheduling` stays unverified (never vouched for).

The newest matching run wins (`current_run` searches `verified_runs` from the end). These steps are
derived from the lock format and its tests; there is no script that automates them.

## Azure Databricks

The [runbook](../DATABRICKS_RUNBOOK.md) lists the prerequisites and procedure for the Azure gate
(workspace with Unity Catalog, compute, grants, source-status evidence, budget approval) and the
evidence to persist. It has not been executed.
