# Glossary

**In plain terms:** the words this site uses, in one place.

as_of (*T*)
:   The instant a feature value describes. Windows end at *T*; only rows available by *T* count.

availability time
:   When a source row or revision became knowable (`sources[].time.available_time`). Distinct
    from event time ([Time semantics](../concepts/time.md)).

adapter
:   A named, exactly versioned implementation (`dev.featlas:databricks@0.1.0`) playing one or
    more roles in a deployment.

adapter lock
:   `crates/featlas-adapters/src/databricks_lock.json`: reviewed live runs pinned to the exact
    bytes of `databricks.rs` and `runner.py`; the only way a Databricks check becomes
    `supported_native` ([Capabilities](../concepts/capabilities.md#the-databricks-adapter-lock)).

assumption / question (reverse engineering)
:   A default the drafter took / a gap it needs a human to fill; both are in the drafting report
    ([Drafting](../reverse/drafting.md)).

characterization case
:   An acceptance case whose goldens are a legacy table's own outputs
    ([Characterization](../reverse/characterization.md)).

compatibility view
:   A view reassembling a split table's original wide shape for consumers that cannot move yet.

consumer
:   A request contract for reading feature values: entity inputs, time inputs, selection mode,
    staleness and missing-value policy.

coverage
:   Positive evidence (`source_status`) that a source is complete over an interval
    ([Missingness](../concepts/missingness.md)).

deployment
:   The physical half of a project: adapters, bindings, targets, schedule, publication rules.

deployment identity
:   Hash of the deployment projection plus member feature-set fingerprints; changes with any
    binding.

differential test
:   Running the same case through generated Spark SQL and through the core and comparing both with
    the goldens.

entity / population
:   Who receives a value (e.g. a device) and the source that lists every eligible entity at *T*.

evidence scope
:   What kind of evidence backs a capability status: `declaration`, `static`, `local_reference`,
    `local_mock`, `live_target`.

execution_ready
:   True only when every required check is supported with evidence of adequate scope (live for
    external targets).

feature set
:   Features published together as one atomic snapshot.

fingerprint (logical)
:   `featlas-canon/1` hash of an object's meaning, independent of bindings and documentation
    ([Identity](../concepts/identity.md)).

golden
:   An independently reasoned expected result in an acceptance case.

lowering
:   Translating the target-neutral plan into Spark SQL statements (`databricks.rs`).

materialization_id / publication_id / publication_sequence
:   Identity of one write / of the original publication / its deterministic order.

parity SQL
:   Read-only query comparing a legacy table with regenerated tables at one `as_of`
    ([Regeneration](../reverse/regenerate.md)).

plan digest / logical plan digest
:   Hash of the full plan (bindings, requirements, core version) / of its logical part only.

published_at
:   The observed visibility instant of the commit that made a snapshot visible; never computed
    from schedules.

quarantine
:   A quality outcome that blocks the whole snapshot from publication
    ([Quality policies](../concepts/quality.md)).

replay (historical serving replay)
:   Reading what was actually visible at a request time (`published_at <= requested_at`).

role
:   `compute`, `catalog`, `offline_store`, `online_store`, `orchestrator`.

SEM-* rule
:   A stable rule identifier of the specification (`spec/…/SPECIFICATION.md`,
    `docs/SEMANTIC_RULES.md`).

source-as-known
:   Recomputing from source rows knowable at *T*; also the consumer mode reading the latest
    committed publication for an as-of.

split strategy
:   A way to group a wide feature set into several tables (`tag:domain`, `tag:business_unit`,
    `filter_family`, …) ([Splitting](../reverse/splitting.md)).

state
:   The label next to each value: `ok`, `empty`, `insufficient_history`, `source_unavailable`,
    `not_published`.

tombstone
:   A revision marking a record deleted.
