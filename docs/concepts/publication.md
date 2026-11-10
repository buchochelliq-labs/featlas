# Publication and replay

**In plain terms:** recomputing a feature for last Tuesday is not the same as knowing what a
model could actually *read* last Tuesday. featlas records when each published snapshot really
became visible (`published_at`, taken from the storage system's commit, never from a schedule)
and offers two ways to read values back: "the latest published value for this as-of" and
"exactly what was visible at that request time" (replay).

## Publication metadata (SEM-PORT-009)

Every output target maps five metadata fields to physical columns:

| Field | Meaning | Assigned by |
| --- | --- | --- |
| `as_of` | The feature as-of *T* of the snapshot | Request |
| `published_at` | Observed visibility instant of the commit | Recorded from the store after the commit |
| `publication_sequence` | Deterministic original publication order | Host / operator |
| `publication_id` | Stable original publication identity | Host / operator |
| `materialization_id` | Identity of this particular write or import | Host / operator |

A feature set with `publication: atomic` is published as one complete snapshot: every
population key exactly once with every member value and state. `published_at` stays `NULL`
until commit evidence is recorded. It is never derived from schedules, job time or
`as_of + delay` (DECISIONS D6, SEM-REPLAY-003).

## Two selection modes

A consumer's `selection.mode` decides how a request row `(entity keys, as_of, requested_at)`
selects among stored publications (`crates/featlas-core/src/consumer.rs::select`):

=== "source_as_known"

    * Eligible: same entity key and `publication.as_of <= request.as_of`.
    * Publications whose `published_at` evidence is still pending **are** eligible (the snapshot
      is committed and visible).
    * Ordered by `(as_of, publication_sequence)` — the declared tie-breaker — never by missing
      evidence (review item R2 in [EVIDENCE.md](../EVIDENCE.md)).

=== "historical_serving_replay"

    * Eligible: same entity key, `publication.as_of <= request.as_of` **and**
      `published_at <= requested_at`.
    * A publication without recorded `published_at` is never eligible.
    * Ordered by `(as_of, published_at, publication_sequence)` (SEM-REPLAY-002).

In both modes:

* two eligible publications with the same ordering key fail with `PUBLICATION_TIE`; no arbitrary
  choice is made;
* the latest eligible publication wins even if its value is null (a newer null supersedes an
  older value; example 01 case `new_null_supersedes`);
* staleness is checked **after** selection: if `request.as_of − selected.as_of` exceeds
  `max_staleness_seconds`, the result is `STALE_FEATURE` (SEM-FRESH-002), with no fallback to an
  older value.

!!! note "Records versus the D4 table"
    `DECISIONS.md` D4 describes source-as-known ordering as `(as_of, published_at, sequence)`;
    the later review fix R2 changed it to `(as_of, sequence)`, which is what the code does.

## Example: replay excludes a later publication

Two publications exist for the same key and as-of; the second became visible at 00:10. A replay
request at `requested_at = 00:05` must see only the first. Records (physical column names of
`fault_prod`):

```json
[
  {"tenant_id": "tenant_a", "device_id": "device_1",
   "device_fault_count_7d": 2, "device_fault_count_7d_state": "ok",
   "feature_as_of": "2026-06-08T00:00:00Z", "published_at": "2026-06-08T00:01:30Z",
   "publication_sequence": 1, "publication_id": "pub-1", "materialization_id": "mat-1"},
  {"tenant_id": "tenant_a", "device_id": "device_1",
   "device_fault_count_7d": 3, "device_fault_count_7d_state": "ok",
   "feature_as_of": "2026-06-08T00:00:00Z", "published_at": "2026-06-08T00:10:00Z",
   "publication_sequence": 2, "publication_id": "pub-2", "materialization_id": "mat-2"}
]
```

Request:

```json
{
  "api_version": "featlas.consumer/v1",
  "deployment": "fault_prod",
  "consumer": "reliability_replay",
  "consumer_version": "1.0.0",
  "rows": [
    {"tenant_id": "tenant_a", "device_id": "device_1",
     "as_of": "2026-06-08T00:00:00Z", "requested_at": "2026-06-08T00:05:00Z"}
  ]
}
```

```sh
fe-spec consume projects/01_fault_count.yaml --request request.json --records records.json
```

Output (trimmed to the selected value):

```json
"values": [
  {
    "feature": "device.fault_count_7d",
    "version": "1.0.0",
    "value": 2,
    "state": "ok",
    "provenance": {
      "publication_id": "pub-1",
      "materialization_id": "mat-1",
      "feature_as_of": "2026-06-08T00:00:00Z",
      "published_at": "2026-06-08T00:01:30Z",
      "publication_sequence": 1
    }
  }
],
"diagnostics": []
```

The response also names the consumer, feature set, `selection_mode`, `surface`
(`offline_snapshot`), deployment and adapter identity; callers never see table names.

## Who decides what

The consumer API is target-neutral. A storage adapter (or an external host, through
`--records`) returns physical rows; the core maps them back to logical fields and applies
selection, freshness, null and atomic-set rules itself. Adapters never decide which value is
selected (`consumer.rs` module docs). On Databricks the generated `consumer_read` statement only
narrows rows by key and `feature_as_of <= max_as_of`.

## Backfills and corrections

`backfill.mode` can only be `source_as_known` and `publish` is `disabled` or
`separate_namespace` (schema), so historical recomputation cannot overwrite or fabricate observed
serving history (SEM-REPLAY-003). How Databricks records `published_at` is described in
[Databricks guide → publication](../guides/databricks.md#publication-protocol).
