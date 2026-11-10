# Time semantics

**In plain terms:** every feature value is computed "as of" a moment *T*. Three different
clocks matter: when something *happened* (event time), when the data system *knew about it*
(availability time) and when the computed value was *published* for a model to read. A value at
*T* may use only rows that happened inside the window **and** were already known at *T*. Late
data and later corrections must not leak into the past.

## The three clocks (SPECIFICATION §5.1)

| Clock | Column / field | Decides |
| --- | --- | --- |
| Event time | `sources[].time.event_time` | Whether a row is inside the feature window |
| Availability time | `sources[].time.available_time` | Whether a row or revision was knowable at *T* (SEM-TIME-002) |
| Publication time | `published_at` on output rows | Whether a computed value was retrievable by a consumer (SEM-REPLAY-001/002) |

All portable timestamps are UTC. Durations are integer elapsed seconds; a "day" is 86 400
seconds, never a calendar day in a local zone.

## Instants

`crates/featlas-core/src/time.rs` accepts only `YYYY-MM-DDTHH:MM:SS[.f{1,6}]Z`. Invalid calendar
dates, missing zones, offsets other than `Z`, leap seconds and more than six fractional digits
are rejected (`INVALID_TIMESTAMP`, SEM-TIME-003); instants compare as integers in microseconds,
never as text. The schema pattern alone would accept a date such as `2026-02-30T…Z`; featlas
rejects it (SPEC_FINDINGS #4). In YAML, timestamps must be quoted strings; plain timestamps are a
parse error (SEM-PARSE-001, DECISIONS D3).

## Event windows (SEM-TIME-001)

For lookback *W*, the window runs from *T − W* to *T*, and `lower`/`upper` each declare
inclusivity. Example 01 declares `lookback_seconds: 604800`, `lower: inclusive`,
`upper: exclusive`, so the window is `[T − 7 d, T)`. `publication.delay_seconds` never shifts
it; `fe-spec explain` says so explicitly:

```text
  event window: [as_of − 604800s, as_of) (publication delay 60s never shifts it)
  availability: available_time ≤ as_of
```

The `evaluate` trace for the boundary case shows the filter in action (4 supplied rows, 2 inside
the window):

```text
│ event_window  │ device.fault_count_7d │ fault_events: 4 → 2 rows in [2026-06-01T00:00:00Z,                 │
│               │                       │ 2026-06-08T00:00:00Z)                                              │
```

## Availability cutoff (SEM-TIME-002)

Each feature declares `availability.cutoff: as_of` with `upper: inclusive` (`available_time ≤ T`)
or exclusive (`available_time < T`). A row that happened before *T* but arrived after *T* is
unavailable for that historical computation. The availability filter is applied **before**
deduplication and revision selection, so a late correction cannot replace an earlier value
retroactively.

## Reconstruction order (SEM-REV-001)

The evaluator enforces one order for every feature and every source
(`crates/featlas-core/src/eval.rs` module docs):

1. validate supplied rows and source status (invalid rows are rejected);
2. apply the source availability cutoff;
3. resolve physical primary-key duplicates under the declared policy;
4. partition by the complete logical key and select the latest eligible revision; contradictory
   ties fail; there is no fallback to older revisions;
5. apply tombstones of the selected revision;
6. reconstruct the eligible population;
7. event window → availability-aware valid-time joins → predicate → complete entity grouping
   (tenant keys included);
8. exact transform and pinned same-entity dependencies;
9. coverage, outage, empty, null, quality and output contracts;
10. values, states and a trace.

Filtering earlier could resurrect a superseded record or hide a retraction, which is why event
windows and predicates run only on reconstructed rows. The Databricks lowering keeps the same
order (see [Databricks guide](../guides/databricks.md#how-lowering-maps-to-spark-sql)).

## Revisions and tombstones

A `versioned` source declares a `revision.order_by` (for example `revision_no desc`),
`selection: latest_available` and an optional tombstone (`tombstone_column`, `tombstone_value`).
The latest *available* revision per logical key is selected; if it is a tombstone, the record is
removed. featlas-specific strictness (DECISIONS D4):

* Any two eligible revisions sharing the full revision-order key with different payloads fail,
  not only when they would win.
* `valid_to <= valid_from` on any reconstructed dimension row fails (`INVALID_INTERVAL`).

## Valid-time joins (SEM-REL-004)

An `as_of` relationship reconstructs the dimension as known at the feature cutoff, then keeps the
right row whose `[valid_from, valid_to)` contains the left row's event time. Overlapping eligible
intervals that produce more than one match are an error (`MULTIPLE_MATCHES`). Example 02
(`02_historical_temperature.yaml`) exercises this, including late dimension corrections.

## Labels

Labels look forward instead of back: the label source is reconstructed at
`availability ≤ dataset_cutoff`, then the horizon `[T, T + H]` with declared bounds is applied to
event time. A row is labelled only when the horizon has matured by the cutoff and status evidence
covers it; otherwise `incomplete_horizon` decides exclusion or error. Feature filters never apply
to labels (`crates/featlas-core/src/label.rs`, SEM-LABEL-001/002, DECISIONS D4). Details, including Spark lowering: [Labels](labels.md).
