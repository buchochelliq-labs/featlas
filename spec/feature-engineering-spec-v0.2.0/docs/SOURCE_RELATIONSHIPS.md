# Source tables, columns, entities, and relationships

This guide explains the data model required by [the normative specification](../SPECIFICATION.md). It does not claim that declared relationships have been validated against a database.

## 1. Separate meaning from physical location

A logical source is a versioned contract for records. A deployment binds it to a local file or an environment-specific Databricks table. The logical definition should remain useful when a catalog name changes.

| Contract layer | Questions it answers | Example information |
| --- | --- | --- |
| Source | What does one stored row mean, and who maintains it? | An event revision received from an operational service; owner; upstream source and code provenance. |
| Column | What does a value mean? | Event identifier; UTC occurrence time; first availability of this revision; amount in integer minor units. |
| Stored primary key | What uniquely identifies a physical record? | Tenant, event ID, revision number. |
| Logical record key | Which revisions describe the same logical event or dimension record? | Tenant and event ID; or tenant and dimension-interval ID. |
| Entity | What is the output grain and eligible population? | Tenant plus customer ID, selected from population history as of the request. |
| Relationship | Which columns may be joined, with what temporal and cardinality rules? | Event device key to device dimension key; many-to-one; fail on multiple eligible matches. |
| Deployment binding | Where are those records read in this environment? | Local fixture path or catalog/schema/table; no embedded credentials. |
| Execution evidence | Which exact records and implementation produced this output? | Snapshot/version IDs, source hashes, spec hash, code digest, run ID. |

Primary keys, foreign-key-like relationships, and cardinalities express different claims. A primary key describes uniqueness of one source's stored rows. A relationship maps source columns to target columns and states the expected match behavior. A declared many-to-one relationship does not prove uniqueness of the target, and a declared foreign key does not prove every event has a matching target. Those require data checks at the relevant as-of time.

## 2. Describe columns so they can be reviewed

A useful source description states the business event, record grain, update/retraction behavior, upstream owner, and coverage limitations. A useful column description states its meaning and the treatment of null or special values.

For an illustrative revisioned event source:

| Column | Meaning | Required role in the contract |
| --- | --- | --- |
| `tenant_id` | Tenant namespace containing the local identifiers. | Entity and join key; prevents collisions across tenants. |
| `event_id` | Stable ID of the logical event, shared by its revisions. | Logical identity. |
| `revision` | Monotonically ordered correction version for the event. | Revision ordering; not event time. |
| `device_id` | Device associated with this selected revision. | Entity or relationship key. |
| `occurred_at` | UTC instant when the business event happened. | Event eligibility and temporal join reference. |
| `available_at` | Earliest instant this revision was queryable under the source contract. | Availability cutoff. |
| `is_deleted` | Whether this revision retracts the logical event. | Deletion/tombstone selection. |
| `fault_code` | Operational fault category; allowed codes explained in its description. | Value/filter lineage. |
| `amount_minor` | Amount expressed in a declared currency's integer minor units. | Value and unit; do not silently mix currencies. |

These names are illustrative. The complete YAML examples are the executable document instances. A source cannot replace a precise availability definition with “ingestion timestamp” if ingestion and queryability differ materially. A pipeline may need an upstream normalization layer that computes the promised availability field.

### Record identity and duplicates

For a revisioned event, stored primary key `(tenant_id, event_id, revision)` and logical key `(tenant_id, event_id)` answer different questions. The first identifies a revision; the second identifies what must be reconstructed.

`duplicate_policy: reject` rejects duplicate identities. `collapse_identical` permits only identical duplicate records. It must not collapse differing amounts, times, delete flags, or categories. A tied revision ordering with contradictory values is an error, even if choosing one would make a golden output pass.

When a selected revision is invalid, the implementation must diagnose that revision. It must not revert to an older valid revision unless a future, explicit semantic contract defines that behavior. Otherwise “repair” silently changes the historical state.

## 3. Entities and population history

An event table is usually an incomplete population: entities with no events are absent. The entity contract therefore points to a population source and maps its named keys to source columns.

The baseline population mode is `as_of_keys`. It uses the declared event/availability upper bounds and deletion policy to determine eligible keys as of T. A revisioned population source must be reconstructed with the same availability-first rules as any other versioned source.

For a production population, completeness covers the full active key universe at T. Complete registration events over only the feature's recent lookback do not establish that older active entities are present; retained history or an equivalent reconstructible state must account for them.

All applicable keys must travel together:

- Population key and output key.
- Feature group-by mapping.
- Relationship key pairs.
- Derived-feature alignment.
- Consumer lookup inputs.
- Publication and replay records.

If customer `42` exists under both tenants `a` and `b`, joining only on customer ID is invalid. A fixture must demonstrate that another tenant's records cannot affect the result.

**Fixture scope:** `population_rows` declares canonical expected population keys and MUST equal the keys reconstructed from the supplied population source history. The reference evaluator compares them. This verifies the supplied synthetic history; it does not establish the completeness or correctness of a production population source.

## 4. Explicit relationship use

The relationship's `from` and `to` specify sources and ordered column lists. Corresponding positions form the join pairs. Its `version` is pinned when a feature transform uses it.

Feature inputs bind aliases to sources and declare columns read. A transform join then names `relationship`, `version`, `left_alias`, and `right_alias`. The aliases must resolve to the relationship's endpoints. Merely declaring a relationship in the project performs no join.

The portable baseline permits one-to-one and many-to-one relationships. It requires `multiple_matches: error`. General one-to-many fan-out is deliberately outside the baseline; adding it requires explicit aggregation grain and double-counting semantics.

| Condition | Required behavior |
| --- | --- |
| One eligible right-side match | Join the declared columns. |
| No match, left/preserve-null policy | Keep the left row with null right-side values. |
| No match, inner/drop policy | Remove the left row from the transform input. |
| No match, error policy | Emit the declared relationship diagnostic. |
| Null join key | Follow `null_keys: unmatched` or `null_keys: error`; null keys do not equal each other. |
| More than one eligible match | Fail; do not pick an arbitrary row or multiply the aggregate. |

Validate cardinality on reconstructed, temporally eligible rows. Checking today's complete dimension and assuming the result holds at every historical instant is insufficient.

## 5. Availability-aware valid-time dimensions

A dimension may describe when a fact is true in the business world and when the system learned it. These are separate axes.

- **Valid time:** the interval in which a device classification, account status, or other attribute applies.
- **Availability time:** the instant at which a particular stored version became knowable.
- **Revision identity:** the logical dimension record being corrected, separate from the entity key.

The baseline historical relationship uses `temporal.mode: as_of`, `reference: left_event_time`, a named validity start/end, inclusive lower and exclusive upper bounds, and `available_time_cutoff: feature_as_of`.

### Example: a correction arrives after the feature cutoff

A device interval is initially believed to be “standard.” A later correction says it was “critical” over the same validity interval.

| Logical interval | Revision | Valid interval | Available at | Classification |
| --- | --- | --- | --- | --- |
| Device A, interval 1 | 1 | January 1 through February 1 | January 1 | standard |
| Device A, interval 1 | 2 | January 1 through February 1 | January 20 | critical |
| Device A, interval 2 | 1 | February 1 onward | January 25 | standard |

For a feature requested on January 15 about an event on January 10, revision 2 is unavailable. The correct join uses interval 1, revision 1. For a request on January 21 about the same event, revision 2 is available and may be selected. Interval 2 must remain a separate logical record; selecting a single latest revision per device would destroy valid history.

The safe order is:

1. Apply the feature cutoff to each dimension revision's availability.
2. Reconstruct the latest eligible revision for each logical interval record.
3. Reject ambiguous ties and process tombstones.
4. Require `valid_from ≤ left_event_time < valid_to` for candidate intervals.
5. Check the declared match cardinality and unmatched policy.
6. Apply the feature's remaining predicates and aggregate.

Null `valid_to` can mean an unbounded future only when the contract allows it. Overlapping surviving intervals are not repaired by picking the most recent start; they fail if they create multiple matches.

### Event corrections also precede filters

Suppose event revision 1 is a fault inside the window, but eligible revision 2 corrects it to a non-fault or moves its event time outside the window. Selecting only fault rows or window rows first would retain revision 1 incorrectly. Reconstruct first, then filter. A tombstone follows the same principle.

## 6. Lineage must cover why a row contributes

Output-value lineage alone is insufficient. Capture all columns that affect eligibility, identity, matching, order, or coverage.

The baseline roles include `value`, `filter`, `entity_key`, `join_key`, `event_time`, `availability_time`, `coverage`, `ordering`, `revision`, `deletion`, `valid_time`, and `population`. One column can have several roles.

A historical joined fault count therefore needs lineage for the count's source, fault predicate, entity and tenant keys, event/availability timestamps, revision order, delete marker, relationship keys, dimension validity interval, dimension availability/revision fields, and source coverage. A count with no numeric value column still has substantial lineage.

A derived feature also records exact feature dependencies. A derived source records upstream source identifiers and transformation/code provenance. The execution manifest binds those declarations to immutable input and code evidence.

## 7. Reproducibility limits to state explicitly

Availability timestamps cannot reconstruct revisions that have been overwritten or deleted. `retention.revision_history_seconds` and `retention.earliest_reproducible_at` constrain claims of historical reproducibility. A target must verify that retained data actually supports the requested interval.

External completeness status must specify the source, interval, applicable entity/partition scope, observation time, and status provenance. “No rows returned” does not establish complete history. A complete status for one partition does not cover another tenant or a larger time interval.

Where source history or completeness evidence is missing, report the limitation. Do not substitute present-day records and label the result historically correct.


## Deployment mappings in 0.2.0

Source relationships continue to use logical source IDs/versions and logical columns. Physical names are mapped by `source_bindings` with an exact source version, a catalog-capable adapter, a generic location, and a complete one-to-one `column_mapping`. Renaming a physical source column does not rewrite joins, feature filters, lineage references, or entity keys. Mapping is a rename operation; casts, unit conversions, timezone changes, and defaults require reviewed upstream normalization.

Feature-store output bindings separately map entity keys, every feature value/state, and publication evidence. The mappings must preserve source availability, revision selection, logical grain, and tenant isolation. See [PORTABILITY.md](PORTABILITY.md) for the full mapping and surface rules.
