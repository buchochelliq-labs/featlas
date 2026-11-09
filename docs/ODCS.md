# Prospective ODCS integration (not implemented)

featlas does **not** import, execute or validate Open Data Contract Standard (ODCS) documents,
and claims no conformance to ODCS. This note records how existing source contracts could map
to a future importer/validator, reusing source contract references and lineage rather than
duplicating conflicting contracts.

| featlas source contract | Prospective ODCS counterpart | Gap / decision needed |
| --- | --- | --- |
| `sources[].id`, `version`, `owner`, `description` | contract id/version, owner/team, description | Version semantics must stay exact pins; ODCS version ranges cannot satisfy featlas references. |
| `columns[]` name/type/nullable/unit/classification/description | schema properties (logical type, required, classification, description) | Type mapping (featlas `integer` = i64, `number` = binary64, `timestamp` = UTC µs) must be explicit; units have no single ODCS field. |
| `primary_key`, `record_semantics.logical_key` | primary key / unique properties | ODCS has no logical-vs-stored key distinction; revisions need an extension or a featlas-side overlay. |
| `time.event_time`, `time.available_time` | (none standard) | Availability time is essential and must remain featlas-declared. |
| `record_semantics` revision order/tombstones | (none standard) | featlas-only. |
| `quality` (null/invalid row policy, lateness, completeness mode) | data quality rules | Completeness must still require positive external status evidence. |
| `retention` | SLA/retention properties | `earliest_reproducible_at` has no direct counterpart. |
| `lineage.upstream_columns`, `lineage.code` | lineage / transformation references | Reusable as-is; code provenance digests must be preserved. |
| deployment `source_bindings[].location` | servers / physical location | Physical locations stay deployment-scoped in featlas. |

Proposed importer behaviour: parse ODCS as untrusted data, map only the fields above, refuse to
fill featlas-only fields (availability, revision semantics) from defaults, and emit a diff
against the existing featlas source contract instead of overwriting it.
