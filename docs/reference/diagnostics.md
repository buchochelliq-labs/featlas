# Diagnostics

**In plain terms:** when featlas rejects something it says exactly what, where and why: a stable
code you can search for, the stage that found it, the specification rule it enforces, the
location in the file, and a message saying how to fix it.

## Shape (`crates/featlas-core/src/diag.rs`)

| Field | Meaning |
| --- | --- |
| `code` | Stable identifier, e.g. `PARSE_AMBIGUOUS_SCALAR` |
| `severity` | `error`, `warning` or `info` |
| `stage` | `parse`, `schema`, `references`, `planning`, `data`, `runtime` |
| `rule` | The SEM rule enforced, e.g. `SEM-PARSE-001` |
| `path` | JSON pointer into the document (array indices), e.g. `/metadata/status` |
| `location` | `line`, `column` when known |
| `related` | Related object ids/versions |
| `message` | Actionable explanation |

Text output renders a diagnostic as
`<severity> <code> [<stage>:<rule>] <path> (line L, column C): <message>`, for example:

```text
error   PARSE_AMBIGUOUS_SCALAR [parse:SEM-PARSE-001] /metadata/status (line 13, column 11): Plain scalar "yes" is ambiguous across YAML versions; quote it or use JSON-compatible true/false/null/number syntax
```

Evaluation errors (fixtures, consumer reads) carry `code`, `rule` and `message`. API errors
(`featlas.FeatlasError`, MCP `error`, CLI `{"error": …}`) carry `code`, `message` and `detail`.

## Codes you will meet often

Each code below appears in this site's examples or in the cited code/tests.

| Code | Stage / rule | Meaning |
| --- | --- | --- |
| `PARSE_AMBIGUOUS_SCALAR`, `PARSE_IMPLICIT_TIMESTAMP`, `PARSE_INTERPOLATION`, `PARSE_DUPLICATE_KEY`, `PARSE_NUMBER_RANGE`, `PARSE_NON_FINITE`, `PARSE_INTEGER_RANGE`, `PARSE_DIRECTIVE` | parse, SEM-PARSE-001 | Restricted YAML/JSON profile violations ([Writing feature specs](../guides/authoring.md#restricted-yaml-common-errors)) |
| `INVALID_TIMESTAMP` | SEM-TIME-003 | Not a valid UTC instant |
| `EMPTY_IDENTITY_REVIEW` (warning) | references, SEM-TRANS-004 | A known-empty value for sum/mean/min/max needs confirmation |
| `OUTPUT_RETENTION_SHORT` (warning) | SEM-DEPLOY-002 | Output retention shorter than the longest window |
| `UNRESOLVED_ADAPTER` | planning, SEM-PORT-002 | No installed adapter for the pinned implementation/version |
| `CAPABILITY_REQUIREMENT_DERIVED` (warning) | planning, SEM-PORT-006 | A requirement derived from operations was not declared |
| `UNSUPPORTED_CAPABILITY` | planning, SEM-PORT-006 | An adapter cannot satisfy a requirement |
| `REPORT_*` | planning, SEM-PORT-006 | Capability report inconsistencies (`check-report`) |
| `DUPLICATE_CONFLICT` | SEM-REV-002 | Rows sharing a key disagree |
| `MULTIPLE_MATCHES`, `INVALID_INTERVAL` | SEM-REL-002, SEM-REV-001 | Join/dimension contract violations |
| `MISSING_SOURCE_STATUS` | SEM-MISS-003 | No coverage evidence for a source |
| `SOURCE_UNAVAILABLE` | SEM-ENT-001 | Population source unavailable (Databricks host) |
| `OUTPUT_TYPE_MISMATCH`, `QUALITY_VIOLATION` | SEM-OUT-001, SEM-QUAL-001 | Output or error-level quality rule failed |
| `QUALITY_QUARANTINED` | SEM-QUAL-001 | Quarantine findings; nothing published |
| `PUBLICATION_TIE` | SEM-REPLAY-002 | Two eligible publications share the ordering key |
| `STALE_FEATURE` | SEM-FRESH-002 | Selected publication too old |
| `NUMERIC_OVERFLOW`, `DIVISION_BY_ZERO`, `NON_FINITE_RESULT` | SEM-TRANS-006 | Runtime arithmetic errors |
| `PUBLICATION_NOT_AUTHORIZED`, `TARGET_TABLE_MISMATCH`, `PUBLICATION_IDENTITY_CONFLICT`, `PUBLICATION_EVIDENCE_MISSING` | host | Databricks publication refusals |
| `DOCUMENT_INVALID`, `PLANNING_FAILED`, `REPORT_INCONSISTENT` | API | Operation-level errors |

This is not a complete catalogue. To list every code with the rule it is reported under, search
the sources, e.g. `grep -rhoE '"[A-Z][A-Z0-9_]{3,}"' crates/featlas-core/src | sort -u`, and read
the surrounding code for the rule.
