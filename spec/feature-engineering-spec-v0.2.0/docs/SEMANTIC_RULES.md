# Semantic rule register

This register makes the normative requirements traceable to future diagnostics and tests. The authoritative explanations are in [SPECIFICATION.md](../SPECIFICATION.md). These rows are **required checks**, not a claim that the reference kit implements every check.

A checker should report a stable rule ID, severity, validation stage, instance path, related object IDs/versions, and a concise explanation with remediation. A diagnostic such as “invalid input” without location or reason is insufficient.

## Validation stages

Run the stages in order, preserving the distinction in reports:

1. **Parse:** restricted YAML/JSON and duplicate-key rejection.
2. **Schema:** Draft 2020-12 structural validation.
3. **References and types:** identity, references, versions, graph, columns, expression types, and internal semantic consistency.
4. **Planning and capabilities:** supported operations and target guarantees, without executing custom code or publishing anything.
5. **Data and fixtures:** actual rows, reconstruction, joins, missingness, and golden comparisons.
6. **Runtime:** target permissions, retained snapshots, operational limits, publication evidence, and real execution.

Schema `default` annotations, where present, are documentation. They do not insert values. A parser/compiler must not invent omitted contract fields. Any future normalization pass that supplies defaults must be explicit, versioned, and reflected in the canonical manifest.

## Rule matrix

| Rule | Requirement | Earliest meaningful check |
| --- | --- | --- |
| SEM-CONF-001 | Report level and passed/failed/unsupported/not-evaluated status honestly. | Every report |
| SEM-CONF-002 | Block dependent stages on prerequisite failure; metadata does not authorize execution. | Every stage |
| SEM-PARSE-001 | Restrict YAML to one JSON-compatible document; reject duplicate keys and forbidden YAML features. | Parse |
| SEM-ID-001 | Unique IDs within each collection; one version of an ID per project. | References |
| SEM-ID-002 | Resolve references to the correct kind and exact pinned version. | References |
| SEM-ID-003 | Acyclic feature DAG; declared dependency and AST references agree. | References/types |
| SEM-ID-004 | Separate language, semantic-object, implementation, and artifact versions. | References/review |
| SEM-SRC-001 | Columns resolve; keys/time/revision/delete/validity types are compatible. | References/types |
| SEM-SRC-002 | Distinguish stored row key from logical revision key; preserve tenant keys. | References/data |
| SEM-SRC-003 | Check actual uniqueness, coverage, and cardinality rather than trusting declarations. | Data |
| SEM-SRC-004 | Trace source provenance and bind executions to immutable input identities. | References/runtime |
| SEM-ENT-001 | Reconstruct population and preserve the intended output grain. | Data |
| SEM-ENT-002 | Preserve tenant identity throughout all data paths. | References/data |
| SEM-REL-001 | Correct endpoints, key arity/types, alias binding, and reachable join inputs. | References/types |
| SEM-REL-002 | Enforce declared cardinality and fail on multiple matches. | Data |
| SEM-REL-003 | Enforce join/unmatched/null-key behavior. | References/data |
| SEM-REL-004 | Apply both dimension availability and valid-time eligibility. | Data |
| SEM-TIME-001 | Apply declared event boundaries at T; publication delay does not shift them. | Data/fixtures |
| SEM-TIME-002 | Apply source availability cutoff independently of event time. | Data/fixtures |
| SEM-TIME-003 | Compare valid UTC instants and supported precision. | Parse/data |
| SEM-REV-001 | Select eligible revisions before windows, predicates, joins, or validity filters. | Data/fixtures |
| SEM-REV-002 | Reject or collapse only the duplicates allowed by the source contract. | Data |
| SEM-REV-003 | Preserve separate dimension intervals; detect overlapping matches. | Data |
| SEM-REPLAY-001 | Distinguish source reconstruction from historical serving replay. | Planning/runtime |
| SEM-REPLAY-002 | Replay from observed publications with deterministic selection and freshness. | Data/runtime |
| SEM-REPLAY-003 | Preserve publication history through corrections and backfills. | Runtime |
| SEM-TRANS-001 | Declare all read columns and exact feature dependencies. | References/types |
| SEM-TRANS-002 | Fixed operator arity/types and explicit unsupported-operation errors. | Types/planning |
| SEM-TRANS-003 | Explicit null and three-valued predicate behavior. | Fixtures |
| SEM-TRANS-004 | Exact aggregates and explicit null/empty/coverage behavior. | Fixtures/data |
| SEM-TRANS-005 | Recency and lookup obey eligibility, ordering, and ambiguity rules. | Fixtures/data |
| SEM-TRANS-006 | Explicit numeric widening, overflow, zero-division, and tolerance. | Types/fixtures |
| SEM-OUT-001 | Enforce output type/unit/nullability/range. | Types/data |
| SEM-LIN-001 | Complete source-column lineage, including all selection and temporal roles. | References/review |
| SEM-LIN-002 | Trace exact feature dependencies and source/code provenance. | References/review |
| SEM-LIN-003 | Distinguish declared lineage from observed execution evidence. | Runtime |
| SEM-MISS-001 | Preserve different missingness and failure reasons. | Fixtures/runtime |
| SEM-MISS-002 | Emit healthy zero only with adequate population and coverage evidence. | Data |
| SEM-MISS-003 | Check history at the correct entity/time scope. | Data/runtime |
| SEM-QUAL-001 | Quality checks have scope, thresholds, and explicit failure behavior. | References/data |
| SEM-FIT-001 | Immutable fitted artifact with recipe/data/split/cutoff/implementation identity. | References/runtime |
| SEM-FIT-002 | Fit preprocessing only on each split's training rows. | Data/review |
| SEM-FIT-003 | Reject incompatible artifact reuse and false historical availability claims. | Planning/runtime |
| SEM-CUSTOM-001 | Explicit immutable custom contract; no arbitrary execution during validation. | Planning |
| SEM-CUSTOM-002 | Required unsupported capabilities fail explicitly. | Planning |
| SEM-CONS-001 | Consumer keys/versions align; substitutions remain consumer-side and visible. | References/runtime |
| SEM-LABEL-001 | Separate label horizon/maturity/censoring from feature inputs. | References/data |
| SEM-LABEL-002 | Test immature and late labels; preserve split isolation. | Fixtures/data |
| SEM-FRESH-001 | Distinguish source lag, effective age, expected delay, and observed publication lag. | Runtime |
| SEM-FRESH-002 | Apply stale/unavailable policy without hidden historical-to-current fallback. | Runtime |
| SEM-DEPLOY-001 | Complete bindings, explicit capabilities, pinned execution implementation. | Planning/runtime |
| SEM-DEPLOY-002 | Enforce permissions and retention at the target; metadata grants no access. | Runtime |
| SEM-PORT-001 | Keep canonical semantics vendor-independent; Databricks is first external acceptance target. | Architecture/target acceptance |
| SEM-PORT-002 | Resolve exact adapter instances/roles/configuration; scheduled deployments need orchestration. | References/planning |
| SEM-PORT-003 | Complete injective source mappings, exact source pins, no implicit normalization. | References/data |
| SEM-PORT-004 | Complete output membership/keys/states/metadata; distinct physical fields/resources. | References/adapter planning |
| SEM-PORT-005 | Logical consumer bindings resolve matching targets and guaranteed publication surface. | References/retrieval |
| SEM-PORT-006 | Derive all required capabilities; evidence scope and exact identities constrain readiness. | Planning/runtime |
| SEM-PORT-007 | Explicit native mode, immutable code, scoped capability, pinned feature suite; metadata-only extensions. | References/planning |
| SEM-PORT-008 | Store rebinding preserves logical identities/meaning; deployment/run identities remain distinct. | Migration/review |
| SEM-PORT-009 | Preserve publication origin/order separately from current materialization and destination visibility. | Runtime/migration |
| SEM-PORT-010 | Actual migration requires target/data/consumer/cutover evidence; local checks are bounded. | Migration acceptance |
| SEM-BUDGET-001 | Bound work and stop on excess; no silent sampling or approximation. | Planning/runtime |
| SEM-DEPLOY-003 | Explicit retry/idempotence/backfill behavior and authorized environment. | Runtime |
| SEM-TEST-001 | Cover decisive edge cases for each supported capability. | Acceptance |
| SEM-TEST-002 | Independently reasoned golden expectations and useful invariance checks. | Review |
| SEM-TEST-003 | Verify and ship the exact reviewed candidate with reproducible evidence. | Release |

## Expression behavior required of a full evaluator

The schema's logical type names are `integer`, `number`, `boolean`, `string`, `timestamp`, and `date`, with `null` additionally available as a literal. Runtime `integer` is signed 64-bit and `number` is finite binary64. No string-to-number, string-to-boolean, or locale-dependent timestamp coercion is implicit.

### Operator arity and type contract

| Operators | Arity | Accepted operands and result |
| --- | --- | --- |
| `add`, `subtract`, `multiply` | Exactly 2 | Numeric operands; integer result when both are integer, otherwise number. |
| `divide`, `safe_divide` | Exactly 2 | Numeric operands; nullable number according to null/zero policy. |
| `absolute` | Exactly 1 | Numeric operand; same numeric type, with checked integer overflow. |
| `equal`, `not_equal` | Exactly 2 | Compatible scalar types; integer/number mixing allowed; nullable boolean result. |
| `less_than`, `less_or_equal`, `greater_than`, `greater_or_equal` | Exactly 2 | Compatible ordered scalar types; nullable boolean result. |
| `and`, `or` | One or more | Boolean operands; nullable boolean result under three-valued logic. |
| `not` | Exactly 1 | Boolean operand; nullable boolean result. |
| `is_null` | Exactly 1 | Any scalar operand; non-null boolean result. |
| `coalesce` | One or more | Compatible result types, allowing null literals and numeric widening; first non-null result. |
| `if_else` | Exactly 3 | Boolean condition, then/else expressions with compatible result types; null condition selects else. |

An explicitly null literal is compatible with a nullable operand/result context. Scalar ordering is numeric for integer/number, chronological for timestamp/date, false before true for booleans, and lexicographic Unicode code-point order for strings. Strings do not receive implicit case folding, collation, or Unicode normalization. Integer/number comparison compares their numeric values; arithmetic widening can lose integer precision beyond binary64's exact range and must not be used to justify nonzero tolerances for integer outputs.

### Evaluation behavior

| Operation class | Required behavior |
| --- | --- |
| `integer` arithmetic | Checked signed 64-bit operations; overflow is a diagnostic. |
| Mixed `integer` / `number` arithmetic | Numeric widening to binary64 `number` is allowed; no other implicit coercion. Precision limits must be documented. |
| Ordinary division | Returns `number`; division by zero is an evaluation diagnostic. |
| `safe_divide` | Returns null when the denominator is zero; otherwise follows numeric division and null rules. |
| Numeric/string comparisons | Compare compatible types; a null operand yields null. |
| `and` / `or` | Three-valued logic and short-circuit evaluation. |
| `not` | Negates true/false; null remains null. |
| `if_else` | Evaluates only the selected branch; a null condition selects the else branch. Both branches must have compatible result types. |
| `coalesce` | Evaluates arguments in order and returns the first non-null value; later arguments are not evaluated. |
| Filter result | Only true retains the row; false or null drops it. |

For three-valued logic: false AND null is false; true AND null is null; true OR null is true; false OR null is null. This is distinct from treating null as false everywhere. Lazy evaluation prevents an unselected expression branch from raising an irrelevant division-by-zero error.

`number` computations must not produce NaN or infinity as JSON values. Compare floating results using the feature's declared tolerance; integer counts and minor-unit amounts must compare exactly. A tolerance must not hide a wrong row set or a unit conversion error.

For a finite actual value a and expected value e, the declared tolerance accepts the comparison when `abs(a - e) ≤ max(absolute, relative × abs(e))`. Non-numeric values and nulls compare by type-aware exact equality. Expected null is not equal to zero, false, or an omitted field.

## Reports and acceptance evidence

A complete implementation should produce a machine-readable report containing:

- Candidate specification and implementation identity.
- Validation stages attempted and their statuses.
- Rule IDs checked, failed, unsupported, and not evaluated.
- Per-case expected and actual outputs or diagnostics.
- Runtime and dependency versions.
- Input snapshot or fixture digests.
- Explicit capability limits and operational checks not performed.

The current kit's report is authoritative about its own observed execution. This register is the target contract. Do not infer that every row was checked merely because a project passes JSON Schema or the bundled examples pass.


## Portability checks supplied by the local kit

`tools/binding_validation.py` checks declared adapter references/roles, version pins, source/output/consumer mapping consistency, scoped explicit requirements, native references, and the single publication surface. `tools/portability_mapping.py` checks in-memory field mapping without storage I/O. `tools/capability_reports.py` checks report structure and consistency against exact project bytes and explicit requirements. None loads a production adapter or proves completeness of planner-derived requirements, physical backend equivalence, evidence authenticity, or live execution readiness. Those remain production planning/runtime gates.
