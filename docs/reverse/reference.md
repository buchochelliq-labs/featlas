# Reverse engineering reference

**In plain terms:** the same operations are available three ways — a command line
(`featlas-reverse`), a read-only MCP server for AI agents, and Python functions — plus three
written "skills" that tell an agent how to use them step by step.

## Installation

Part of the `featlas` wheel (`python-packages` includes `featlas_reverse`). Extra `reverse`
installs `sqlglot==30.30.0` and `PyYAML==6.0.3`; the console script is `featlas-reverse`
(`python/pyproject.toml`).

## CLI: `featlas-reverse`

| Subcommand | Arguments | Output |
| --- | --- | --- |
| `analyze SQL` | — | Analysis JSON (tables, fact alias, joins, group by, window, filters, outputs, unsupported) |
| `draft` | `--table`, `--sql`, `--source` (repeatable), `--decisions`, `--out`, `--report` | Writes spec YAML and report JSON; prints `{spec, report, valid, assumptions, questions, unsupported}`; exit 2 when blocked by a question, 1 when the spec does not validate |
| `split-analyze SPEC` | `--feature-set`, `--table`, `--max-groups` (6), `--min-size` (2) | Strategies, metrics, `recommended`, `decision_required` |
| `split-apply SPEC` | `--feature-set`, `--assignment`, `--strategy`, `--colocate-dependencies`, `--out`, `--mapping` | Writes split YAML and `{mapping, compatibility_view_sql}`; prints `{spec, groups, valid}` |
| `parity SPEC` | `--legacy-table`, `--legacy-as-of-column`, `--key` (repeatable), `--as-of` | Parity SQL on stdout |
| `mcp` | — | Serves the MCP tools on stdio |

## MCP server: `featlas-reverse`

`python -m featlas_reverse.mcp_server` (or `featlas-reverse mcp`): JSON-RPC 2.0, one message per
line, protocol `2025-06-18`, server name `featlas-reverse`. Every tool is side-effect free —
documents, SQL and table descriptions are passed inline and results returned inline; tools are
annotated `readOnlyHint: true`, `openWorldHint: false`. Unknown or missing arguments return
`isError` with `INVALID_ARGUMENTS` (names only; argument types are not schema-validated);
`FeatlasError`, `ValueError` and `KeyError` from a tool return `isError` with the error code.

| Tool | Required arguments | Returns |
| --- | --- | --- |
| `reverse_analyze_sql` | `sql` (`dialect`, default `databricks`) | Analysis |
| `reverse_draft_spec` | `target_table`, `sql`, `sources` (`decisions`, `dialect`) | `project_yaml` (null when blocked), `report`, `validation` (passed, diagnostics, fixture summary, failed cases) |
| `spec_validate` | `document` (`format`) | Validation plus fixture run |
| `spec_generate` | `document`, `deployment` (`format`, `include_files`) | `execution_ready`, capability report, generated files (or their names), manifest; nothing executed or published |
| `split_analyze` | `document`, `feature_set` (`legacy_table`, `max_groups`, `min_size`) | Strategies and metrics |
| `split_apply` | `document`, `feature_set`, `assignment` (`tables`, `colocate_dependencies`) | `project_yaml`, `groups`, `mapping`, `compatibility_view_sql`, `validation` |
| `parity_sql` | `legacy_table`, `legacy_as_of_column`, `keys`, `as_of` (`document`, `deployment`, `mapping`, `tolerances`) | `sql`, `mapping` |

The repository's `.mcp.json` registers two servers for agents working in the checkout:
`featlas-reverse` (`python -m featlas_reverse.mcp_server`) and `featlas`
(`target/release/fe-spec mcp serve`, see [MCP server](../reference/mcp.md)).

!!! note "Run from outside `python/`"
    `python -m featlas_reverse.mcp_server` imports the `featlas` package. If the current
    directory is `python/` and the extension was installed as a wheel (not with
    `maturin develop`), the source `python/featlas/` without the compiled `_native` module can
    shadow the installed package; `.github/workflows/ci.yml` notes the same effect for pytest.

## Python modules

| Module | Main entry points |
| --- | --- |
| `featlas_reverse.analyze` | `analyze_sql(sql, dialect="databricks") -> Analysis` (`to_dict()`); dataclasses `Window`, `Output`, `Analysis` |
| `featlas_reverse.describe` | `load(path_or_dict)`, `column(desc, name)`, `logical_type(spark_type)`, `from_spark(spark, table)` |
| `featlas_reverse.draft` | `draft_spec(target, analysis, sources, decisions) -> {"project", "report"}`, `to_yaml(project)` |
| `featlas_reverse.characterize` | `legacy_sample(spark, analysis, views, as_of, param="as_of")`, `eligibility_sql(analysis)` |
| `featlas_reverse.split` | `feature_profile(...)`, `analyze_split(project, feature_set, legacy=None, max_groups=6, min_size=2)`, `apply_split(project, feature_set, assignment, tables=None, colocate_dependencies=False)` |
| `featlas_reverse.parity` | `parity_sql(legacy_table, legacy_as_of_column, keys, mapping, as_of_literal, tolerances=None, new_as_of_column="feature_as_of")`, `mapping_from_project(project, deployment=None)` |

## Agent skills (`.claude/skills/`)

| Skill | When | Produces |
| --- | --- | --- |
| `featlas-reverse-engineer` | Document, port, migrate or "spec out" a legacy feature table; first step before splitting or regenerating | `spec.yaml` (validates, fixtures pass), `drafting-report.json`, a review note (assumptions, open questions, legacy divergences, unsupported columns) |
| `featlas-split-wide-table` | A feature set that is too wide | Split spec, column mapping, compatibility view, `split-decision.md` decision record ([framework](splitting.md#step-2-decide)) |
| `featlas-regenerate` | Rebuild or re-platform tables from a spec | Generated artifacts, (authorized) builds, parity results per `as_of` with explained differences, a cutover proposal |

Each skill is read-only except where it explicitly requires authorization (publishing new
tables); none modifies or drops the legacy table, and cutover is always left to a human.

## Tests

| File | CI job | Needs |
| --- | --- | --- |
| `python/tests/test_reverse.py` | `spark-differential` | pyspark 4.0.1, Java, sqlglot |
| `python/tests/test_reverse_mcp.py` | `python-bindings` | sqlglot |

See [Reverse engineering → Evidence](index.md#evidence).
