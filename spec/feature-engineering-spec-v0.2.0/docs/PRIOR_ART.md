# Existing standards and feature engineering systems

Research date: **2026-11-09**. This is a review of official documentation and project repositories, not a hands-on certification or an exhaustive novelty assessment. This revision chooses Databricks as the first target while retaining an independent authoring contract.

## 1. What already exists

| Project | Documented overlap | Consequence for this specification |
| --- | --- | --- |
| [Snowflake declarative Feature Store](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/feature-development-lifecycle) | YAML/Python entities, sources, feature views/groups, environment targets, and plan/apply workflow. | YAML-to-deployment is established prior art. A future Snowflake adapter should reuse its native objects where equivalent. |
| [Databricks Feature Views](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-views) and [FeatureSpec](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-function-serving) | Declarative transformations/materialization plus reusable feature/function serving definitions. | Use native supported mechanisms, with an owned Spark/SQL-to-UC-table path for the initial temporal contract. See the detailed [mapping](DATABRICKS_MAPPING.md). |
| [Chronon](https://chronon.ai/test_deploy_serve/Test.html) | Python definitions compile to Thrift represented as JSON for its execution engine. | A close predecessor for a compiled feature specification; its engine format is not evidence of cross-platform interchange. |
| [Chronon joins](https://chronon.ai/authoring_features/Join.html) | Observation/population source, keys, timestamps, feature groups, and configurable historical accuracy. | Reuse the distinction between observation timeline and feature source timing; prove exact equivalence of windows. |
| [Feathr](https://feathr-ai.github.io/feathr/concepts/feature-definition.html) | Sources, anchored features, derived features, typed keys, source timestamps, and feature dependencies. | A close predecessor for the source-to-feature dependency model. |
| [Feast BatchFeatureView](https://docs.feast.dev/getting-started/concepts/batch-feature-view) | Transformations, aggregation, dependent feature views, graph resolution, materialization, and historical retrieval. | Do not characterize Feast as only a registry. Assess its actual supported compute/backend combinations. |
| [Redis Feature Form](https://redis.io/docs/latest/develop/ai/featureform/concepts/) | Declarative resource graph, provider roles, and portability between compute environments. Its [apply workflow](https://redis.io/docs/latest/develop/ai/featureform/define-and-deploy-features/) supports planning. | A close precedent for logical/physical separation. The current [deployment](https://redis.io/docs/latest/operate/featureform/deploy/) requires a license key. |
| [Chalk agent tooling](https://docs.chalk.ai/docs/development-with-llms) and [MCP server](https://docs.chalk.ai/docs/mcp-server) | Feature-development prompts/skills and operational/query-plan tools for agents. | Agent prompts and MCP are established interfaces, not unique differentiators. |
| [Tecton Feature Views](https://docs.tecton.ai/docs/defining-features/feature-views) | Batch, streaming, and request-time transformations over registered sources and other feature views. | Execution lifecycle and reusable feature definitions are established prior art. |
| [Featuretools serialization](https://docs.featuretools.com/en/latest/generated/featuretools.save_features.html) and [time handling](https://docs.featuretools.com/en/stable/getting_started/handling_time.html) | Feature definitions serialized as JSON; secondary time indexes identify when columns become known. Serialized definitions are not guaranteed compatible across library versions. | Serialization and availability-aware inputs both have precedents; version and portability claims need evidence. |

Snowflake documents a current [declarative-authoring limitation](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/feature-store-declarative-authoring): real-time feature views are outside that plan/apply workflow, while streaming feature views are supported. Backend support must be checked by capability rather than inferred from a product category.

## 2. Standards to integrate

### ODCS and Data Contract CLI

The [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/latest/home/) documents version 3.2.0 and a JSON Schema for YAML contracts. It covers descriptions, schema, quality, ownership, service levels, infrastructure, and references. Its [relationship model](https://bitol-io.github.io/open-data-contract-standard/v3.2.0/references/) supports table/property relationships, composite keys, stable identifiers, and cross-contract references. Its [context block](https://bitol-io.github.io/open-data-contract-standard/v3.2.0/context/) explicitly supports AI-agent instructions, verified statements, constraints, join guidance, cardinality, and time-range guidance.

[Data Contract CLI agent workflows](https://cli.datacontract.com/ai-agents) cover importing, enriching, validating, testing, diagnosing, checking breaking changes, and generating artifacts. Its [ODCS support documentation](https://cli.datacontract.com/open-data-contract-standard) distinguishes structural acceptance/round-tripping from progressively implemented interpretation by tests/import/export. The older [Data Contract Specification](https://github.com/datacontract/datacontract-specification) is deprecated in favor of ODCS.

Recommended future integration: import locked ODCS source contracts into a resolved logical source snapshot and preserve contract IDs, versions, content digests, table/column IDs, and authoritative metadata. Validate imported fields rather than keeping competing authoritative copies. This bundle does **not** implement an ODCS importer, claim ODCS conformance, or use metadata extensions as an executable import directive.

### Apache Ossie and dbt

[Apache Ossie](https://github.com/apache/ossie) provides semantic schemas, validation, Python models, converters, and portability test cases. [dbt's Ossie integration](https://docs.getdbt.com/docs/build/ossie-semantic-models) has explicit limitations and can drop unsupported constructs with warnings. A feature adapter must treat dropped required semantics as blockers.

[dbt unit tests](https://docs.getdbt.com/reference/resource-properties/unit-tests) already express `given` inputs and `expect` outputs in YAML. Generate native tests from portable fixtures where suitable, while preserving the authoritative golden expectations and temporal interpretation.

### OpenLineage

[OpenLineage's object model](https://openlineage.io/docs/spec/object-model/) supports design-time and runtime events. Its [column-lineage facet](https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet/) distinguishes transformations/aggregation from indirect join, grouping, filtering, sorting, window, and conditional influences.

Recommended future integration: export declared plan dependencies and observed run lineage using that vocabulary. Source foreign keys, calculation dependencies, and observed execution lineage have different meanings and should remain distinguishable.

## 3. Product boundary selected here

The individual ingredients—declarative features, YAML/JSON, dependency graphs, temporal retrieval, versioning, fixtures, and agent tooling—already exist. The proposed contribution is a focused contract and conformance layer: precise logical meaning, explicit source relationships, capability-scoped target mappings, and evidence that an implementation preserves the required behavior.

The reviewed sources did not establish one independent standard covering every combination in this contract across unrelated runtimes, especially source availability/revisions, observed publication history, complete population/coverage, and target-specific capability evidence. That is a bounded research result, not proof of novelty or an assertion that other products cannot support those behaviors.

The implementation should reuse Databricks execution, catalog, feature tables, and supported serving mechanisms. It should not build a new distributed storage engine or scheduler as a prerequisite. Establish the selected target first, prove the adapter boundary with local fixtures, and add other real stores only when their adapters and semantics are tested.
