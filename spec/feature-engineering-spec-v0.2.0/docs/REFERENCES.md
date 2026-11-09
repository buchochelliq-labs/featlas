# References

These sources inform the format and platform mapping. The normative feature semantics are defined by this bundle; they are not an assertion that every referenced platform implements those semantics automatically.

**Retrieval date: 2026-11-09.** JSON Schema and YAML links below identify specific published specification versions. Databricks documentation, Feast `master`, and scikit-learn `stable` are evolving references; recheck relevant APIs and pin exact implementation versions before building an adapter. All Databricks references below are official Azure Databricks documentation and were reviewed for this revision. Their documented capabilities do not establish what is installed or enabled in a particular workspace. No general-availability or preview status is assumed from cached search summaries.

## ref-json-schema-core

**JSON Schema Draft 2020-12**

- URL: [https://json-schema.org/draft/2020-12/](https://json-schema.org/draft/2020-12/)
- Used for: schema dialect, local definitions/references, composition, and validation vocabulary.
- Boundary: the schema describes document structure. Cross-object references, semantic DAGs, source histories, runtime capability, and physical data guarantees require additional validation.

## ref-json-schema-validation

**JSON Schema Validation: A Vocabulary for Structural Validation of JSON**

- URL: [https://json-schema.org/draft/2020-12/json-schema-validation](https://json-schema.org/draft/2020-12/json-schema-validation)
- Used for: type, numeric, string, array, object, and annotation keywords.
- Boundary: annotation keywords such as `default` do not by themselves insert missing values. Format validation must be deliberately configured; this bundle also imposes its explicit UTC profile.

## ref-yaml

**YAML Ain't Markup Language, Version 1.2.2**

- URL: [https://yaml.org/spec/1.2.2/](https://yaml.org/spec/1.2.2/)
- Used for: the underlying human-editable serialization format.
- Boundary: this bundle deliberately restricts YAML to a JSON-compatible profile. YAML features prohibited by this bundle remain prohibited even if a general YAML parser accepts them.

## ref-databricks-authoring

**Databricks Feature Store: authoring strategies**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/)
- Used for: the distinction between declarative Feature Views and application-populated feature tables, and the recommendation of Feature Views for new use cases.
- Boundary: the bundle deliberately starts with an owned Spark SQL/DataFrame pipeline into feature tables to make temporal reconstruction reviewable. A platform recommendation does not prove equivalence with the canonical contract.

## ref-databricks-uc-tables

**Feature tables in Unity Catalog**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/uc/feature-tables-uc](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/uc/feature-tables-uc)
- Used for: Delta tables with primary-key metadata as feature tables, supported creation/update paths, and timestamp-key designation.
- Boundary: table registration is distinct from computing correct features. The adapter must verify actual keys, source history, snapshots, types, permissions, and publication behavior.

## ref-databricks-constraints

**Constraints on Azure Databricks**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/tables/constraints](https://learn.microsoft.com/en-us/azure/databricks/tables/constraints)
- Used for: the distinction between enforced `NOT NULL`/`CHECK` constraints and informational primary-key, foreign-key, and unique constraints.
- Boundary: do not infer uniqueness, join cardinality, or referential integrity from declared key metadata. Validate these properties before relying on them.

## ref-databricks-feature-views

**Feature Views**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-views](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-views)
- Used for: declarative feature authoring, training, materialization, supported source/function families, and documented limitations.
- Boundary: a Feature View is an optional adapter lowering, not the portable feature model. Exact workspace/runtime/package requirements are probed at implementation time.

## ref-databricks-feature-views-api

**Feature Views API reference**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-views-api-reference](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-views-api-reference)
- Used for: source, function, window, dependency, access-control, and training/materialization interfaces.
- Boundary: supported native operators and dependency shapes are narrower than an arbitrary portable DAG. Validate each lowering, including timestamp meaning, source lateness, null behavior, and native dependency restrictions.

## ref-databricks-materialization

**Materialize Feature Views**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/materialized-features](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/materialized-features)
- Used for: distinct batch/streaming materialization paths and limitations, including batch rolling windows, column selections, derived/UDF features, and streaming historical corrections.
- Boundary: offline recomputation and online materialization need separate conformance evidence. A supported materialization API does not prove the bundle's atomic publication or historical serving replay requirements.

## ref-databricks-concepts

**Databricks Feature Store overview and glossary**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/concepts](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/concepts)
- Used for: distinctions among Feature Views, feature tables, offline/online stores, `FeatureLookup`, and `FeatureSpec`. A FeatureSpec is a reusable catalog entity describing feature lookups/functions and their lineage.
- Boundary: these concepts guide adapters. A FeatureSpec is not an atomic publication unit or evidence that all member values belonged to one committed snapshot. Serving it requires the appropriate online resources.

## ref-databricks-time-series

**Point-in-time feature joins**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/time-series](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/time-series)
- Used for: timestamp-key/as-of joins, the distinction from Delta table-version time travel, latest-value online publication, null selection, and the scope of `lookback_window`.
- Boundary: source availability and observed publication remain separate semantics. The documented time-series online path does not provide historical as-of lookup, and training lookback limits do not enforce online freshness.

## ref-databricks-serving

**Serve feature tables**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-function-serving](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/feature-function-serving)
- Used for: feature-output versus model-output serving, FeatureSpec-based endpoints, dependency versioning, and live endpoint updates. The documentation notes that FeatureSpecs/UDFs have no version field and describes versioning by name.
- Boundary: Ready status alone is insufficient: dependency recreation can invalidate grants, and an in-place UDF update need not replace deployed logic. Verify the actual query path and versioned migration. This archive neither provisions nor tests an endpoint.

## ref-databricks-online-store

**Databricks Online Feature Stores**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-store](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-store)
- Used for: Lakebase-backed online storage; `TRIGGERED`, `CONTINUOUS`, and `SNAPSHOT` synchronization; CDF prerequisites; unsupported publish parameters; and endpoint routing limitations.
- Boundary: endpoints resolve the oldest online table when an offline table has several published copies; publishing another copy is not a routing switch. New Lakebase Autoscaling endpoints cannot combine multiple online stores. The page does not establish atomic feature-set visibility across synchronization and serving; that requires separate evidence.

## ref-databricks-acid

**What are ACID guarantees on Azure Databricks?**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/lakehouse/acid](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/acid)
- Used for: Delta transaction commits, default transaction scope, and support for multi-table atomic transactions when catalog commits are enabled on participating tables.
- Boundary: do not state that Databricks universally lacks multi-table transactions. Equally, Delta atomicity does not establish an atomic transaction covering online synchronization or endpoint reads.

## ref-databricks-transactions

**Transaction modes**

- URL: [https://learn.microsoft.com/en-us/azure/databricks/transactions/transaction-modes](https://learn.microsoft.com/en-us/azure/databricks/transactions/transaction-modes)
- Used for: interactive/non-interactive transaction mechanisms and eligible table/compute prerequisites for atomic multi-table writes.
- Boundary: probe prerequisites in the actual target and pin successful versions. Documented transaction support is not evidence that an illustrative deployment can use it. Streaming checkpoint advancement also needs its own recovery/idempotence analysis.

## ref-feast-pit

**Feast: Point-in-time joins**

- URL: [https://docs.feast.dev/master/getting-started/concepts/point-in-time-joins](https://docs.feast.dev/master/getting-started/concepts/point-in-time-joins)
- Used for: historical feature lookup, lookback relative to each observation time, and the distinction between event-time matching and availability filtering.
- Boundary: the current page describes optional created-timestamp filtering. The specification's availability rules do not assume that event-time lookup alone prevents late-correction leakage.

## ref-sklearn-leakage

**scikit-learn: Common pitfalls and recommended practices**

- URL: [https://scikit-learn.org/stable/common_pitfalls.html](https://scikit-learn.org/stable/common_pitfalls.html)
- Used for: fitting preprocessing only on training data and consistently applying the fitted transformation to held-out data.
- Boundary: this motivates the split-specific artifact contract. It does not establish safety of an arbitrary serialized model, custom operation, or target-specific implementation.

## Reading order

Read the schema and normative specification for this bundle's behavior. Consult platform documentation when implementing an adapter. If platform behavior differs, emit an unsupported-capability diagnostic or propose a versioned extension; do not silently weaken the contract.

