# Feature Engineering Specification — 0.2.0

A portable specification bundle for scalar tabular ML features, with **Azure Databricks as the required first production implementation target**. The canonical contract, feature identities, and consumer API remain independent of the feature store. Compute, catalog, offline storage, online storage, and orchestration can be rebound through separate adapter roles.

The bundle contains JSON Schemas, normative semantics, source relationships and lineage, complete YAML projects, synthetic acceptance fixtures, a bounded local verification kit, and a final coding-agent handoff. It does not contain a production feature platform or a verified live Databricks integration. Example data, resources, adapter declarations/versions, configuration, budgets, and teams are synthetic; real runtime locks must be verified on the target.

## Start here

| Goal | Read or run |
| --- | --- |
| Hand off implementation | [CODING_AGENT_PROMPT.md](CODING_AGENT_PROMPT.md), then [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) |
| Understand normative meaning | [SPECIFICATION.md](SPECIFICATION.md) and [SEMANTIC_RULES.md](docs/SEMANTIC_RULES.md) |
| Inspect the structural contract | [project.schema.json](schemas/project.schema.json) |
| Author a project | [00_minimal_template.yaml](examples/00_minimal_template.yaml) |
| Understand source tables, joins, and lineage | [SOURCE_RELATIONSHIPS.md](docs/SOURCE_RELATIONSHIPS.md) |
| Understand adapter boundaries | [PORTABILITY.md](docs/PORTABILITY.md) |
| Implement the first target | [DATABRICKS_MAPPING.md](docs/DATABRICKS_MAPPING.md) |
| Plan specification/store migration | [MIGRATION.md](docs/MIGRATION.md) and [CHANGE_MANAGEMENT.md](docs/CHANGE_MANAGEMENT.md) |
| Review scope and prior art | [GAPS_AND_DECISIONS.md](docs/GAPS_AND_DECISIONS.md) and [PRIOR_ART.md](docs/PRIOR_ART.md) |
| Inspect evidence and sources | [VERIFICATION.md](VERIFICATION.md) and [REFERENCES.md](docs/REFERENCES.md) |

## Architecture and implementation direction

Logical definitions describe entities, sources, relationships, features, feature sets, and consumers. Deployment `adapters`, role selectors, complete column/output mappings, and `consumer_bindings` supply physical implementation choices. One adapter may implement multiple roles without coupling their public contracts. Runtime requirements and capability reports distinguish declarations from verified support.

The initial production path is a canonical plan lowered to generated Spark SQL/DataFrame execution into Unity Catalog feature tables. Feature Views are an optional, validated lowering. Offline publication does not establish online-store or serving support; online capabilities require separate evidence. Applications consume logical identities, full keys, and time requests through a neutral API rather than physical table names or vendor SDK types.

A Rust canonical core with thin Python Databricks SDK/host I/O is the current greenfield recommendation, subject to repository constraints. The invariant is a single semantic implementation with no vendor dispatch in the core. Explicit `native_bindings` are permitted only under the declared native policy; ordinary `extensions` remain metadata. Source contract references and lineage support future ODCS integration, with no current ODCS import or execution claimed.

Version 0.2.0 changes deployment bindings from the earlier provider/engine shape to role-based adapters and complete mappings. Follow the migration guide; do not infer missing keys, clocks, or publication semantics from defaults.

## Semantic guarantees

The portable baseline defines exact aggregates, recency, lookup, and derived expressions. UTC timestamps have at most microsecond precision; durations are elapsed seconds; boundaries are explicit. Availability filtering precedes revision selection, which precedes event/business/valid-interval filtering. Tenant keys, historical dimension identity, population reconstruction, and coverage evidence remain part of the contract.

Empty, null, outage, and unknown-history states are distinct. Historical-serving replay uses observed publications, including latest-null supersession, and differs from source-as-known recomputation. Learned/custom contracts require immutable artifacts, explicit capabilities, and independent execution evidence. Structural validity does not establish executable support.

## Example projects

| YAML file | Main use |
| --- | --- |
| [00_minimal_template.yaml](examples/00_minimal_template.yaml) | Complete starting point with a known-empty count. |
| [01_fault_count.yaml](examples/01_fault_count.yaml) | First implementation slice: seven-day count, coverage, outages, tenants, and publication replay. |
| [02_historical_temperature.yaml](examples/02_historical_temperature.yaml) | Corrections/retractions and historical dimensions with availability and valid time. |
| [03_commerce_ratio.yaml](examples/03_commerce_ratio.yaml) | Integer money, dependent features, safe ratios, and separate labels. |
| [04_fitted_and_custom_contracts.yaml](examples/04_fitted_and_custom_contracts.yaml) | Immutable artifacts and explicit execution capability boundaries. |
| [05_lookup_and_recency.yaml](examples/05_lookup_and_recency.yaml) | Latest-value ties/nulls and floor-rounded subsecond recency. |
| [06_store_migration.yaml](examples/06_store_migration.yaml) | Storage rebinding with a local test double; boundary separation, not a second production store. |

Every YAML project is self-contained, with one version per ID in each collection and exact reference pins. Imports are not implicit. See [EXAMPLE_USES.md](docs/EXAMPLE_USES.md) for walkthroughs.

## Run the supplied verification kit

Use Python 3.12 and install the verification dependencies in a virtual environment. Exact observed versions, counts, coverage, and limitations belong in [VERIFICATION.md](VERIFICATION.md).

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python tools/verify_bundle.py
```

On Windows, activate with `.venv\Scripts\activate`. Other existing commands are:

```sh
python tools/verify_bundle.py --project examples/01_fault_count.yaml --fixtures
python tools/verify_bundle.py --json
python -m unittest discover -s tests -v
python tools/verify_manifest.py
```

The default bundle command checks schemas, complete examples, implemented semantic checks, supported fixtures, artifact digests, and regression/negative tests. `--project` alone validates one document; add `--fixtures` to run its supported cases. Unsupported adapter cases are **not executed**, never passed. Verification does not fetch schema references, import custom artifacts, query production tables, or call cloud services. The future `fe-spec` CLI is an implementation target, not a tool shipped here.

## Evidence and archive integrity

Schema validity, semantic validation, local conformance, mapping tests, and live target execution establish different things. The implementation handoff requires independently reviewed goldens, explicit capability limits, migration/backfill/replay/cutover obligations, and fresh evidence tied to exact candidate bytes. Refer to the verification record for observed outcomes rather than assuming every normative requirement is implemented.

`MANIFEST.json` records the byte length and SHA-256 of every payload file except itself. `python tools/verify_manifest.py` checks those bytes; additional user files, environments, and caches are outside its scope. The delivered ZIP has a separate checksum. The archive excludes installed dependencies, credentials, environment files, private development logs, and temporary authoring scripts.
