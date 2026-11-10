# Writing feature specs

**In plain terms:** start from an example, change the logical parts (sources, entity, feature),
write a few small test cases whose expected answers you worked out by hand, and run
`fe-spec validate` and `fe-spec test` until both pass. Only then add or change a deployment,
which says where the data physically lives.

## A workflow

1. **Copy an example.** `spec/feature-engineering-spec-v0.2.0/examples/00_minimal_template.yaml`
   is the smallest complete project; `01_fault_count.yaml` is the reference for windows,
   availability, duplicates, tenants, coverage and replay.
2. **Describe sources precisely.** Every column needs a type, nullability, unit and a
   description saying what it measures (SPECIFICATION §4.1). Declare `event_time` *and*
   `available_time`; declare `record_semantics` honestly (`append_only` vs `versioned`).
3. **Define the entity and its population.** The population source decides who gets a row,
   including entities with no events.
4. **Write the feature.** Declare exactly the input columns you use, the window and its
   boundaries, the availability cutoff, missingness for every case, history requirements and the
   lineage role of every contributing column (SEM-LIN-001).
5. **Write golden cases by hand** in `acceptance_suites`. Expected values must be reasoned
   independently, not produced by featlas (SEM-TEST-002). Cover the decisive cases of
   SEM-TEST-001: event and availability boundaries, duplicates, corrections and retractions, null
   versus zero, empty population, coverage, tenant isolation, relationship cardinality, dimension
   availability, derived dependencies and publication timing.
6. **Validate and test:**

    ```sh
    fe-spec validate my_project.yaml
    fe-spec test my_project.yaml
    fe-spec explain my_project.yaml --feature my.feature_id
    fe-spec evaluate my_project.yaml --fixture my_case --format text
    ```

7. **Add a deployment** with real adapter pins, then `fe-spec plan` and `fe-spec capabilities`.

## Restricted YAML: common errors

The parser applies the specification's restricted profile before schema validation
(SEM-PARSE-001, DECISIONS D3). Things that are fine in ordinary YAML but rejected here:

| Write this | Not this | Why |
| --- | --- | --- |
| `status: "yes"` or `true` | `status: yes` | `yes`, `no`, `on`, `off`, `~`, `NULL`, `.inf`, `0x1F`, `1_000` resolve differently across YAML versions |
| `as_of: '2026-06-08T00:00:00Z'` | `as_of: 2026-06-08T00:00:00Z` | Plain timestamps/dates are implicit types |
| repeated blocks written out | `&anchor` / `*alias` / `<<:` | Anchors, aliases and merge keys are prohibited |
| one document | `---` separated documents | Single document only |
| `"literal ${HOME}"` is still rejected | `${…}` anywhere in a string | Interpolation markers are prohibited in any string |
| unique keys | duplicate keys | No "last key wins" |

Other limits: no explicit tags or `%YAML` directives, no BOM or NUL, integers must fit in i64,
numbers must be finite and must not underflow (`PARSE_NUMBER_RANGE`), document ≤ 4 MiB, depth ≤
128, ≤ 2 M nodes. JSON input must be strict JSON and then follows the same rules. featlas is
stricter than the bundled kit, which accepts e.g. `yes` as a string; all shipped examples pass
(SPEC_FINDINGS #8).

## Things the validator will insist on

* **Exact references.** `entity: device` must match one entity; versions in references must
  match exactly (SEM-ID-002).
* **Declared columns.** Every column used in a predicate, expression, ordering, grouping, join or
  coverage condition must be listed in the input's `columns` (SEM-TRANS-001).
* **Typed expressions.** No implicit string/number/boolean coercion; integer→number widening only
  (`value.rs`, SEM-TRANS-006).
* **Tenant keys.** Tenant keys must appear in grouping, joins, outputs, consumers and labels; a
  join that drops the tenant is `TENANT_JOIN_MISSING` (SEM-ENT-002). Tenant columns are derived
  from the tenant key's `group_by` mapping and relationship key pairs, not from column names
  (EVIDENCE C7).
* **Missingness agrees with the transform.** `missingness.null_input` must agree with the
  transform's null contract; `empty` uses the empty policy only after coverage is established.
* **Lineage completeness.** Missing time, key, revision, deletion, validity, coverage or filter
  lineage is a correctness gap (`lineage_must_cover_roles`).
* **Recency event time.** Recency must use the window's event-time column
  (`RECENCY_EVENT_TIME`, EVIDENCE C8).
* **Labels.** A label's `available_time_column` must equal the source's declared availability
  column (`LABEL_AVAILABILITY_COLUMN`, SPEC_FINDINGS #6).
* **Learned / custom contracts** validate structurally but are never executed; planning a
  deployment that needs them fails with `UNSUPPORTED_CAPABILITY` (DECISIONS D7).

## Changing a feature

Changing meaning (window, filter, unit, boundary, null policy, transform) requires a new semantic
`version` (SEM-ID-004, SEM-OUT-001). `fe-spec hash` makes the effect visible: the feature's
logical fingerprint changes. Changing only descriptions, owners, tags or `extensions` does not
change any logical fingerprint ([Identity](../concepts/identity.md)).

## Examples and derived projects

The vendored examples use synthetic adapter pins (`org.example.fe_spec:*@0.0.0-example`) that
deliberately fail planning with `UNRESOLVED_ADAPTER`. `projects/derive.py` regenerates
`projects/*.yaml` from them, changing only:

* adapter pins → `dev.featlas:local_reference`, `dev.featlas:databricks`,
  `dev.featlas:memory_store` at `0.1.0`;
* local output formats `parquet` → `jsonl` (what the local store writes);
* for example 05, an added Databricks deployment `lookup_prod` built from `lookup_dev`.

It also writes `projects/live/*.yaml`, the same projects with the Unity Catalog namespace
`example_catalog` replaced by `featlas` (deployment bindings only), used by the live workflow.
Regenerate with `python3 projects/derive.py`; do not hand-edit `projects/`.

| Example | Exercises |
| --- | --- |
| `00_minimal_template` | Smallest complete project |
| `01_fault_count` | Windows, availability, duplicates, tenants, coverage, publication replay |
| `02_historical_temperature` | Versioned sources, as-of dimension joins, late revisions |
| `03_commerce_ratio` | Aggregates, derived ratio, number tolerances |
| `04_fitted_and_custom_contracts` | Learned/custom contracts (validated, not executed); `adapter_required` suite |
| `05_lookup_and_recency` | Lookup ordering/ties, floor-rounded recency |
| `06_store_migration` | Rebinding the store; migration plan |

(The "Exercises" column summarizes each example's description and suites; read the file for the
authoritative list of cases.)
