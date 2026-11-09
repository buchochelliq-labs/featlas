# Example uses and verification

All examples are synthetic, complete project documents. Names, owners, coordinates, budgets, and artifact values are illustrative; they are not records of a real organization or an executed deployment.

## 1. Choose an example

| File | Use case | What to inspect |
| --- | --- | --- |
| [00_minimal_template.yaml](../examples/00_minimal_template.yaml) | Commented starting template for a 60-second fault count. | Required structure, explicit population, column descriptions, empty versus unknown, and one small fixture. |
| [01_fault_count.yaml](../examples/01_fault_count.yaml) | Seven-day device fault count. | Event/availability boundaries, duplicates, tenant isolation, coverage, outages, and observed publication replay. |
| [02_historical_temperature.yaml](../examples/02_historical_temperature.yaml) | Hot-reading count with a changing device threshold. | Revisioned events, retractions, corrections moving timestamps, availability-aware historical dimension joins, interval boundaries and cardinality. |
| [03_commerce_ratio.yaml](../examples/03_commerce_ratio.yaml) | Seven-day and 30-day spend plus a derived ratio. | Integer money units, exact versioned dependencies, safe division, and a separately declared forward label. |
| [04_fitted_and_custom_contracts.yaml](../examples/04_fitted_and_custom_contracts.yaml) | Frozen scaler followed by a custom priority score. | Split-specific artifact metadata, immutable digests, typed contracts, missing capabilities, and adapter-required goldens. |
| [05_lookup_and_recency.yaml](../examples/05_lookup_and_recency.yaml) | Latest fault classification and elapsed time since a fault. | Floor rounding, exact timestamp ordering, tie diagnostics, and a successful null lookup. |

Core examples include development and production binding intent. These blocks show how logical definitions map to environment-specific coordinates; they do not create tables, schedules, endpoints, or accounts.

## 2. Commands that run in this archive

From the extracted bundle root, install the dependencies as described in [README.md](../README.md), then run:

```sh
python tools/verify_bundle.py
```

That is the bundled verification kit. Its report identifies what it checked, any failures, and limitations.

To check one complete project document:

```sh
python tools/verify_bundle.py --project examples/01_fault_count.yaml
```

To request the local fixture checks or a machine-readable report:

```sh
python tools/verify_bundle.py --fixtures
python tools/verify_bundle.py --json
```

A schema/semantic pass is not evidence that arbitrary learned/custom code ran or that a Databricks deployment works. A case whose expected result is `UNSUPPORTED_CAPABILITY` verifies the rejection boundary; it does not implement that capability. Suites marked `adapter_required` need a separately implemented, verified adapter.

The current verification report is the source for actual executed counts and coverage. This guide intentionally does not hard-code counts that would become stale as fixtures evolve.

## 3. Fault count: make “zero” meaningful

The feature `device.fault_count_7d` uses `device_registry` as its population and `fault_events` as its event source. Its entity key includes `tenant_id` and `device_id`.

The event window is **[T − 604,800 seconds, T)**, and source availability is **at or before T**. An event exactly at the lower boundary is included. An event exactly at T is excluded. An event within the window that arrives after T is also excluded.

Its business predicate requires `is_fault == true`. False and null do not pass. With a known device and complete seven-day source coverage, no qualifying events produces value `0` with state `empty`. An outage produces the declared unavailable behavior instead. Four days of coverage cannot establish a seven-day zero.

The replay consumer makes a separate claim: it selects a feature value that was actually published by `requested_at`. A newer computation that has not yet been published remains invisible. A new visible null supersedes an older numeric value, and a stale value triggers the freshness policy.

### How to adapt this pattern

Replace the source dictionary and provenance, population/key mapping, predicate, output unit, time window, and operational contracts. Then update all input-column and lineage declarations together. Keep the edge cases and reason through the expected values independently.

Changing only a source table path without reviewing the logical source contract is insufficient.

## 4. Historical temperature: both clocks matter

The feature `device.hot_reading_count_24h` counts readings at or above the threshold that applies at each reading's event time.

Its relationship is `reading_to_device_threshold`:

| Side | Logical source | Join columns |
| --- | --- | --- |
| From | `temperature_readings` | `tenant_id`, `device_id` |
| To | `device_threshold_history` | `tenant_id`, `device_id` |

The join is left/many-to-one. Unmatched thresholds remain null; a null comparison does not classify the reading as hot. Multiple matching threshold intervals fail.

The dimension's logical key includes a stable `interval_id`. This matters when a correction changes `valid_from`: the correction still belongs to the same logical interval and must replace the earlier revision before interval filtering.

A representative expected result is easy to review: a 75°C reading exceeds an old 70°C threshold, but does not exceed a later 80°C threshold. A correction to the old threshold arriving after T must not influence the result at T. At the exact interval boundary, the new interval applies because validity is lower-inclusive and upper-exclusive.

The fixtures also include a selected reading revision whose event time moves outside the window and an available tombstone. Both must remove the earlier contribution. Filtering before revision selection would get these cases wrong.

## 5. Commerce: units and dependency meaning

`customer.spend_7d` and `customer.spend_30d` sum nonnegative settled purchase amounts in **integer USD cents**. Their result type is `integer`, and expected integer results use zero tolerance.

`customer.recent_spend_ratio` refers to both feature IDs at exact versions and aligns them to the same customer and T. In its main golden case:

- Seven-day spend is 2,500 cents.
- 30-day spend is 10,000 cents.
- The ratio is 2,500 / 10,000 = **0.25**.

Matching money units cancel. A zero denominator uses `safe_divide` and produces an intentional nullable result, with state `ok`. That null has a different reason from incomplete history or an outage.

The consumer's label is separately defined over a future seven-day horizon. Its example label sums the source's observed purchase amounts under the label's own declared rules; it is not automatically the same settled/nonnegative filter as the features. A business target that needs a different predicate should use a precisely defined label source or a future explicit label extension. Horizon maturity, availability, and censoring must be checked by a label-capable implementation.

The archive's feature fixtures do not by themselves prove full training-dataset or label construction. Consult the report for the implementation boundary.

## 6. Learned and custom: contract before execution

The fourth example contains a frozen scaler artifact at [fault-count-scaler-v1.json](../examples/artifacts/fault-count-scaler-v1.json) and an inert behavioral reference at [priority-score-reference.txt](../examples/artifacts/priority-score-reference.txt). Their declared SHA-256 digests identify the bundled bytes.

The mathematical goldens are independently understandable:

1. The upstream fault count is 4.
2. The scaler's synthetic mean is 2 and scale is 2.
3. The expected standardized value is (4 − 2) / 2 = **1.0**.
4. The custom score clips that value divided by 4 to [0, 1], producing **0.25**.

These are adapter acceptance expectations, not evidence that the bundle fitted a model or executed the custom code. The tiny scaler values are explicitly synthetic. The custom artifact is text describing behavior, not an installed executable implementation.

A conforming adapter must verify the artifact digest, split identity, input/output contract, finite arithmetic, and resource limits; attest its own executable implementation; and pass the adapter-required goldens. A wrong split must fail even if its numeric parameters happen to look plausible.

A future fitting workflow must create a separate artifact for each training split/fold and record the training data identity, recipe, cutoffs, and implementation. It must never refit while applying to evaluation or serving rows.

## 7. Recency and lookup: ordering is part of meaning

The final example defines `device.last_fault_age_seconds` and `device.latest_fault_classification`. Recency returns integer elapsed seconds with required `rounding: floor`, so an event 59.9 seconds before T has age **59**. A complete window with no qualifying fault uses the declared known-empty null state.

The lookup orders eligible events by its declared timestamp ordering. A later false classification supersedes an earlier true one. Two eligible events tied on the complete ordering produce `LOOKUP_TIE`; they do not trigger arbitrary first-row selection. A selected null classification is a successful nullable lookup and does not resurrect an older non-null value.

## 8. Suggested future product workflows

The commands below are **design examples for the future product**, not executable commands supplied in this ZIP:

```sh
fe-spec validate examples/02_historical_temperature.yaml --format json
fe-spec explain examples/02_historical_temperature.yaml \
  --feature device.hot_reading_count_24h
fe-spec test examples/02_historical_temperature.yaml \
  --suite temperature_history_conformance
fe-spec plan project.yaml --deployment DEPLOYMENT_ID --format json
fe-spec generate project.yaml --deployment DEPLOYMENT_ID --out generated/
```

Use [CODING_AGENT_PROMPT.md](../CODING_AGENT_PROMPT.md) to implement those interfaces or equivalent repository-appropriate names. Generated code should remain reviewable and unexecuted until the target, capabilities, permissions, budgets, and intended action are established.

## 9. Add a new feature safely

1. Define the entity and population semantics.
2. Describe every source and column, including record identity, availability, revisions, provenance, and retained history.
3. Define reusable relationships and their cardinality, time, and unmatched behavior.
4. Declare the transform using typed inputs, exact dependency versions, explicit bounds, output type/unit, and missingness.
5. Add complete lineage, including population, time, revision, deletion, validity, ordering, and coverage influences.
6. Define consumer freshness, labels where needed, and environment-specific binding intent.
7. Add isolated goldens that distinguish the intended behavior from plausible wrong implementations.
8. Run the bundled checks, then the future product's capability and target acceptance stages as applicable.
9. Record the exact candidate evidence and review the semantic diff before changing a consumer or publication.

A useful golden answers “which rows can contribute, and why?” before it answers “what number should the code return?”


## Store switching without changing feature meaning

Use [06_store_migration.yaml](../examples/06_store_migration.yaml) with `python tools/verify_bundle.py --project examples/06_store_migration.yaml --fixtures`. Its logical entities, sources, relationships, features, sets, consumers, and golden expectations are identical to example 01. One deployment selects the illustrative Databricks stack; another keeps its compute/catalog/scheduler but replaces offline storage and consumer routing with a synthetic alternative adapter. Output keys, values, states, and original publication metadata are renamed explicitly.

The full bundle command also runs mapping round-trip and negative binding checks and validates the [unverified capability report](../examples/reports/databricks_capabilities_unverified.json). These are local consistency checks. The first actual implementation remains Databricks; a real store migration requires a working destination adapter, retained data/history, parity evidence, and a reviewed cutover plan. See [PORTABILITY.md](PORTABILITY.md) and [MIGRATION.md](MIGRATION.md).
