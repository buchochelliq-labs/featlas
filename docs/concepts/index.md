# Concepts

**In plain terms:** a feature is only trustworthy if everyone agrees on *what* was counted,
*which data was knowable when*, *what an empty result means*, and *when the value was actually
available to a model*. The specification makes each of those explicit; featlas checks and
executes them exactly. These pages explain the ideas you need before reading the CLI or
Databricks material.

| Page | Question it answers |
| --- | --- |
| [The specification model](spec-model.md) | What objects does a project contain and how do they refer to each other? |
| [Time semantics](time.md) | Which rows count for a value at time *T*? |
| [Missingness and coverage](missingness.md) | What is the difference between `0`, `null`, "no history" and "source down"? |
| [Publication and replay](publication.md) | When was a value visible, and what could a consumer have read then? |
| [Identity and hashing](identity.md) | Which hash changes when the meaning changes, and which when only bindings change? |
| [Quality policies](quality.md) | What happens when a value fails a check: stop, warn or quarantine? |
| [Labels](labels.md) | How are forward-looking training labels built without leaking the future? |
| [Capabilities and readiness](capabilities.md) | Can a target run this, and on what evidence? |

The normative text is the vendored `spec/feature-engineering-spec-v0.2.0/SPECIFICATION.md`
and its rule register `docs/SEMANTIC_RULES.md`. Where the specification left a choice open,
featlas records the interpretation in [DECISIONS.md](../DECISIONS.md); those choices are cited
by decision number (D1–D8).
