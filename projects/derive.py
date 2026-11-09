#!/usr/bin/env python3
"""Regenerate projects/*.yaml from the vendored, unmodified examples.

Rewrites only deployment adapter implementation/version pins (synthetic
``org.example.fe_spec:*@0.0.0-example`` → installed ``dev.featlas:*@0.1.0``)
and local output formats (parquet → jsonl, which the local store writes).
For example 05, which ships only a local deployment, a Databricks deployment
``lookup_prod`` is appended, built from lookup_dev by text substitution.
Logical definitions and acceptance goldens are copied unchanged; the Rust
test suite asserts that logical fingerprints equal those of the examples.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "spec/feature-engineering-spec-v0.2.0/examples"

PINS = [
    ("implementation: org.example.fe_spec:local_reference", "implementation: dev.featlas:local_reference"),
    ("implementation: org.example.fe_spec:databricks", "implementation: dev.featlas:databricks"),
    ("implementation: org.example.fe_spec:mock_store", "implementation: dev.featlas:memory_store"),
    ("version: 0.0.0-example", "version: 0.1.0"),
]


def rewrite(text: str) -> str:
    for a, b in PINS:
        text = text.replace(a, b)
    text = re.sub(r"(path: \./example-output/\S+)\.parquet", r"\1.jsonl", text)
    return text.replace("      format: parquet", "      format: jsonl")


def databricks_lookup(text: str) -> str:
    start = text.index("- id: lookup_dev\n")
    end = text.index("acceptance_suites:\n")
    block = text[start:end]
    block = block.replace("- id: lookup_dev", "- id: lookup_prod").replace("name: Lookup Dev", "name: Lookup Prod")
    block = block.replace("environment: development", "environment: staging")
    block = block.replace("adapter: local_reference", "adapter: databricks_platform")
    block = block.replace("compute: local_reference", "compute: databricks_platform").replace("catalog: local_reference", "catalog: databricks_platform")
    for source in ("device_registry", "fault_events"):
        block = block.replace(
            "    location:\n      kind: fixture\n      suite: lookup_recency_conformance\n    version: 1.0.0\n    adapter: databricks_platform\n    column_mapping:\n      tenant_id: tenant_id\n      device_id: device_id\n      " + ("registered_at" if source == "device_registry" else "event_id"),
            f"    location:\n      kind: table\n      namespace:\n      - example_catalog\n      - synthetic_raw\n      name: {source}\n      format: delta\n    version: 1.0.0\n    adapter: databricks_platform\n    column_mapping:\n      tenant_id: tenant_id\n      device_id: device_id\n      " + ("registered_at" if source == "device_registry" else "event_id"),
            1,
        ) if source == "device_registry" else block.replace(
            "    location:\n      kind: fixture\n      suite: lookup_recency_conformance\n    version: 1.0.0\n    adapter: databricks_platform\n    column_mapping:\n      event_id: event_id",
            "    location:\n      kind: table\n      namespace:\n      - example_catalog\n      - synthetic_raw\n      name: fault_events\n      format: delta\n    version: 1.0.0\n    adapter: databricks_platform\n    column_mapping:\n      event_id: event_id",
            1,
        )
    block = re.sub(r"    location:\n      kind: file\n      path: \S+\n      format: \S+\n", "    location:\n      kind: table\n      namespace:\n      - example_catalog\n      - synthetic_features\n      name: device_latest_observations\n      format: delta\n", block)
    block = re.sub(
        r"  adapters:\n  - id: local_reference\n    implementation: \S+\n    version: \S+\n    roles:\n(    - \w+\n)+    configuration: \{\}\n",
        "  adapters:\n  - id: databricks_platform\n    implementation: dev.featlas:databricks\n    version: 0.1.0\n    roles:\n    - compute\n    - catalog\n    - offline_store\n    configuration:\n      connection_profile: example_databricks\n      feature_api: uc_feature_tables\n",
        block,
    )
    assert "local_reference" not in block, block
    return text[:end] + block + text[end:]


def main() -> None:
    for src in sorted(EXAMPLES.glob("0*.yaml")):
        text = rewrite(src.read_text(encoding="utf-8"))
        if src.name == "05_lookup_and_recency.yaml":
            text = databricks_lookup(text)
        header = f"# Derived from spec/feature-engineering-spec-v0.2.0/examples/{src.name} by projects/derive.py.\n# Only deployment adapter pins/local output formats differ (05 also gains lookup_prod); logical contracts are unchanged.\n"
        (ROOT / "projects" / src.name).write_text(header + text, encoding="utf-8")


if __name__ == "__main__":
    main()
