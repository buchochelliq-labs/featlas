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


LIVE_CATALOG = "featlas"


def main() -> None:
    (ROOT / "projects" / "live").mkdir(exist_ok=True)
    for src in sorted(EXAMPLES.glob("0*.yaml")):
        text = rewrite(src.read_text(encoding="utf-8"))
        if src.name == "05_lookup_and_recency.yaml":
            text = databricks_lookup(text)
        header = f"# Derived from spec/feature-engineering-spec-v0.2.0/examples/{src.name} by projects/derive.py.\n# Only deployment adapter pins/local output formats differ (05 also gains lookup_prod); logical contracts are unchanged.\n"
        (ROOT / "projects" / src.name).write_text(header + text, encoding="utf-8")
        # Live variant: the same project with Unity Catalog tables in the live test catalog.
        live = text.replace("\n      - example_catalog\n", f"\n      - {LIVE_CATALOG}\n")
        live_header = header + f"# Live variant: Unity Catalog namespace example_catalog -> {LIVE_CATALOG} (deployment bindings only).\n"
        (ROOT / "projects" / "live" / src.name).write_text(live_header + live, encoding="utf-8")


def platform() -> None:
    """projects/live/platform.yaml: live 01 plus two deployments that exercise the platform
    surfaces on Databricks: ``fault_job`` (the scheduled job, 5-minute grid, its own output
    table) and ``fault_online`` (single-surface online deployment: latest-snapshot staging table
    published to an Online Feature Store, observed through a Feature Serving endpoint). Logical
    definitions and acceptance goldens are unchanged."""
    import copy

    import yaml

    doc = yaml.safe_load((ROOT / "projects/live/01_fault_count.yaml").read_text(encoding="utf-8"))
    prod = next(d for d in doc["deployments"] if d["id"] == "fault_prod")
    job = copy.deepcopy(prod)
    job.update(id="fault_job", name="Fault count scheduled job (live platform test)", environment="staging",
               description="The fault_prod contract on a 5-minute grid with its own output table; deployed as a Databricks job by the live platform test.")
    job["schedule"] = {"mode": "interval", "every_seconds": 300, "offset_seconds": 0}
    job["output_targets"][0]["location"]["name"] = "device_reliability_job"
    job["slo"]["max_compute_seconds"] = 900  # serverless job start-up is part of a run
    online = copy.deepcopy(prod)
    online.update(id="fault_online", name="Fault count online (live platform test)", environment="staging",
                  description="Single-surface online deployment: the latest complete snapshot is staged in a Unity Catalog feature table, published to a Databricks Online Feature Store and observed through a Feature Serving endpoint.")
    online["schedule"] = {"mode": "manual"}
    online["execution"]["orchestrator"] = None
    online["adapters"][0]["roles"] = ["compute", "catalog", "online_store"]
    online["adapters"][0]["configuration"]["online_store"] = {"name": "featlas-free", "capacity": "CU_1", "serving_endpoint": "featlas-device-reliability"}
    t = online["output_targets"][0]
    t["role"] = "online_store"
    t["location"]["name"] = "device_reliability_online_src"
    online["publication"]["surface"] = "online_store"
    online["consumer_bindings"] = [{"consumer": "reliability_batch", "version": "1.0.0", "adapter": "databricks_platform", "role": "online_store"}]
    online["runtime"]["required_capabilities"] = [c for c in online["runtime"]["required_capabilities"] if c["capability"] not in ("core.scheduling", "core.serving_replay") and c["role"] != "orchestrator"]
    for c in online["runtime"]["required_capabilities"]:
        if c["role"] == "offline_store":
            c["role"] = "online_store"
    doc["deployments"] += [job, online]

    class Dumper(yaml.SafeDumper):
        def ignore_aliases(self, data):  # noqa: D401
            return True

    def str_rep(d, s):
        style = "'" if re.fullmatch(r"(?i)(y|n|yes|no|on|off|true|false|null|~|[-+]?[\d._:]+([eE][-+]?\d+)?|\d{4}-\d\d-\d\d.*)", s) or s == "" else None
        return d.represent_scalar("tag:yaml.org,2002:str", s, style=style)

    Dumper.add_representer(str, str_rep)
    header = "# Generated by projects/derive.py (platform): projects/live/01_fault_count.yaml plus deployments fault_job and fault_online.\n"
    (ROOT / "projects/live/platform.yaml").write_text(header + yaml.dump(doc, Dumper=Dumper, sort_keys=False, allow_unicode=True, width=100), encoding="utf-8")


if __name__ == "__main__":
    main()
    platform()
