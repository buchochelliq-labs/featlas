"""Differential tests: core-generated Spark SQL on local Apache Spark vs goldens and the core.

Evidence scope: local Apache Spark only. This does not satisfy the Azure
Databricks live acceptance gate; it checks that the generated statements
preserve the canonical stage order and contract semantics on a Spark engine.
Requires: pyspark, a Java runtime, PyYAML (test-only fixture reading) and a
built ``fe-spec`` binary.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
yaml = pytest.importorskip("yaml")

from featlas_databricks.core import Core  # noqa: E402
from featlas_databricks.differential import run_case, utc_process  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PROJECTS = [
    ("01_fault_count.yaml", "fault_prod"),
    ("02_historical_temperature.yaml", "temperature_prod"),
    ("03_commerce_ratio.yaml", "commerce_prod"),
    ("05_lookup_and_recency.yaml", "lookup_prod"),
    ("06_store_migration.yaml", "fault_prod"),
]
RESULTS: list[dict] = []


@pytest.fixture(scope="session")
def spark():
    utc_process()
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("featlas-differential")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    out = os.environ.get("FEATLAS_DIFFERENTIAL_REPORT")
    if out:
        Path(out).write_text(json.dumps({"spark_version": s.version, "results": RESULTS}, indent=2, default=str), encoding="utf-8")
    s.stop()


@pytest.fixture(scope="session")
def core():
    return Core()


def cases():
    for name, deployment in PROJECTS:
        doc = yaml.safe_load((ROOT / "projects" / name).read_text(encoding="utf-8"))
        for suite in doc["acceptance_suites"]:
            if suite["runner"] != "reference":
                continue
            for case in suite["cases"]:
                yield pytest.param(name, deployment, case, id=f"{name}:{case['id']}")


_GENERATED: dict = {}


def generated(core: Core, name: str, deployment: str) -> dict:
    key = (name, deployment)
    if key not in _GENERATED:
        out = tempfile.mkdtemp(prefix="featlas-gen-")
        _GENERATED[key] = core.generate(str(ROOT / "projects" / name), deployment, out)
    return _GENERATED[key]


@pytest.mark.parametrize("name,deployment,case", list(cases()))
def test_generated_spark_sql_matches_goldens_and_core(spark, core, name, deployment, case):
    project = str(ROOT / "projects" / name)
    result = run_case(spark, core, project, generated(core, name, deployment), case)
    result["project"] = name
    result["deployment"] = deployment
    RESULTS.append(result)
    assert result["core"] == "passed", f"core evaluator does not pass this golden: {result}"
    assert result["status"] == "passed", result


def test_custom_contract_deployment_is_rejected_before_code_generation(core):
    from featlas_databricks.core import CoreError

    with pytest.raises(CoreError) as e:
        core.generate(str(ROOT / "projects" / "04_fitted_and_custom_contracts.yaml"), "contracts_prod", tempfile.mkdtemp())
    assert e.value.code == "PLANNING_FAILED"
    assert any(d["code"] == "UNSUPPORTED_CAPABILITY" for d in e.value.detail)


# ---- Multi-violation cases: both engines must report the same *first* code -----------------

def _mutated(tmp_path, name: str, deployment: str, suite: int, case_index: int, mutate):
    doc = yaml.safe_load((ROOT / "projects" / name).read_text(encoding="utf-8"))
    case = doc["acceptance_suites"][suite]["cases"][case_index]
    mutate(case)
    path = tmp_path / name.replace(".yaml", ".json")
    path.write_text(json.dumps(doc), encoding="utf-8")
    return str(path), case


def _first_codes(spark, core, path, deployment, case):
    out = tempfile.mkdtemp(prefix="featlas-gen-")
    result = run_case(spark, core, path, core.generate(path, deployment, out), case)
    return result.get("spark_code"), result.get("core_code")


def test_invalid_interval_precedes_multiple_matches(spark, core, tmp_path):
    def mutate(case):
        case["source_rows"]["device_threshold_history"].append({
            "tenant_id": "tenant_a", "device_id": "device_2", "interval_id": "bad", "valid_from": "2026-06-05T00:00:00Z",
            "valid_to": "2026-06-04T00:00:00Z", "revision_no": 1, "available_at": "2026-06-01T00:00:00Z", "is_deleted": False, "critical_threshold_c": 50.0})
    path, case = _mutated(tmp_path, "02_historical_temperature.yaml", "temperature_prod", 0, 6, mutate)
    assert _first_codes(spark, core, path, "temperature_prod", case) == ("INVALID_INTERVAL", "INVALID_INTERVAL")


def test_reconstruction_conflict_precedes_lookup_tie(spark, core, tmp_path):
    def mutate(case):
        rows = case["source_rows"]["fault_events"]
        twin = dict(rows[0])
        twin["is_fault"] = not rows[0]["is_fault"] if rows[0]["is_fault"] is not None else True
        rows.append(twin)
    path, case = _mutated(tmp_path, "05_lookup_and_recency.yaml", "lookup_prod", 0, 3, mutate)
    assert _first_codes(spark, core, path, "lookup_prod", case) == ("DUPLICATE_CONFLICT", "DUPLICATE_CONFLICT")
