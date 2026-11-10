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
from featlas_databricks.differential import run_case  # noqa: E402
from featlas_databricks.errors import FeatlasError  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PROJECTS = [
    ("01_fault_count.yaml", "fault_prod"),
    ("02_historical_temperature.yaml", "temperature_prod"),
    ("03_commerce_ratio.yaml", "commerce_prod"),
    ("05_lookup_and_recency.yaml", "lookup_prod"),
    ("06_store_migration.yaml", "fault_prod"),
    # SEM-QUAL-001 failure handling (projects/quality/derive.py): warn, quarantine, error.
    ("quality/quality_warn.yaml", "fault_prod"),
    ("quality/quality_quarantine.yaml", "fault_prod"),
    ("quality/quality_error.yaml", "fault_prod"),
]
RESULTS: list[dict] = []


@pytest.fixture(scope="session")
def spark():
    from spark_session import get_spark

    s = get_spark()
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


def test_quarantine_reports_the_violating_rows_and_warn_keeps_values(spark, core):
    """Quarantine raises with the offending rows (nothing to publish); warn records a finding."""
    from featlas_databricks.differential import register_source
    from featlas_databricks.runner import SnapshotRun

    for name, expect in (("quality/quality_quarantine.yaml", "quarantine"), ("quality/quality_warn.yaml", "warning")):
        project = str(ROOT / "projects" / name)
        art = generated(core, name, "fault_prod")["artifacts"]["device_reliability"]
        doc = yaml.safe_load((ROOT / "projects" / name).read_text(encoding="utf-8"))
        case = doc["acceptance_suites"][0]["cases"][0]
        rel = {}
        for source, spec in art["expected_source_schemas"].items():
            register_source(spark, f"fixture_{source}", spec, case["source_rows"][source])
            rel[source] = f"fixture_{source}"
        cov = core.coverage(project, "fault_prod", "device_reliability", case["as_of"], case["context"]["source_status"])
        run = SnapshotRun(spark, art, case["as_of"], cov, set(), rel, population_source="device_registry")
        run.prepare()
        if expect == "quarantine":
            with pytest.raises(FeatlasError) as e:
                run.execute()
            assert e.value.code == "QUALITY_QUARANTINED"
            [q] = e.value.detail["quarantined"]
            assert q["entity_keys"] == {"tenant_id": "tenant_b", "device_id": "device_1"} and q["value"] == 2 and q["check"] == "range"
        else:
            run.execute()
            [w] = run.quality_warnings
            assert w["outcome"] == "warning" and w["value"] == 2 and w["entity_keys"]["tenant_id"] == "tenant_b"


LABELS = json.loads((ROOT / "projects/labels/spend_training_dataset.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", LABELS["cases"], ids=[c["id"] for c in LABELS["cases"]])
def test_generated_label_sql_matches_goldens_and_core(spark, core, case):
    """Label datasets through generated Spark SQL vs hand-reasoned goldens and the core."""
    from featlas_databricks.differential import register_source
    from featlas_databricks.labels import LabelRun

    project = str(ROOT / LABELS["project"])
    gen = core.generate(project, LABELS["deployment"])
    art = gen["label_artifacts"][LABELS["consumer"]]
    source_rows = LABELS["source_rows"] + case.get("extra_source_rows", [])
    rel = {}
    for source, spec in art["expected_source_schemas"].items():
        register_source(spark, f"fixture_label_{source}", spec, source_rows)
        rel[source] = f"fixture_label_{source}"
    want = case["expected"]
    label_input = {"rows": case["rows"], "source_rows": source_rows, "source_status": case["source_status"]}
    try:
        core_ds = core.labels(project, LABELS["consumer"], label_input)
        core_code = None
    except Exception as e:  # noqa: BLE001 - FeatlasError from the bindings
        core_ds, core_code = None, getattr(e, "code", None)
    requests = core.label_requests(project, LABELS["consumer"], case["rows"], case["source_status"])
    run = LabelRun(spark, art, requests, source_relations=rel)
    if "diagnostic" in want:
        with pytest.raises(FeatlasError) as e:
            run.run()
        assert e.value.code == want["diagnostic"]["code"] == core_code
        return
    ds = run.run()
    rows = [{k: r[k] for k in ("request_index", "value", "state", "eligible_rows")} for r in ds["rows"]]
    assert rows == want["rows"]
    assert rows == [{k: r[k] for k in ("request_index", "value", "state", "eligible_rows")} for r in core_ds["rows"]]
    assert ds["rows"] == core_ds["rows"], "entity keys, as_of and cutoff match the core too"
    assert [x["request_index"] for x in ds["excluded"]] == [w["request_index"] for w in want["excluded"]]
    for x, w in zip(ds["excluded"], want["excluded"]):
        assert w["reason_contains"] in x["reason"]
