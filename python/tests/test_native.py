"""Tests of the PyO3 bindings: same results as the Rust CLI, typed errors, no side effects."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

import featlas
from featlas import FeatlasError, Project

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "spec/feature-engineering-spec-v0.2.0/examples"


def fe_spec() -> str | None:
    for candidate in (ROOT / "target/release/fe-spec", ROOT / "target/debug/fe-spec"):
        if candidate.exists():
            return str(candidate)
    return shutil.which("fe-spec")


def test_module_metadata():
    assert featlas.SPEC_VERSION == "0.2.0"
    assert featlas.API_SCHEMA == "featlas.api/v1"
    assert featlas.CORE_VERSION == "0.1.0"


@pytest.mark.parametrize("name,passed,not_executed", [
    ("00_minimal_template.yaml", 1, 0),
    ("01_fault_count.yaml", 13, 0),
    ("02_historical_temperature.yaml", 9, 0),
    ("03_commerce_ratio.yaml", 6, 0),
    ("04_fitted_and_custom_contracts.yaml", 3, 3),
    ("05_lookup_and_recency.yaml", 5, 0),
    ("06_store_migration.yaml", 13, 0),
])
def test_reference_goldens_through_bindings(name, passed, not_executed):
    s = Project.from_path(EXAMPLES / name).run_fixtures()["summary"]
    assert (s["passed"], s["failed"], s["not_executed"]) == (passed, 0, not_executed)


def test_invalid_document_raises_typed_error_with_location():
    text = (EXAMPLES / "00_minimal_template.yaml").read_text().replace("kind: FeatureEngineeringProject", "kind: FeatureEngineeringProject\nkind: x", 1)
    report = featlas.validate(text)
    assert report["passed"] is False
    d = report["report"]["diagnostics"][0]
    assert (d["code"], d["rule"], d["location"]["line"]) == ("PARSE_DUPLICATE_KEY", "SEM-PARSE-001", 5)
    with pytest.raises(FeatlasError) as e:
        Project(text)
    assert e.value.code == "DOCUMENT_INVALID"


def test_file_digest_is_exact_bytes():
    import hashlib

    path = EXAMPLES / "01_fault_count.yaml"
    assert Project.from_path(path).file_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_logical_identities_survive_rebinding():
    a = Project.from_path(EXAMPLES / "01_fault_count.yaml").logical_identities
    b = Project.from_path(ROOT / "projects/06_store_migration.yaml").logical_identities
    assert a["features"] == b["features"] and a["logical_project"] == b["logical_project"]


def test_generate_is_in_memory_and_rejects_synthetic_pins(tmp_path):
    with pytest.raises(FeatlasError) as e:
        Project.from_path(EXAMPLES / "01_fault_count.yaml").generate("fault_prod")
    assert e.value.code == "PLANNING_FAILED"
    assert any(d["code"] == "UNRESOLVED_ADAPTER" for d in e.value.detail)
    gen = Project.from_path(ROOT / "projects/01_fault_count.yaml").generate("fault_prod", "2026-11-09T00:00:00Z")
    assert "databricks/device_reliability/script.sql" in gen["files"]
    assert gen["manifest"]["execution_ready"] is False
    assert list(tmp_path.iterdir()) == []


def test_capabilities_and_labels():
    report = Project.from_path(ROOT / "projects/01_fault_count.yaml").capabilities("fault_prod", "2026-11-09T00:00:00Z")
    # Supported only with live evidence from the adapter lock; scheduling is never observed, so not ready.
    assert report["execution_ready"] is False
    for c in report["checks"]:
        assert c["status"] == "unverified" or (c["status"] == "supported_native" and c["evidence_scope"] == "live_target" and c["evidence"]), c
    assert next(c for c in report["checks"] if c["capability"] == "core.scheduling")["status"] == "unverified"
    p = Project.from_path(EXAMPLES / "03_commerce_ratio.yaml")
    row = {"tenant_id": "tenant_a", "customer_id": "customer_1", "as_of": "2026-07-01T00:00:00Z", "requested_at": "2026-07-09T00:00:00Z", "dataset_cutoff": "2026-07-09T00:00:00Z"}
    ds = p.labels("spend_training_dataset", {"rows": [row], "source_rows": [], "source_status": {"source": "purchase_events", "state": "available", "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2026-07-09T00:00:00Z"}})
    assert ds["rows"][0]["value"] == 0 and ds["rows"][0]["state"] == "empty"


def test_restricted_json_inputs():
    p = Project.from_path(EXAMPLES / "01_fault_count.yaml")
    with pytest.raises(FeatlasError) as e:
        featlas._call(featlas._native.coverage, p.document, "yaml", "fault_dev", "device_reliability", "2026-06-08T00:00:00Z", '[{"source":"a","source":"b","state":"unavailable"}]')
    assert e.value.code == "JSON_INVALID"


@pytest.mark.skipif(fe_spec() is None, reason="fe-spec binary not built")
def test_transport_parity_with_cli():
    path = EXAMPLES / "02_historical_temperature.yaml"
    cli = json.loads(subprocess.run([fe_spec(), "test", str(path), "--format", "json"], capture_output=True, text=True, check=True).stdout)
    native = Project.from_path(path).run_fixtures()
    strip = lambda s: [(c["case"], c["status"], c.get("actual_rows"), c.get("actual_diagnostic")) for c in s["summary"]["cases"]]  # noqa: E731
    assert strip(cli) == strip(native)
    cli_hash = json.loads(subprocess.run([fe_spec(), "hash", str(path)], capture_output=True, text=True, check=True).stdout)
    assert cli_hash["logical"] == Project.from_path(path).hash()["logical"]
