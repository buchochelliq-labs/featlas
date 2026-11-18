"""Scheduled deployments: the generated Databricks job and the host's slot identity."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from featlas import Project

ROOT = Path(__file__).resolve().parents[2]


def test_generated_job_is_paused_bounded_and_idempotent():
    g = Project.from_path(ROOT / "projects/01_fault_count.yaml").generate("fault_prod")
    job = json.loads(g["files"]["databricks/job.yml"])
    [(key, spec)] = job["resources"]["jobs"].items()
    assert key == "featlas_fault_prod"
    assert spec["schedule"] == {"quartz_cron_expression": "0 0 0/1 * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}
    assert spec["max_concurrent_runs"] == 1 and spec["timeout_seconds"] == 300
    [task] = spec["tasks"]
    params = task["python_wheel_task"]["parameters"]
    assert task["max_retries"] == 2 and "--publish" in params
    assert params[params.index("--as-of-from-trigger") + 1] == "{{job.trigger.time.iso_datetime}}"
    assert "--publication-id" not in params, "the identity is derived from the slot, so retries are idempotent"
    assert spec["tags"]["featlas_plan_digest"] == g["manifest"]["plan_digest"]


def test_manual_deployments_get_no_job():
    g = Project.from_path(ROOT / "projects/01_fault_count.yaml").generate("fault_prod")
    assert "databricks/job.yml" in g["files"]
    p = Project.from_path(ROOT / "projects/05_lookup_and_recency.yaml")
    assert "databricks/job.yml" not in p.generate("lookup_prod")["files"]


@pytest.mark.parametrize(
    "trigger,grid,offset,want",
    [
        ("2026-06-01T10:37:12.000Z", 3600, 0, "2026-06-01T10:00:00Z"),
        ("2026-06-01T10:00:00Z", 3600, 0, "2026-06-01T10:00:00Z"),
        ("2026-06-01T01:59:59Z", 86400, 7200, "2026-05-31T02:00:00Z"),
        ("2026-06-01T02:00:00Z", 86400, 7200, "2026-06-01T02:00:00Z"),
    ],
)
def test_as_of_is_the_trigger_floored_to_the_schedule_grid(trigger, grid, offset, want):
    pytest.importorskip("pyspark")
    from featlas_databricks.cli import as_of_from_trigger

    assert as_of_from_trigger(trigger, grid, offset) == want


def test_job_bundle_becomes_a_jobs_api_payload_unchanged_but_for_variables():
    from featlas_databricks.jobs import job_settings

    g = Project.from_path(ROOT / "projects/live/platform.yaml").generate("fault_job")
    bundle = json.loads(g["files"]["databricks/job.yml"])
    values = {"project_path": "/Volumes/featlas/x/a/platform.yaml", "featlas_wheel": "/Volumes/featlas/x/a/featlas.whl", "source_status_table": "featlas.x.status"}
    s = job_settings(bundle, values)
    assert s["schedule"] == {"quartz_cron_expression": "0 0/5 * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}
    assert s["environments"] == [{"environment_key": "featlas", "spec": {"client": "3", "dependencies": [values["featlas_wheel"]]}}]
    params = s["tasks"][0]["python_wheel_task"]["parameters"]
    assert params[0] == values["project_path"] and params[params.index("--status-table") + 1] == values["source_status_table"]
    assert s["timeout_seconds"] == 900 and s["max_concurrent_runs"] == 1
    with pytest.raises(ValueError, match="without a value"):
        job_settings(bundle, {"project_path": "p"})
    with pytest.raises(ValueError, match="undeclared"):
        job_settings(bundle, {**values, "other": "x"})


def test_feature_views_mapping_is_exact_or_falls_back_with_reasons():
    import sys

    sys.path.insert(0, str(Path(__file__).parent))
    import fv_project

    p, _ = fv_project.project("featlas.raw.device_events", "featlas.raw.devices", "featlas.features.device_events")
    proj = Project(fv_project.yaml_text(p))
    dep = p["deployments"][0]["id"]
    g = proj.generate(dep)
    fv = g["feature_views"]["device_events"]
    assert "databricks/device_events/feature_views.json" in g["files"]
    by = {f["column"]: f for f in fv["features"]}
    assert {c for c, f in by.items() if f["api"] == "feature_views"} == fv_project.MAPPED, {c: f["reasons"] for c, f in by.items()}
    assert {c for c, f in by.items() if f["api"] == "uc_feature_tables"} == fv_project.UNMAPPED
    assert any("ApproxCountDistinct" in r for r in by["distinct_codes_1h"]["reasons"])
    assert any("recency" in r for r in by["seconds_since_event"]["reasons"])
    d = by["fault_severity_1h"]["definition"]
    assert d["function"] == {"operator": "Sum", "input": "severity"} and "`is_fault`" in d["source"]["filter_condition"]
    assert d["time_window"] == {"type": "rolling", "window_duration_seconds": 3600} and d["entity"] == ["tenant_id", "device_id"]
    assert by["events_1h"]["definition"]["function"] == {"operator": "Count", "input": "event_id"}
    assert by["last_severity_1h"]["definition"]["function"]["operator"] == "Last" and by["last_severity_1h"]["assumptions"]
    # The same features with availability distinct from event time never map.
    g2 = Project.from_path(ROOT / "projects/live/platform.yaml")
    doc = json.loads(json.dumps(__import__("yaml").safe_load((ROOT / "projects/live/platform.yaml").read_text())))
    for d0 in doc["deployments"]:
        if d0["id"] == "fault_prod":
            d0["adapters"][0]["configuration"]["feature_api"] = "feature_views"
    from featlas_reverse.draft import to_yaml

    fv2 = Project(to_yaml(doc)).generate("fault_prod")["feature_views"]["device_reliability"]
    [f] = fv2["features"]
    assert f["api"] == "uc_feature_tables" and any("available" in r and "event time" in r for r in f["reasons"]), f
    assert g2  # the unmodified project still loads
