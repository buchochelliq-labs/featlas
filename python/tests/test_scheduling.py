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
