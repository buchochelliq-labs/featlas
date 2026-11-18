"""Deploy the generated ``databricks/job.yml`` (a Databricks Asset Bundle job resource) through the
Jobs API, without the Databricks CLI.

``job_settings(bundle, variables)`` substitutes the bundle's ``${var.<name>}`` references and
returns the Jobs API 2.2 ``create`` payload for its single job, unchanged otherwise: schedule
(still PAUSED), tasks, retries, environment, concurrency, timeout and tags all come from the
generated plan. Deploying (creating) and unpausing are separate, explicit operations: unpausing
is the publication approval for the deployment's environment (DECISIONS D14).
"""

from __future__ import annotations

import json
import re
from typing import Any

_VAR = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")


def job_settings(bundle: dict, variables: dict[str, str]) -> dict:
    declared = set(bundle.get("variables", {}))
    missing = declared - set(variables)
    if missing:
        raise ValueError(f"bundle variables without a value: {sorted(missing)}")
    unknown = set(variables) - declared
    if unknown:
        raise ValueError(f"values for undeclared bundle variables: {sorted(unknown)}")
    jobs = bundle["resources"]["jobs"]
    if len(jobs) != 1:
        raise ValueError(f"expected one job resource, got {len(jobs)}")
    (settings,) = jobs.values()

    def sub(x: Any) -> Any:
        if isinstance(x, str):
            return _VAR.sub(lambda m: variables[m.group(1)], x)
        if isinstance(x, list):
            return [sub(v) for v in x]
        if isinstance(x, dict):
            return {k: sub(v) for k, v in x.items()}
        return x

    out = sub(settings)
    left = _VAR.findall(json.dumps(out))
    if left:
        raise ValueError(f"unresolved bundle variables {left}")
    return out


def create(workspace, settings: dict) -> int:
    """Create the job (resource creation; needs the ``jobs`` OAuth scope). Returns its id."""
    return int(workspace.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"])


def set_pause(workspace, job_id: int, schedule: dict, paused: bool) -> None:
    sched = dict(schedule, pause_status="PAUSED" if paused else "UNPAUSED")
    workspace.api_client.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {"schedule": sched}})


def run_now(workspace, job_id: int) -> int:
    return int(workspace.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id})["run_id"])


def get_run(workspace, run_id: int) -> dict:
    return workspace.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})


def list_runs(workspace, job_id: int) -> list[dict]:
    return workspace.api_client.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "limit": 25}).get("runs", [])


def run_output(workspace, task_run_id: int) -> dict:
    return workspace.api_client.do("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": task_run_id})


def delete(workspace, job_id: int) -> None:
    workspace.api_client.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
