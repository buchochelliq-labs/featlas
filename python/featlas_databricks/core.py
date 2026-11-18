"""In-process boundary to the authoritative Rust core via the ``featlas`` bindings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from featlas import FeatlasError, Project


class CoreError(RuntimeError):
    def __init__(self, message: str, code: str = "", detail: Any = None):
        super().__init__(message)
        self.code = code
        self.detail = detail


class Core:
    """Path-oriented facade used by the host; every result comes from the Rust core."""

    def __init__(self):
        self._projects: dict[str, Project] = {}

    def project(self, path: str) -> Project:
        key = str(Path(path).resolve())
        if key not in self._projects:
            try:
                self._projects[key] = Project.from_path(path)
            except FeatlasError as e:
                raise CoreError(str(e), e.code, e.detail) from None
        return self._projects[key]

    def generate(self, project: str, deployment: str, out_dir: str | None = None) -> dict:
        try:
            gen = self.project(project).generate(deployment)
        except FeatlasError as e:
            raise CoreError(str(e), e.code, e.detail) from None
        if out_dir:
            root = Path(out_dir)
            for rel, content in gen["files"].items():
                target = root / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8")
            (root / "manifest.json").write_text(json.dumps(gen["manifest"], indent=2), encoding="utf-8")
        return {"artifacts": gen["artifacts"], "label_artifacts": gen.get("label_artifacts", {}), "feature_views": gen.get("feature_views", {}), "manifest": gen["manifest"], "plan": gen["plan"], "capability_report": gen["capability_report"], "files": gen["files"]}

    def coverage(self, project: str, deployment: str, feature_set: str, as_of: str, statuses: list[dict]) -> dict:
        return self.project(project).coverage(deployment, feature_set, as_of, statuses)

    def consume(self, project: str, request: dict, records: list[dict] | None) -> dict:
        try:
            return {"response": self.project(project).consume(request, records)}
        except FeatlasError as e:
            return {"error": {"code": e.code, "message": e.message, "detail": e.detail}}

    def labels(self, project: str, consumer: str, label_input: dict) -> dict:
        return self.project(project).labels(consumer, label_input)

    def label_requests(self, project: str, consumer: str, rows: list[dict], source_status: dict) -> dict:
        return self.project(project).label_requests(consumer, rows, source_status)

    def evaluate_case(self, project: str, case_id: str) -> dict:
        return self.project(project).evaluate_case(case_id)
