"""featlas: Python bindings to the featlas Rust core (feature engineering specification 0.2.0).

All semantics live in Rust (``featlas._native``); this module only converts
between Python objects and the core's JSON. Documents are passed as exact
bytes so the project-file digest matches the CLI's. Nothing here reads files
named inside documents, fetches URIs, deploys or publishes.

    >>> from featlas import Project
    >>> p = Project.from_path("spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml")
    >>> p.run_fixtures()["summary"]["passed"]
    13
"""

from __future__ import annotations

import json
from os import PathLike
from pathlib import Path
from typing import Any

from . import _native

__all__ = ["Project", "FeatlasError", "validate", "CORE_VERSION", "API_SCHEMA", "SPEC_VERSION"]
__version__ = "0.1.0"

CORE_VERSION: str = _native.CORE_VERSION
API_SCHEMA: str = _native.API_SCHEMA
SPEC_VERSION: str = _native.SPEC_VERSION


class FeatlasError(Exception):
    """A core failure with a stable ``code``, a ``message`` and structured ``detail``."""

    def __init__(self, code: str, message: str, detail: Any = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.detail = detail


def _call(fn, *args) -> Any:
    try:
        return json.loads(fn(*args))
    except _native.FeatlasError as exc:
        code, message, detail = exc.args
        raise FeatlasError(code, message, json.loads(detail)) from None


def _fmt(path: str | PathLike[str]) -> str:
    return "json" if str(path).lower().endswith(".json") else "yaml"


def validate(document: bytes | str, format: str = "yaml") -> dict:
    """Staged validation report; never raises for an invalid document."""
    data = document.encode("utf-8") if isinstance(document, str) else bytes(document)
    return _call(_native.validate, data, format)


class Project:
    """A validated project document (parse, schema and reference/type stages passed)."""

    def __init__(self, document: bytes | str, format: str = "yaml"):
        self.document = document.encode("utf-8") if isinstance(document, str) else bytes(document)
        self.format = format
        report = _call(_native.validate, self.document, format)
        self.validation = report
        if not report["passed"]:
            raise FeatlasError("DOCUMENT_INVALID", "The project did not pass parse, schema and reference validation", report["report"])
        self.file_sha256: str = report["report"]["file_sha256"]
        self.logical_identities: dict = report["logical_identities"]

    @classmethod
    def from_path(cls, path: str | PathLike[str]) -> "Project":
        return cls(Path(path).read_bytes(), _fmt(path))

    def _c(self, fn, *args) -> Any:
        return _call(fn, self.document, self.format, *args)

    def hash(self) -> dict:
        return self._c(_native.hash)

    def explain(self, feature: str) -> dict:
        return self._c(_native.explain, feature)

    def plan(self, deployment: str) -> dict:
        return self._c(_native.plan, deployment)

    def capabilities(self, deployment: str, assessed_at: str | None = None) -> dict:
        return self._c(_native.capabilities, deployment, assessed_at)

    def check_report(self, report: dict) -> dict:
        return _call(_native.check_report, json.dumps(report), self.document, self.format)

    def run_fixtures(self, suite: str | None = None) -> dict:
        return self._c(_native.run_fixtures, suite)

    def evaluate_case(self, case_id: str) -> dict:
        return self._c(_native.evaluate_case, case_id)["result"]

    def coverage(self, deployment: str, feature_set: str, as_of: str, statuses: list[dict]) -> dict:
        return self._c(_native.coverage, deployment, feature_set, as_of, json.dumps(statuses))["features"]

    def generate(self, deployment: str, generated_at: str | None = None) -> dict:
        """Plan, capability report and adapter code in memory; ``files`` maps paths to contents."""
        return self._c(_native.generate, deployment, generated_at)

    def consume(self, request: dict, records: list[dict] | None = None) -> dict:
        return self._c(_native.consume, json.dumps(request), None if records is None else json.dumps(records))["response"]

    def migration_plan(self, from_deployment: str, to_deployment: str) -> dict:
        return self._c(_native.migration_plan, from_deployment, to_deployment)

    def labels(self, consumer: str, label_input: dict) -> dict:
        return self._c(_native.labels, consumer, json.dumps(label_input))["dataset"]

    def label_requests(self, consumer: str, rows: list[dict], source_status: dict) -> dict:
        """Eligible and excluded label requests (maturity and completeness; no label data)."""
        return self._c(_native.label_requests, consumer, json.dumps({"rows": rows, "source_status": source_status}))["requests"]
