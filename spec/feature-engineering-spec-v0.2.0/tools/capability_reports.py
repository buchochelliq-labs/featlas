"""Bounded report self-consistency checks, never adapter certification.

Evidence URIs are not opened. Digests and runtime versions are producer claims;
this module checks their presence/shape and consistency, not their authenticity.
It covers explicit requirements and every additional check listed by the producer,
but cannot discover requirements omitted by a production planner.
"""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

from validation import Diagnostic, DocumentError, load_yaml_text, schema_registry, structural_diagnostics

ROOT = Path(__file__).resolve().parents[1]
MAX_REPORT_BYTES = 4 * 1024 * 1024
SUPPORTED = frozenset({"supported_native", "supported_emulated"})


@lru_cache(maxsize=1)
def _schemas():
    project_schema, registry, _ = schema_registry(ROOT / "schemas")
    report_schema = json.loads((ROOT / "schemas/capability-report.schema.json").read_text(encoding="utf-8"))
    return project_schema, report_schema, registry


def _json_value(value):
    # Type-aware comparison: Python equality alone considers True equal to 1.
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def report_structural_diagnostics(report) -> list[Diagnostic]:
    """Validate report shape using only shipped schemas, before reading its fields."""
    try:
        if len(_json_value(report).encode("utf-8")) > MAX_REPORT_BYTES:
            return [Diagnostic("REPORT_DOCUMENT_INVALID", "/", "Capability report exceeds the 4 MiB limit")]
        _, report_schema, registry = _schemas()
        return structural_diagnostics(report, report_schema, registry)
    except (TypeError, ValueError, RecursionError) as exc:
        return [Diagnostic("REPORT_DOCUMENT_INVALID", "/", f"Report is not bounded JSON data: {exc}")]


def _has_runtime_pins(versions):
    """Reject empty records and obvious aliases, without assuming provider SemVer."""
    placeholders = {"latest", "unknown", "unverified", "unobserved", "not_observed",
                    "not observed", "not installed", "none", "n/a", "auto", "placeholder", "example"}
    return bool(versions) and all(
        name.strip() and value.strip() and value.strip().lower() not in placeholders
        and not value.strip().startswith(("<", ">", "=", "~", "^")) and "*" not in value
        for name, value in versions.items()
    )


def validate_capability_report(report, project, project_file_bytes) -> list[Diagnostic]:
    """Check a report against one exact project file and deployment.

    A successful result means the records are consistent, not that capabilities,
    runtime observations, or execution_ready have been independently established.
    All report checks are treated as producer-listed requirements for readiness;
    this includes additional planner-derived checks beyond explicit requirements.
    False execution_ready remains valid when supported checks have other blockers.

    Callers must separately validate project semantics; this helper checks project
    structure and the report relationship. The bundle verifier runs both project
    structural and semantic validation before passing a project to this helper.
    """
    diagnostics = report_structural_diagnostics(report)
    if diagnostics:
        return diagnostics

    def add(code, path, message):
        diagnostics.append(Diagnostic(code, path, message))

    if not isinstance(project_file_bytes, bytes):
        add("REPORT_PROJECT_INVALID", "/project_file_sha256", "Exact project file bytes are required")
        return diagnostics
    if hashlib.sha256(project_file_bytes).hexdigest() != report["project_file_sha256"]:
        add("REPORT_PROJECT_HASH_MISMATCH", "/project_file_sha256", "Report does not identify these exact project bytes")
    try:
        parsed = load_yaml_text(project_file_bytes.decode("utf-8"))
        if _json_value(project) != _json_value(parsed):
            add("REPORT_PROJECT_BYTES_MISMATCH", "/project_file_sha256", "Supplied project differs from the hashed file contents")
            return diagnostics
    except (DocumentError, UnicodeError, TypeError, ValueError, RecursionError) as exc:
        add("REPORT_PROJECT_INVALID", "/project_file_sha256", f"Project bytes cannot be validated: {exc}")
        return diagnostics

    project_schema, _, registry = _schemas()
    project_errors = structural_diagnostics(project, project_schema, registry)
    if project_errors:
        diagnostics.extend(Diagnostic("REPORT_PROJECT_INVALID", "/project" + d.path, d.message)
                           for d in project_errors)
        return diagnostics
    metadata = project["metadata"]
    if (report["project_id"], report["project_version"]) != (metadata["id"], metadata["version"]):
        add("REPORT_PROJECT_IDENTITY_MISMATCH", "/project_id", "Report project ID/version differs from the exact project")
    target = report["deployment"]
    matches = [d for d in project["deployments"] if (d["id"], d["version"]) == (target["id"], target["version"])]
    if len(matches) != 1:
        add("REPORT_DEPLOYMENT_MISMATCH", "/deployment", "Report must select exactly one deployment ID/version")
        return diagnostics
    deployment = matches[0]
    expected_adapters = {a["id"]: a for a in deployment["adapters"]}
    if len(expected_adapters) != len(deployment["adapters"]):
        add("REPORT_PROJECT_INVALID", "/deployment/adapters", "Deployment adapter IDs are ambiguous")
        return diagnostics

    assessments = {}
    for index, adapter in enumerate(report["adapters"]):
        path = f"/adapters/{index}"
        if adapter["id"] in assessments:
            add("REPORT_DUPLICATE_ADAPTER", path + "/id", "Adapter instance is assessed more than once")
        assessments.setdefault(adapter["id"], adapter)
        expected = expected_adapters.get(adapter["id"])
        if expected and (adapter["implementation"], adapter["version"]) != (expected["implementation"], expected["version"]):
            add("REPORT_ADAPTER_IDENTITY_MISMATCH", path, "Adapter implementation/version differs from the deployment pin")
    if assessments.keys() != expected_adapters.keys():
        add("REPORT_ADAPTER_ROSTER_MISMATCH", "/adapters",
            f"Adapter roster differs: missing={sorted(expected_adapters.keys() - assessments.keys())}; "
            f"extra={sorted(assessments.keys() - expected_adapters.keys())}")

    checks = {}
    for index, check in enumerate(report["checks"]):
        path = f"/checks/{index}"
        key = (check["adapter"], check["role"], check["capability"])
        if key in checks:
            add("REPORT_DUPLICATE_CHECK", path, "Adapter/role/capability is assessed more than once")
        checks.setdefault(key, check)
        adapter = expected_adapters.get(check["adapter"])
        if adapter is None:
            add("REPORT_CHECK_ADAPTER_UNKNOWN", path + "/adapter", "Check names no deployment adapter")
        elif check["role"] not in adapter["roles"]:
            add("REPORT_CHECK_ROLE_MISMATCH", path + "/role", "Check role is not bound to this adapter")
        if check["status"] in SUPPORTED:
            if not check["evidence"]:
                add("REPORT_SUPPORT_EVIDENCE_MISSING", path + "/evidence", "Supported claims require digest-pinned evidence references")
            if not assessments.get(check["adapter"], {}).get("implementation_sha256"):
                add("REPORT_IMPLEMENTATION_DIGEST_MISSING", path + "/adapter", "Supported claims require the exact implementation digest")
            if check["evidence_scope"] == "declaration":
                add("REPORT_DECLARATION_CANNOT_SUPPORT", path + "/evidence_scope", "Input declarations do not establish support")

    required = {(r["adapter"], r["role"], r["capability"])
                for r in deployment["runtime"]["required_capabilities"]}
    missing = required - checks.keys()
    for adapter, role, capability in sorted(missing):
        add("REPORT_REQUIRED_CAPABILITY_MISSING", "/checks", f"Missing required assessment: {adapter}/{role}/{capability}")

    if report["execution_ready"]:
        if missing or any(c["status"] not in SUPPORTED for c in report["checks"]):
            add("REPORT_NOT_EXECUTION_READY", "/execution_ready", "Every explicit and producer-listed required check must be supported")
        if report["execution_scope"] == "external_target":
            for index, check in enumerate(report["checks"]):
                if check["evidence_scope"] != "live_target":
                    add("REPORT_LIVE_EVIDENCE_REQUIRED", f"/checks/{index}/evidence_scope", "External readiness requires live-target evidence for every required check")
            # Every declared adapter is assessed, including currently unused instances.
            for index, adapter in enumerate(report["adapters"]):
                if not _has_runtime_pins(adapter["runtime_versions"]):
                    add("REPORT_RUNTIME_VERSIONS_REQUIRED", f"/adapters/{index}/runtime_versions", "External readiness requires nonempty observed version pins, not placeholders or mutable aliases")
    return diagnostics
