#!/usr/bin/env python3
"""Validate this specification bundle; no cloud calls or artifact execution."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path

from validation import Diagnostic, DocumentError, load_yaml, load_yaml_text, schema_registry, structural_diagnostics, semantic_diagnostics
from fixture_runtime import EvaluationError, evaluate_case, compare_rows
from capability_reports import report_structural_diagnostics, validate_capability_report

ROOT = Path(__file__).resolve().parents[1]


def verify_local_artifact(artifact):
    """Verify a bundle:// artifact without importing it or resolving remote URIs."""
    uri = artifact["uri"]
    if not uri.startswith("bundle://"):
        return None
    candidate = (ROOT / uri[len("bundle://"):]).resolve()
    if not candidate.is_relative_to(ROOT):
        raise DocumentError("Artifact reference escapes bundle")
    if not candidate.is_file():
        raise DocumentError(f"Missing local artifact: {uri}")
    if hashlib.sha256(candidate.read_bytes()).hexdigest() != artifact["sha256"]:
        raise DocumentError(f"Artifact digest mismatch: {uri}")
    return candidate


def verify(paths, run_fixtures=True, run_unit_tests=True, capability_report_paths=()):
    schema, registry, count = schema_registry(ROOT / "schemas")
    report = {
        "spec_version": "0.2.0",
        "scope": "Local specification conformance kit, not a production or Databricks execution certification",
        "schemas_checked": count,
        "projects_checked": 0,
        "fixtures_executed": 0,
        "fixtures_passed": 0,
        "adapter_cases_not_executed": 0,
        "artifact_digests_checked": 0,
        "capability_reports_checked": 0,
        "capability_reports_consistent": True,
        "capability_reports": [],
        "unit_tests_run": 0,
        "unit_test_failures": 0,
        "diagnostics": [],
        "projects": [],
        "limitations": [
            "Learned/custom artifacts are hashed but never executed; adapter-required goldens are not passes.",
            "No live Databricks queries, publication, cost/SLO measurements, or access-policy checks were performed.",
            "Semantic checks cover the local rules implemented in validation.py; full normative production conformance also requires SEMANTIC_RULES.md.",
            "Warning/quarantine output artifacts are not implemented; a triggered such quality gate is UNSUPPORTED_CAPABILITY.",
            "Incremental engines, label generation and fitted-transform training are specifications for the future implementation.",
            "Capability reports are checked for record consistency and explicit requirement coverage only. Report execution_ready remains a producer claim; planner-derived completeness, evidence authenticity, implementation digests, and runtime observations are not independently established. Evidence references are never fetched.",
        ],
    }
    checked_artifacts = set()
    projects_by_hash = {}
    for path in paths:
        entry = {"file": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path), "schema_valid": False,
                 "semantic_valid": False, "fixtures": []}
        report["projects"].append(entry)
        report["projects_checked"] += 1
        try:
            project_file_bytes = path.read_bytes()
            project = load_yaml_text(project_file_bytes.decode("utf-8"))
            diagnostics = structural_diagnostics(project, schema, registry)
            entry["schema_valid"] = not diagnostics
            if not diagnostics:
                diagnostics = semantic_diagnostics(project)
                entry["semantic_valid"] = not diagnostics
            report["diagnostics"].extend({"file": entry["file"], **d.to_dict()} for d in diagnostics)
            if diagnostics:
                continue
            projects_by_hash.setdefault(hashlib.sha256(project_file_bytes).hexdigest(), []).append(
                (entry["file"], project, project_file_bytes))
            # Only locally bundled fixtures are read. URI references are never fetched.
            for feature in project["features"]:
                transform = feature["transform"]
                artifact = transform.get("artifact") or transform.get("implementation")
                if artifact is None:
                    continue
                resolved = verify_local_artifact(artifact)
                if resolved is not None and resolved not in checked_artifacts:
                    checked_artifacts.add(resolved)
                    report["artifact_digests_checked"] += 1
            if not run_fixtures:
                continue
            features = {f["id"]: f for f in project["features"]}
            for suite in project["acceptance_suites"]:
                for case in suite["cases"]:
                    result = {"suite": suite["id"], "case": case["id"]}
                    entry["fixtures"].append(result)
                    if suite["runner"] == "adapter_required":
                        result["outcome"] = "not_executed_adapter_required"
                        report["adapter_cases_not_executed"] += 1
                        continue
                    report["fixtures_executed"] += 1
                    expected = case["expected"]
                    try:
                        actual = evaluate_case(project, case)
                        if "diagnostic" in expected:
                            ok, message = False, f"Expected diagnostic {expected['diagnostic']['code']}; calculation succeeded"
                        else:
                            ok, message = compare_rows(features[case["feature"]["id"]], actual, expected["rows"])
                    except EvaluationError as exc:
                        wanted = expected.get("diagnostic", {})
                        ok = wanted.get("code") == exc.code and wanted.get("message_contains", "") in str(exc)
                        message = "" if ok else f"Unexpected {exc.code}: {exc}"
                    result["outcome"] = "passed" if ok else "failed"
                    if ok:
                        report["fixtures_passed"] += 1
                    else:
                        result["message"] = message
                        report["diagnostics"].append({"file": entry["file"], "code": "FIXTURE_FAILED", "path": case["id"], "message": message})
        except Exception as exc:
            report["diagnostics"].append({"file": entry["file"], "code": "VERIFICATION_ERROR", "path": "/", "message": f"{type(exc).__name__}: {exc}"})
    for path in capability_report_paths:
        path = Path(path).resolve()
        entry = {"file": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
                 "consistent": False}
        report["capability_reports"].append(entry)
        report["capability_reports_checked"] += 1
        try:
            assessment = load_yaml(path)
            diagnostics = report_structural_diagnostics(assessment)
            if not diagnostics:
                entry["claimed_execution_ready"] = assessment["execution_ready"]
                matches = projects_by_hash.get(assessment["project_file_sha256"], [])
                if not matches:
                    diagnostics = [Diagnostic("REPORT_PROJECT_NOT_FOUND", "/project_file_sha256",
                                              "No validated project with these exact file bytes was included")]
                else:
                    # Identical bytes are interchangeable; project IDs alone are not.
                    project_file, project, project_file_bytes = matches[0]
                    entry["project_file"] = project_file
                    diagnostics = validate_capability_report(assessment, project, project_file_bytes)
            entry["consistent"] = not diagnostics
            report["diagnostics"].extend({"file": entry["file"], **d.to_dict()} for d in diagnostics)
        except Exception as exc:
            report["diagnostics"].append({"file": entry["file"], "code": "REPORT_VERIFICATION_ERROR", "path": "/", "message": f"{type(exc).__name__}: {exc}"})
    report["capability_reports_consistent"] = all(entry["consistent"] for entry in report["capability_reports"])
    if run_unit_tests:
        suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_*.py")
        output = io.StringIO()
        result = unittest.TextTestRunner(stream=output, verbosity=1).run(suite)
        report["unit_tests_run"] = result.testsRun
        report["unit_test_failures"] = len(result.failures) + len(result.errors)
        if not result.wasSuccessful():
            report["diagnostics"].append({"code": "UNIT_TEST_FAILED", "path": "tests", "message": output.getvalue()})
    report["passed"] = not report["diagnostics"]
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, help="Validate one project instead of all examples")
    parser.add_argument("--fixtures", action="store_true", help="Also execute reference fixtures for --project; default bundle mode already executes them")
    parser.add_argument("--json", action="store_true", help="Print a structured verification report")
    args = parser.parse_args(argv)
    paths = [args.project.resolve()] if args.project else sorted((ROOT / "examples").glob("*.yaml"))
    if not paths:
        parser.error("No YAML examples found")
    capability_paths = sorted((ROOT / "examples/reports").glob("*.json")) if args.project is None else []
    report = verify(paths, run_fixtures=args.fixtures or args.project is None, run_unit_tests=args.project is None,
                    capability_report_paths=capability_paths)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("PASS" if report["passed"] else "FAIL")
        print(f"Schemas: {report['schemas_checked']}; projects: {report['projects_checked']}")
        print(f"Reference fixtures: {report['fixtures_passed']}/{report['fixtures_executed']}; adapter-required cases not executed: {report['adapter_cases_not_executed']}")
        print(f"Unit/negative tests: {report['unit_tests_run']}; failures: {report['unit_test_failures']}; artifact digests: {report['artifact_digests_checked']}")
        print(f"Capability reports: {report['capability_reports_checked']} checked; records consistent: {report['capability_reports_consistent']}")
        for item in report["diagnostics"]:
            print(f"{item['code']} {item.get('file', '')} {item['path']}: {item['message']}")
        print("Scope: local specification verification only; no production/cloud execution.")
        print("Report limits: readiness remains a producer claim; no planner-completeness, evidence-authenticity, or runtime certification; evidence references are not fetched.")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
