"""Report consistency checks must not turn declarations into target evidence."""
import copy
import hashlib
import importlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from validation import load_yaml, load_yaml_text
from verify_bundle import verify


class CapabilityReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_path = ROOT / "examples/06_store_migration.yaml"
        cls.original_bytes = cls.project_path.read_bytes()
        cls.base = load_yaml_text(cls.original_bytes.decode("utf-8"))

    def setUp(self):
        self.project = copy.deepcopy(self.base)
        self.project_bytes = self.original_bytes
        self.report = self.make_report()

    def make_report(self, deployment_index=0):
        deployment = self.project["deployments"][deployment_index]
        return {
            "spec_version": "0.2.0", "kind": "AdapterCapabilityReport",
            "project_id": self.project["metadata"]["id"],
            "project_version": self.project["metadata"]["version"],
            "project_file_sha256": hashlib.sha256(self.project_bytes).hexdigest(),
            "deployment": {"id": deployment["id"], "version": deployment["version"]},
            "assessed_at": "2026-11-09T00:00:00Z", "execution_scope": "external_target",
            "adapters": [{"id": a["id"], "implementation": a["implementation"],
                          "version": a["version"], "implementation_sha256": None,
                          "runtime_versions": {}} for a in deployment["adapters"]],
            "checks": [{"adapter": r["adapter"], "role": r["role"], "capability": r["capability"],
                        "status": "unverified", "evidence_scope": "declaration", "evidence": [],
                        "limitations": ["Synthetic test declaration; no adapter has been executed."]}
                       for r in deployment["runtime"]["required_capabilities"]],
            "execution_ready": False,
            "limitations": ["Synthetic unit-test report, not production evidence."],
        }

    def validate(self):
        try:
            validate = importlib.import_module("capability_reports").validate_capability_report
        except (ModuleNotFoundError, AttributeError):
            self.fail("The capability-report validator has not been implemented")
        return validate(self.report, self.project, self.project_bytes)

    def codes(self):
        return {d.code for d in self.validate()}

    def refresh_project_bytes(self):
        self.project_bytes = json.dumps(self.project, ensure_ascii=False).encode("utf-8")
        self.report["project_file_sha256"] = hashlib.sha256(self.project_bytes).hexdigest()

    def supported_records(self, scope="live_target"):
        # Digest/version values here are invented test data, never shipped as target evidence.
        for adapter in self.report["adapters"]:
            adapter["implementation_sha256"] = "1" * 64
            adapter["runtime_versions"] = {"runtime": "18.0", "feature_client": "0.16.0"}
        for check in self.report["checks"]:
            check.update(status="supported_native", evidence_scope=scope,
                         evidence=[{"uri": "urn:test:synthetic-evidence", "sha256": "2" * 64,
                                    "description": "Synthetic record for validator tests only."}])

    def test_truthful_unverified_report_is_valid_but_not_ready(self):
        self.assertEqual(self.validate(), [])
        self.assertFalse(self.report["execution_ready"])

    def test_structure_is_checked_before_project_fields(self):
        self.report.pop("checks")
        self.project = None
        self.project_bytes = b"invalid project"
        self.assertEqual(self.codes(), {"SCHEMA_INVALID"})

    def test_same_project_id_does_not_hide_a_different_file_hash(self):
        self.report["project_file_sha256"] = "0" * 64
        self.assertIn("REPORT_PROJECT_HASH_MISMATCH", self.codes())

    def test_project_id_and_version_are_checked(self):
        for field, wrong in [("project_id", "another_project"), ("project_version", "99.0.0")]:
            with self.subTest(field=field):
                self.report = self.make_report()
                self.report[field] = wrong
                self.assertIn("REPORT_PROJECT_IDENTITY_MISMATCH", self.codes())

    def test_supplied_project_must_match_the_hashed_bytes(self):
        self.project["metadata"]["description"] = "An object different from the hashed document."
        self.assertIn("REPORT_PROJECT_BYTES_MISMATCH", self.codes())

    def test_project_comparison_distinguishes_boolean_and_integer(self):
        self.project["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"][0]["is_fault"] = 1
        self.assertIn("REPORT_PROJECT_BYTES_MISMATCH", self.codes())

    def test_deployment_id_and_version_are_checked(self):
        for field, wrong in [("id", "missing_deployment"), ("version", "99.0.0")]:
            with self.subTest(field=field):
                self.report = self.make_report()
                self.report["deployment"][field] = wrong
                self.assertIn("REPORT_DEPLOYMENT_MISMATCH", self.codes())

    def test_exact_adapter_roster_rejects_missing_and_extra_instances(self):
        self.report = self.make_report(1)
        self.report["adapters"].pop()
        self.assertIn("REPORT_ADAPTER_ROSTER_MISMATCH", self.codes())
        self.report = self.make_report()
        extra = copy.deepcopy(self.report["adapters"][0])
        extra["id"] = "unbound_adapter"
        self.report["adapters"].append(extra)
        self.assertIn("REPORT_ADAPTER_ROSTER_MISMATCH", self.codes())

    def test_duplicate_adapter_assessments_are_rejected(self):
        self.report["adapters"].append(copy.deepcopy(self.report["adapters"][0]))
        self.assertIn("REPORT_DUPLICATE_ADAPTER", self.codes())

    def test_adapter_implementation_and_version_must_match_deployment(self):
        for field, wrong in [("implementation", "org.other:adapter"), ("version", "99.0.0")]:
            with self.subTest(field=field):
                self.report = self.make_report()
                self.report["adapters"][0][field] = wrong
                self.assertIn("REPORT_ADAPTER_IDENTITY_MISMATCH", self.codes())

    def test_check_cannot_name_an_unknown_adapter(self):
        self.report["checks"][0]["adapter"] = "missing_adapter"
        self.assertIn("REPORT_CHECK_ADAPTER_UNKNOWN", self.codes())

    def test_check_cannot_use_an_unbound_adapter_role(self):
        self.report["checks"][0]["role"] = "online_store"
        self.assertIn("REPORT_CHECK_ROLE_MISMATCH", self.codes())

    def test_every_explicit_requirement_needs_a_check(self):
        self.report["checks"].pop()
        self.assertIn("REPORT_REQUIRED_CAPABILITY_MISSING", self.codes())

    def test_duplicate_check_does_not_satisfy_requirement_coverage(self):
        self.report["checks"].append(copy.deepcopy(self.report["checks"][0]))
        self.assertIn("REPORT_DUPLICATE_CHECK", self.codes())

    def test_unrecognized_required_capability_cannot_be_ignored(self):
        self.project["deployments"][0]["runtime"]["required_capabilities"].append({
            "adapter": "databricks_platform", "role": "compute",
            "capability": "new_vendor.previously_unknown", "reason": "Explicit new requirement."})
        self.refresh_project_bytes()
        self.assertIn("REPORT_REQUIRED_CAPABILITY_MISSING", self.codes())

    def test_requirement_coverage_is_scoped_to_adapter_and_role(self):
        self.report = self.make_report(1)
        self.project["deployments"][1]["runtime"]["required_capabilities"].append({
            "adapter": "alternative_store", "role": "offline_store",
            "capability": "core.aggregate", "reason": "Separate scoped requirement."})
        self.refresh_project_bytes()
        self.assertIn("REPORT_REQUIRED_CAPABILITY_MISSING", self.codes())

    def test_supported_claim_requires_digest_pinned_evidence(self):
        self.supported_records()
        self.report["checks"][0]["evidence"] = []
        self.assertIn("REPORT_SUPPORT_EVIDENCE_MISSING", self.codes())

    def test_supported_claim_requires_adapter_implementation_digest(self):
        self.supported_records()
        self.report["adapters"][0]["implementation_sha256"] = None
        self.assertIn("REPORT_IMPLEMENTATION_DIGEST_MISSING", self.codes())

    def test_declaration_scope_cannot_support_a_claim_even_with_digests(self):
        self.supported_records("declaration")
        self.assertIn("REPORT_DECLARATION_CANNOT_SUPPORT", self.codes())

    def test_external_readiness_requires_supported_checks(self):
        for status in ["unverified", "unsupported"]:
            with self.subTest(status=status):
                self.report = self.make_report()
                self.report["checks"][0]["status"] = status
                self.report["execution_ready"] = True
                self.assertIn("REPORT_NOT_EXECUTION_READY", self.codes())

    def test_nonlive_scopes_cannot_establish_external_readiness(self):
        for scope in ["static", "local_reference", "local_mock", "declaration"]:
            with self.subTest(scope=scope):
                self.report = self.make_report()
                self.supported_records(scope)
                self.report["execution_ready"] = True
                self.assertIn("REPORT_LIVE_EVIDENCE_REQUIRED", self.codes())

    def test_external_readiness_requires_runtime_versions_for_every_adapter(self):
        self.report = self.make_report(1)
        self.supported_records()
        self.report["execution_ready"] = True
        self.report["adapters"][1]["runtime_versions"] = {}
        self.assertIn("REPORT_RUNTIME_VERSIONS_REQUIRED", self.codes())

    def test_runtime_aliases_and_placeholders_are_not_observed_version_pins(self):
        for version in ["latest", "unknown", "unverified", ">=18.0", "*", "   "]:
            with self.subTest(version=version):
                self.report = self.make_report()
                self.supported_records()
                self.report["execution_ready"] = True
                self.report["adapters"][0]["runtime_versions"] = {"runtime": version}
                self.assertIn("REPORT_RUNTIME_VERSIONS_REQUIRED", self.codes())

    def test_extra_planner_check_cannot_be_unverified_when_ready(self):
        self.supported_records()
        extra = copy.deepcopy(self.report["checks"][0])
        extra.update(capability="planner.additional_requirement", status="unverified", evidence=[])
        self.report["checks"].append(extra)
        self.report["execution_ready"] = True
        self.assertIn("REPORT_NOT_EXECUTION_READY", self.codes())

    def test_false_readiness_is_allowed_with_supported_checks_and_other_blockers(self):
        self.supported_records()
        self.report["limitations"] = ["Production authorization and budget review remain outstanding."]
        self.assertEqual(self.validate(), [])

    def test_complete_live_scoped_records_pass_record_validation_only(self):
        self.supported_records()
        self.report["execution_ready"] = True
        self.assertEqual(self.validate(), [])

    def test_local_reference_scope_does_not_require_external_runtime_versions(self):
        self.supported_records("local_reference")
        self.report["execution_scope"] = "local_reference"
        self.report["execution_ready"] = True
        self.report["adapters"][0]["runtime_versions"] = {}
        self.assertEqual(self.validate(), [])

    def test_evidence_shape_is_validated_without_reading_or_fetching_its_uri(self):
        self.supported_records()
        self.report["checks"][0]["evidence"][0]["uri"] = "file:///not-a-real-evidence-file/report.json"
        self.assertEqual(self.validate(), [])
        self.report["checks"][0]["evidence"][0]["sha256"] = "not-a-digest"
        self.assertIn("SCHEMA_INVALID", self.codes())

    def test_bundled_example_is_truthful_and_bound_to_actual_project_bytes(self):
        path = ROOT / "examples/reports/databricks_capabilities_unverified.json"
        self.assertTrue(path.is_file(), "The truthful unverified report example is missing")
        self.report = load_yaml(path)
        self.assertEqual(self.validate(), [])
        self.assertFalse(self.report["execution_ready"])
        self.assertEqual({c["status"] for c in self.report["checks"]}, {"unverified"})
        self.assertEqual({c["evidence_scope"] for c in self.report["checks"]}, {"declaration"})
        self.assertTrue(all(a["implementation_sha256"] is None and not a["runtime_versions"]
                            for a in self.report["adapters"]))

    def verify_with_report_paths(self, paths, reports, run_fixtures=False):
        try:
            return verify(paths, run_fixtures=run_fixtures, run_unit_tests=False,
                          capability_report_paths=reports)
        except TypeError as exc:
            if "capability_report_paths" in str(exc):
                self.fail("The bundle verifier does not yet check capability reports")
            raise

    def test_bundle_matching_uses_bytes_hash_with_repeated_project_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            first, second, report_path = folder / "first.yaml", folder / "second.yaml", folder / "report.json"
            first.write_bytes(self.project_bytes + b"\n# A distinct file with the same logical ID.\n")
            second.write_bytes(self.project_bytes)
            report_path.write_text(json.dumps(self.report), encoding="utf-8")
            result = self.verify_with_report_paths([first, second], [report_path])
        self.assertTrue(result["passed"], result["diagnostics"])
        self.assertEqual(result["capability_reports_checked"], 1)
        self.assertTrue(result["capability_reports_consistent"])
        self.assertEqual(result["capability_reports"][0]["project_file"], str(second))

    def test_report_without_matching_project_bytes_fails_full_check(self):
        self.report["project_file_sha256"] = "0" * 64
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"
            report_path.write_text(json.dumps(self.report), encoding="utf-8")
            result = self.verify_with_report_paths([self.project_path], [report_path])
        self.assertFalse(result["passed"])
        self.assertIn("REPORT_PROJECT_NOT_FOUND", {d["code"] for d in result["diagnostics"]})
        self.assertFalse(result["capability_reports_consistent"])

    def test_bundle_report_validation_preserves_fixture_counts(self):
        baseline = verify([self.project_path], run_fixtures=True, run_unit_tests=False)
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"
            report_path.write_text(json.dumps(self.report), encoding="utf-8")
            result = self.verify_with_report_paths([self.project_path], [report_path], run_fixtures=True)
        self.assertTrue(result["passed"], result["diagnostics"])
        for key in ["fixtures_executed", "fixtures_passed", "adapter_cases_not_executed"]:
            self.assertEqual(result[key], baseline[key], key)


if __name__ == "__main__":
    unittest.main()
