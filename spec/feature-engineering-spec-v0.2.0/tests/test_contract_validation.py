"""Negative contracts prove that plausible but inconsistent projects are rejected."""
import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from validation import load_yaml, schema_registry, structural_diagnostics, semantic_diagnostics
from fixture_runtime import evaluate_case, EvaluationError


class ContractValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema, cls.registry, _ = schema_registry(ROOT / "schemas")
        cls.base = load_yaml(ROOT / "examples/01_fault_count.yaml")

    def setUp(self):
        self.project = copy.deepcopy(self.base)

    def codes(self):
        structural = structural_diagnostics(self.project, self.schema, self.registry)
        return {d.code for d in structural or semantic_diagnostics(self.project)}

    def test_valid_contract_passes(self):
        self.assertEqual(self.codes(), set())

    def test_unknown_core_property_fails(self):
        self.project["features"][0]["descripton"] = "Misspelled field"
        self.assertIn("SCHEMA_INVALID", self.codes())

    def test_wrong_structural_type_fails(self):
        self.project["features"][0]["time"]["window"]["lookback_seconds"] = "seven days"
        self.assertIn("SCHEMA_INVALID", self.codes())

    def test_unknown_source_fails(self):
        self.project["features"][0]["inputs"][0]["source"] = "missing_source"
        self.assertIn("UNKNOWN_SOURCE", self.codes())

    def test_unknown_column_fails(self):
        self.project["features"][0]["inputs"][0]["columns"].append("missing_column")
        self.assertIn("UNKNOWN_COLUMN", self.codes())

    def test_duplicate_feature_identity_fails(self):
        self.project["features"].append(copy.deepcopy(self.project["features"][0]))
        self.assertIn("DUPLICATE_ID", self.codes())

    def test_missing_column_lineage_fails(self):
        self.project["features"][0]["lineage"]["sources"] = self.project["features"][0]["lineage"]["sources"][1:]
        self.assertIn("MISSING_LINEAGE", self.codes())

    def test_dependency_cycle_fails(self):
        feature = self.project["features"][0]
        feature["dependencies"] = [{"feature": feature["id"], "version": feature["version"], "alignment": "same_entity_as_of"}]
        self.assertIn("DEPENDENCY_CYCLE", self.codes())

    def test_feature_version_pin_fails(self):
        self.project["feature_sets"][0]["features"][0]["version"] = "9.0.0"
        self.assertIn("VERSION_MISMATCH", self.codes())

    def test_fixture_wrong_column_type_fails(self):
        case = self.project["acceptance_suites"][0]["cases"][0]
        case["source_rows"]["fault_events"][0]["is_fault"] = "true"
        self.assertIn("FIXTURE_TYPE_MISMATCH", self.codes())

    def test_fixture_misspelled_column_fails(self):
        case = self.project["acceptance_suites"][0]["cases"][0]
        case["source_rows"]["fault_events"][0]["ocurred_at"] = "2026-06-01T00:00:00Z"
        self.assertIn("FIXTURE_COLUMNS", self.codes())

    def test_invalid_calendar_timestamp_fails(self):
        self.project["features"][0]["history"]["earliest_as_of"] = "2026-02-30T00:00:00Z"
        self.assertIn("SCHEMA_INVALID", self.codes())

    def test_missingness_and_output_nullability_conflict(self):
        self.project["features"][0]["output"]["nullable"] = False
        self.assertIn("NULLABILITY_CONFLICT", self.codes())

    def test_population_must_be_reconstructed(self):
        case = copy.deepcopy(self.project["acceptance_suites"][0]["cases"][0])
        case["population_rows"][0]["device_id"] = "invented_device"
        with self.assertRaises(EvaluationError) as caught:
            evaluate_case(self.project, case)
        self.assertEqual(caught.exception.code, "POPULATION_MISMATCH")

    def test_omitted_completeness_evidence_is_error(self):
        case = copy.deepcopy(self.project["acceptance_suites"][0]["cases"][0])
        case["context"]["source_status"] = []
        with self.assertRaises(EvaluationError) as caught:
            evaluate_case(self.project, case)
        self.assertEqual(caught.exception.code, "MISSING_SOURCE_STATUS")

    def test_as_of_join_missing_tenant_key_fails(self):
        self.project = load_yaml(ROOT / "examples/02_historical_temperature.yaml")
        relation = self.project["relationships"][0]
        relation["from"]["columns"] = [c for c in relation["from"]["columns"] if c != "tenant_id"]
        relation["to"]["columns"] = [c for c in relation["to"]["columns"] if c != "tenant_id"]
        self.assertIn("TENANT_JOIN_MISSING", self.codes())

    def test_relationship_version_pin_fails(self):
        self.project = load_yaml(ROOT / "examples/02_historical_temperature.yaml")
        self.project["features"][0]["transform"]["joins"][0]["version"] = "9.0.0"
        self.assertIn("VERSION_MISMATCH", self.codes())

    def test_duplicate_entity_mapping_rejected(self):
        feature = self.project["features"][0]
        extra = copy.deepcopy(feature["transform"]["group_by"][0])
        extra["entity_key"] = "device_id"
        feature["transform"]["group_by"].append(extra)
        self.assertIn("DUPLICATE_ENTITY_MAPPING", self.codes())

    def test_entity_mapping_requires_declared_input_and_lineage(self):
        feature = self.project["features"][0]
        feature["inputs"][0]["columns"].remove("tenant_id")
        feature["lineage"]["sources"] = [item for item in feature["lineage"]["sources"]
            if not (item["source"] == "fault_events" and item["column"] == "tenant_id")]
        self.assertIn("UNDECLARED_INPUT_COLUMN", self.codes())

    def test_declared_quality_range_is_executed(self):
        self.project["features"][0]["quality"]["checks"] = [{"kind": "range", "minimum": 100, "severity": "error"}]
        case = self.project["acceptance_suites"][0]["cases"][0]
        with self.assertRaises(EvaluationError) as caught:
            evaluate_case(self.project, case)
        self.assertEqual(caught.exception.code, "QUALITY_VIOLATION")


if __name__ == "__main__":
    unittest.main()
