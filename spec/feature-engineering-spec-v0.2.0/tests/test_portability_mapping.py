"""Exercise local field mappings; no alternative-store execution is implied."""
import copy
import importlib
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from fixture_runtime import evaluate_case
from validation import load_yaml


# Independently authored rows catch casts, dropped keys, and lost missingness.
SOURCE = {
    "id": "counter_observations",
    "version": "1.0.0",
    "columns": [
        {"name": "tenant_id", "type": "string", "nullable": False},
        {"name": "device_id", "type": "string", "nullable": False},
        {"name": "observed_at", "type": "timestamp", "nullable": False},
        {"name": "count", "type": "integer", "nullable": False},
        {"name": "reading", "type": "number", "nullable": True},
    ],
}
SOURCE_BINDING = {
    "source": "counter_observations",
    "version": "1.0.0",
    "column_mapping": {
        "tenant_id": "tenant_key",
        "device_id": "asset_key",
        "observed_at": "event_time",
        "count": "event_count",
        "reading": "temperature",
    },
}
PHYSICAL_SOURCE_ROW = {
    "tenant_key": "tenant_a",
    "asset_key": "device_1",
    "event_time": "2026-06-07T12:00:00.123456Z",
    "event_count": 7,
    "temperature": None,
    "unmapped_ingestion_note": "ignored",
}
LOGICAL_SOURCE_ROW = {
    "tenant_id": "tenant_a",
    "device_id": "device_1",
    "observed_at": "2026-06-07T12:00:00.123456Z",
    "count": 7,
    "reading": None,
}
SNAPSHOT = {
    "entity": {"tenant_id": "tenant_a", "device_id": "device_1"},
    "values": {"device.fault_count_7d": {"value": 0, "state": "ok"}},
    "as_of": "2026-06-08T00:00:00Z",
    "published_at": "2026-06-08T00:00:20.123456Z",
    "publication_sequence": 7,
    "publication_id": "original_publication_7",
    "materialization_id": "migration_import_1",
}
ALTERNATIVE_ROW = {
    "tenant_key": "tenant_a",
    "asset_key": "device_1",
    "fault_total": 0,
    "fault_status": "ok",
    "evaluated_at": "2026-06-08T00:00:00Z",
    "original_visible_at": "2026-06-08T00:00:20.123456Z",
    "original_sequence": 7,
    "original_publication_id": "original_publication_7",
    "target_import_id": "migration_import_1",
}
UC_ROW = {
    "tenant_id": "tenant_a",
    "device_id": "device_1",
    "device_fault_count_7d": 0,
    "device_fault_count_7d_state": "ok",
    "feature_as_of": "2026-06-08T00:00:00Z",
    "published_at": "2026-06-08T00:00:20.123456Z",
    "publication_sequence": 7,
    "publication_id": "original_publication_7",
    "materialization_id": "migration_import_1",
}


class PortabilityMappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project = load_yaml(ROOT / "examples/06_store_migration.yaml")

    def setUp(self):
        try:
            self.mapping = importlib.import_module("portability_mapping")
        except ModuleNotFoundError as exc:
            if exc.name != "portability_mapping":
                raise
            self.fail("The local portability mapping helper is not implemented")
        self.deployment = copy.deepcopy(self.project["deployments"][1])
        self.target = self.deployment["output_targets"][0]

    def test_source_mapping_projects_declared_columns_without_casting(self):
        result = self.mapping.normalize_source_rows(SOURCE, SOURCE_BINDING, [PHYSICAL_SOURCE_ROW])
        self.assertEqual(result, [LOGICAL_SOURCE_ROW])
        self.assertIs(type(result[0]["count"]), int)
        self.assertIsNone(result[0]["reading"])

    def test_source_mapping_rejects_string_seven_for_integer(self):
        row = dict(PHYSICAL_SOURCE_ROW, event_count="7")
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.normalize_source_rows(SOURCE, SOURCE_BINDING, [row])
        self.assertEqual(caught.exception.code, "SOURCE_VALUE_TYPE_MISMATCH")

    def test_source_mapping_rejects_missing_nullable_physical_field(self):
        row = dict(PHYSICAL_SOURCE_ROW)
        del row["temperature"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.normalize_source_rows(SOURCE, SOURCE_BINDING, [row])
        self.assertEqual(caught.exception.code, "MISSING_PHYSICAL_FIELD")

    def test_source_mapping_rejects_missing_or_extra_logical_columns(self):
        for change in ("missing", "extra"):
            with self.subTest(change=change):
                binding = copy.deepcopy(SOURCE_BINDING)
                if change == "missing":
                    del binding["column_mapping"]["tenant_id"]
                else:
                    binding["column_mapping"]["invented"] = "another_field"
                with self.assertRaises(self.mapping.MappingError) as caught:
                    self.mapping.normalize_source_rows(SOURCE, binding, [])
                self.assertEqual(caught.exception.code, "SOURCE_COLUMN_MAPPING_INCOMPLETE")

    def test_source_mapping_rejects_physical_column_collision(self):
        binding = copy.deepcopy(SOURCE_BINDING)
        binding["column_mapping"]["device_id"] = "tenant_key"
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.normalize_source_rows(SOURCE, binding, [PHYSICAL_SOURCE_ROW])
        self.assertEqual(caught.exception.code, "PHYSICAL_COLUMN_COLLISION")

    def test_source_mapping_rejects_wrong_source_or_version(self):
        for key, value in (("source", "another_source"), ("version", "2.0.0")):
            with self.subTest(key=key):
                binding = dict(SOURCE_BINDING, **{key: value})
                with self.assertRaises(self.mapping.MappingError) as caught:
                    self.mapping.normalize_source_rows(SOURCE, binding, [])
                self.assertEqual(caught.exception.code, "SOURCE_BINDING_MISMATCH")

    def test_encode_matches_hand_authored_alternative_physical_row(self):
        self.assertEqual(self.mapping.encode_snapshot(self.target, SNAPSHOT), ALTERNATIVE_ROW)

    def test_decode_matches_hand_authored_logical_snapshot(self):
        self.assertEqual(self.mapping.decode_snapshot(self.target, ALTERNATIVE_ROW), SNAPSHOT)

    def test_storage_rebinding_preserves_zero_null_state_and_publication_fields(self):
        original_target = self.project["deployments"][0]["output_targets"][0]
        for value, state in ((0, "ok"), (None, "insufficient_history")):
            with self.subTest(value=value, state=state):
                original_row = dict(UC_ROW, device_fault_count_7d=value,
                                    device_fault_count_7d_state=state)
                wanted = dict(ALTERNATIVE_ROW, fault_total=value, fault_status=state)
                logical = self.mapping.decode_snapshot(original_target, original_row)
                rebound = self.mapping.encode_snapshot(self.target, logical)
                self.assertEqual(rebound, wanted)
                self.assertEqual(self.mapping.decode_snapshot(self.target, rebound), logical)
        # These are retained declarative selections, not live compute evidence.
        self.assertEqual(self.deployment["execution"], self.project["deployments"][0]["execution"])
        self.assertEqual(self.deployment["execution"]["compute"], "databricks_platform")
        self.assertNotEqual(self.target["adapter"], original_target["adapter"])

    def test_output_mapping_rejects_collision_across_field_groups(self):
        for group in ("state", "metadata", "entity"):
            with self.subTest(group=group):
                target = copy.deepcopy(self.target)
                if group == "state":
                    target["feature_mapping"][0]["state_column"] = "fault_total"
                elif group == "metadata":
                    target["metadata_columns"]["published_at"] = "tenant_key"
                else:
                    target["entity_key_mapping"]["device_id"] = "tenant_key"
                for operation, row in ((self.mapping.encode_snapshot, SNAPSHOT),
                                       (self.mapping.decode_snapshot, ALTERNATIVE_ROW)):
                    with self.assertRaises(self.mapping.MappingError) as caught:
                        operation(target, row)
                    self.assertEqual(caught.exception.code, "PHYSICAL_COLUMN_COLLISION")

    def test_output_mapping_rejects_incomplete_metadata_mapping(self):
        del self.target["metadata_columns"]["publication_id"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.encode_snapshot(self.target, SNAPSHOT)
        self.assertEqual(caught.exception.code, "PUBLICATION_MAPPING_INCOMPLETE")

    def test_output_mapping_rejects_missing_state_mapping(self):
        del self.target["feature_mapping"][0]["state_column"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.decode_snapshot(self.target, ALTERNATIVE_ROW)
        self.assertEqual(caught.exception.code, "OUTPUT_MAPPING_INCOMPLETE")

    def test_output_mapping_rejects_duplicate_feature_identity(self):
        self.target["feature_mapping"].append({
            "feature": "device.fault_count_7d", "version": "1.0.0",
            "column": "other_value", "state_column": "other_state",
        })
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.encode_snapshot(self.target, SNAPSHOT)
        self.assertEqual(caught.exception.code, "DUPLICATE_FEATURE_MAPPING")

    def test_encode_rejects_incomplete_entity_or_feature_mapping(self):
        for group in ("entity", "feature"):
            with self.subTest(group=group):
                snapshot = copy.deepcopy(SNAPSHOT)
                if group == "entity":
                    del snapshot["entity"]["tenant_id"]
                else:
                    snapshot["values"]["undeclared_feature"] = {"value": 1, "state": "ok"}
                with self.assertRaises(self.mapping.MappingError) as caught:
                    self.mapping.encode_snapshot(self.target, snapshot)
                self.assertEqual(caught.exception.code, "SNAPSHOT_MAPPING_MISMATCH")

    def test_encode_rejects_missing_state_instead_of_inventing_one(self):
        snapshot = copy.deepcopy(SNAPSHOT)
        del snapshot["values"]["device.fault_count_7d"]["state"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.encode_snapshot(self.target, snapshot)
        self.assertEqual(caught.exception.code, "MISSING_SNAPSHOT_FIELD")

    def test_decode_rejects_missing_physical_state(self):
        row = dict(ALTERNATIVE_ROW)
        del row["fault_status"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.decode_snapshot(self.target, row)
        self.assertEqual(caught.exception.code, "MISSING_PHYSICAL_FIELD")

    def test_publication_fields_are_required_without_fabricated_defaults(self):
        snapshot = copy.deepcopy(SNAPSHOT)
        del snapshot["published_at"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.encode_snapshot(self.target, snapshot)
        self.assertEqual(caught.exception.code, "MISSING_SNAPSHOT_FIELD")
        row = dict(ALTERNATIVE_ROW)
        del row["original_publication_id"]
        with self.assertRaises(self.mapping.MappingError) as caught:
            self.mapping.decode_snapshot(self.target, row)
        self.assertEqual(caught.exception.code, "MISSING_PHYSICAL_FIELD")

    def test_mapped_rows_use_existing_fixture_evaluator_for_tenant_counts(self):
        case = copy.deepcopy(next(c for c in self.project["acceptance_suites"][0]["cases"]
                                  if c["id"] == "tenant_isolation"))
        sources = {source["id"]: source for source in self.project["sources"]}
        for binding in self.deployment["source_bindings"]:
            sid = binding["source"]
            case["source_rows"][sid] = self.mapping.normalize_source_rows(
                sources[sid], binding, case["source_rows"][sid])
        actual = evaluate_case(self.project, case)
        self.assertEqual(actual, [
            {"entity_keys": {"tenant_id": "tenant_a", "device_id": "device_1"}, "value": 1, "state": "ok"},
            {"entity_keys": {"tenant_id": "tenant_b", "device_id": "device_1"}, "value": 2, "state": "ok"},
        ])
        for result in actual:
            snapshot = copy.deepcopy(SNAPSHOT)
            snapshot["entity"] = result["entity_keys"]
            snapshot["values"]["device.fault_count_7d"] = {"value": result["value"], "state": result["state"]}
            physical = self.mapping.encode_snapshot(self.target, snapshot)
            self.assertEqual(self.mapping.decode_snapshot(self.target, physical), snapshot)


if __name__ == "__main__":
    unittest.main()
