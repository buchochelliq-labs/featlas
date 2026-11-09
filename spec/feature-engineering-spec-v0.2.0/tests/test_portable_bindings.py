"""Portable contracts must survive a store change and reject lossy bindings."""
import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from validation import load_yaml, schema_registry, structural_diagnostics, semantic_diagnostics


class PortableBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema, cls.registry, _ = schema_registry(ROOT / "schemas")
        cls.base = load_yaml(ROOT / "examples/06_store_migration.yaml")

    def setUp(self):
        self.project = copy.deepcopy(self.base)
        self.deployment = self.project["deployments"][1]

    def codes(self):
        errors = structural_diagnostics(self.project, self.schema, self.registry)
        return {d.code for d in errors or semantic_diagnostics(self.project)}

    def test_databricks_compute_with_another_store_is_valid(self):
        self.assertEqual(self.codes(), set())

    def test_new_namespaced_implementation_needs_no_core_schema_change(self):
        self.deployment["adapters"][-1]["implementation"] = "net.independent.vendor:new_store"
        self.assertEqual(self.codes(), set())

    def test_mutable_adapter_version_is_rejected(self):
        self.deployment["adapters"][-1]["version"] = "latest"
        self.assertIn("SCHEMA_INVALID", self.codes())

    def test_adapter_implementation_requires_namespace(self):
        self.deployment["adapters"][-1]["implementation"] = "databricks"
        self.assertIn("SCHEMA_INVALID", self.codes())

    def test_unknown_output_adapter_is_rejected(self):
        self.deployment["output_targets"][0]["adapter"] = "missing_store"
        self.assertIn("UNKNOWN_ADAPTER", self.codes())

    def test_duplicate_adapter_ids_are_rejected(self):
        self.deployment["adapters"].append(copy.deepcopy(self.deployment["adapters"][0]))
        self.assertIn("DUPLICATE_ADAPTER", self.codes())

    def test_storage_adapter_cannot_satisfy_compute_role(self):
        self.deployment["execution"]["compute"] = "alternative_store"
        self.assertIn("ADAPTER_ROLE_MISMATCH", self.codes())

    def test_unknown_orchestrator_is_rejected(self):
        self.deployment["execution"]["orchestrator"] = "missing_scheduler"
        self.assertIn("UNKNOWN_ADAPTER", self.codes())

    def test_source_version_is_pinned(self):
        self.deployment["source_bindings"][0]["version"] = "99.0.0"
        self.assertIn("VERSION_MISMATCH", self.codes())

    def test_source_mapping_cannot_drop_a_logical_column(self):
        self.deployment["source_bindings"][0]["column_mapping"].pop("tenant_id")
        self.assertIn("SOURCE_COLUMN_MAPPING_INCOMPLETE", self.codes())

    def test_source_mapping_cannot_invent_a_logical_column(self):
        self.deployment["source_bindings"][0]["column_mapping"]["invented"] = "extra"
        self.assertIn("SOURCE_COLUMN_MAPPING_INCOMPLETE", self.codes())

    def test_source_mapping_cannot_collapse_two_columns(self):
        mapping = self.deployment["source_bindings"][0]["column_mapping"]
        mapping["device_id"] = mapping["tenant_id"]
        self.assertIn("PHYSICAL_COLUMN_COLLISION", self.codes())

    def test_source_reader_requires_catalog_role(self):
        self.deployment["source_bindings"][0]["adapter"] = "alternative_store"
        self.assertIn("ADAPTER_ROLE_MISMATCH", self.codes())

    def test_output_must_preserve_all_entity_keys(self):
        self.deployment["output_targets"][0]["entity_key_mapping"].pop("tenant_id")
        self.assertIn("OUTPUT_ENTITY_MAPPING_INCOMPLETE", self.codes())

    def test_output_feature_version_is_pinned(self):
        self.deployment["output_targets"][0]["feature_mapping"][0]["version"] = "99.0.0"
        self.assertIn("VERSION_MISMATCH", self.codes())

    def test_output_state_must_not_overwrite_value(self):
        mapping = self.deployment["output_targets"][0]["feature_mapping"][0]
        mapping["state_column"] = mapping["column"]
        self.assertIn("PHYSICAL_COLUMN_COLLISION", self.codes())

    def test_output_metadata_must_not_overwrite_entity_key(self):
        self.deployment["output_targets"][0]["metadata_columns"]["published_at"] = "tenant_key"
        self.assertIn("PHYSICAL_COLUMN_COLLISION", self.codes())

    def test_repeated_target_for_same_set_and_role_is_rejected(self):
        self.deployment["output_targets"].append(copy.deepcopy(self.deployment["output_targets"][0]))
        self.assertIn("DUPLICATE_OUTPUT_TARGET", self.codes())

    def test_required_capability_is_scoped_to_valid_adapter_role(self):
        self.deployment["runtime"]["required_capabilities"][0]["adapter"] = "alternative_store"
        self.assertIn("ADAPTER_ROLE_MISMATCH", self.codes())

    def test_online_surface_needs_online_target(self):
        self.deployment["publication"]["surface"] = "online_store"
        self.assertIn("PUBLICATION_SURFACE_UNBOUND", self.codes())

    def test_fixture_source_cannot_be_an_output_destination(self):
        self.deployment["output_targets"][0]["location"] = {"kind": "fixture", "suite": "does_not_exist"}
        self.assertIn("OUTPUT_LOCATION_READ_ONLY", self.codes())

    def test_format_change_does_not_create_a_different_resource(self):
        target = self.deployment["output_targets"][0]
        duplicate = copy.deepcopy(target)
        self.deployment["adapters"][-1]["roles"].append("online_store")
        duplicate["role"] = "online_store"
        duplicate["location"]["format"] = "another_encoding"
        self.deployment["output_targets"].append(duplicate)
        self.assertIn("DUPLICATE_OUTPUT_RESOURCE", self.codes())

    def test_offline_guarantee_cannot_cover_an_online_consumer(self):
        target = copy.deepcopy(self.deployment["output_targets"][0])
        self.deployment["adapters"][-1]["roles"].append("online_store")
        target["role"] = "online_store"
        target["location"]["uri"] = "example-store://migration-demo/separate-online"
        self.deployment["output_targets"].append(target)
        self.deployment["consumer_bindings"][1]["role"] = "online_store"
        self.assertIn("CONSUMER_SURFACE_MISMATCH", self.codes())

    def test_consumer_binding_requires_existing_target(self):
        self.deployment["consumer_bindings"][0]["adapter"] = "databricks_platform"
        self.assertIn("CONSUMER_TARGET_MISSING", self.codes())

    def test_consumer_version_is_pinned(self):
        self.deployment["consumer_bindings"][0]["version"] = "99.0.0"
        self.assertIn("VERSION_MISMATCH", self.codes())

    def test_binding_change_preserves_logical_objects(self):
        original = load_yaml(ROOT / "examples/01_fault_count.yaml")
        for collection in ("entities", "sources", "relationships", "features", "feature_sets", "consumers", "acceptance_suites"):
            self.assertEqual(self.project[collection], original[collection], collection)

    def test_native_binding_requires_explicit_native_mode(self):
        feature = self.project["features"][0]
        self.deployment["portability"]["native_bindings"] = [{
            "feature": {"id": feature["id"], "version": feature["version"]},
            "adapter": "databricks_platform", "role": "compute",
            "implementation": {"uri": "urn:example:native", "revision": "immutable-example", "sha256": "1" * 64},
            "required_capabilities": ["core.aggregate"],
            "conformance_suite": "fault_count_conformance",
            "migration_notes": "Supply an equivalent implementation and retain exact golden results."
        }]
        self.assertIn("NATIVE_MODE_REQUIRED", self.codes())

    def test_native_binding_cannot_invent_required_capability(self):
        feature = self.project["features"][0]
        self.deployment["portability"] = {"mode": "allow_native", "native_bindings": [{
            "feature": {"id": feature["id"], "version": feature["version"]},
            "adapter": "databricks_platform", "role": "compute",
            "implementation": {"uri": "urn:example:native", "revision": "immutable-example", "sha256": "1" * 64},
            "required_capabilities": ["native.undeclared"],
            "conformance_suite": "fault_count_conformance",
            "migration_notes": "Supply an equivalent implementation and retain exact golden results."
        }]}
        self.assertIn("NATIVE_CAPABILITY_UNDECLARED", self.codes())


if __name__ == "__main__":
    unittest.main()
