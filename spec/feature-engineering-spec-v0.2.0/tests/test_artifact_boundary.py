import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from validation import load_yaml, DocumentError
from verify_bundle import verify, verify_local_artifact


class ArtifactBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "examples/04_fitted_and_custom_contracts.yaml"
        cls.project = load_yaml(cls.path)
        cls.artifact = next(f["transform"]["artifact"] for f in cls.project["features"] if f["transform"]["kind"] == "learned")

    def test_digest_mismatch_is_rejected(self):
        artifact = copy.deepcopy(self.artifact)
        artifact["sha256"] = "0" * 64
        with self.assertRaises(DocumentError):
            verify_local_artifact(artifact)

    def test_local_artifact_cannot_escape_bundle(self):
        artifact = {"uri": "bundle://../BUILD_NOTES.md", "sha256": "0" * 64}
        with self.assertRaises(DocumentError):
            verify_local_artifact(artifact)

    def test_external_adapter_cases_are_not_counted_as_passes(self):
        report = verify([self.path], run_fixtures=True, run_unit_tests=False)
        self.assertTrue(report["passed"], report["diagnostics"])
        self.assertEqual(report["artifact_digests_checked"], 2)
        self.assertEqual(report["adapter_cases_not_executed"], 3)
        self.assertEqual(report["fixtures_executed"], 3)
        self.assertEqual(report["fixtures_passed"], 3)


if __name__ == "__main__":
    unittest.main()
