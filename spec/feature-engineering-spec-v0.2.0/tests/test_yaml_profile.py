"""These cases pin the document-loading boundary, not a particular parser."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from validation import load_yaml_text, DocumentError


class YamlProfileTests(unittest.TestCase):
    def test_duplicate_mapping_keys_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("name: first\nname: second\n")

    def test_nested_duplicates_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("outer:\n  id: one\n  id: two\n")

    def test_aliases_and_anchors_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("a: &base [1, 2]\nb: *base\n")

    def test_tags_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("value: !!str 12\n")

    def test_merge_key_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("'<<': {name: example}\n")

    def test_nonstring_mapping_keys_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("1: something\n")

    def test_multiple_documents_rejected(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("id: one\n---\nid: two\n")

    def test_json_scalar_types_and_dates(self):
        obj = load_yaml_text("flag: true\nnumber: 12\nratio: 1.25\nmissing: null\nword: yes\nwhen: 2026-11-09\n")
        self.assertIs(obj["flag"], True)
        self.assertEqual(obj["number"], 12)
        self.assertEqual(obj["ratio"], 1.25)
        self.assertIsNone(obj["missing"])
        self.assertEqual(obj["word"], "yes")
        self.assertEqual(obj["when"], "2026-11-09")

    def test_root_must_be_mapping(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("[1, 2]\n")

    def test_timestamp_literals_must_be_quoted(self):
        with self.assertRaises(DocumentError):
            load_yaml_text("when: 2026-11-09T10:00:00Z\n")


if __name__ == "__main__":
    unittest.main()
