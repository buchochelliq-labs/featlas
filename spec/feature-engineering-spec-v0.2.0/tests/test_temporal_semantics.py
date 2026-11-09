import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from fixture_runtime import EvaluationError, select_source_rows, in_window, expression

AS_OF = "2026-11-09T10:00:00Z"
SOURCE = {
    "time": {"event_time": "occurred_at", "available_time": "available_at", "timezone": "UTC"},
    "record_semantics": {
        "kind": "versioned", "logical_key": ["tenant", "device", "id"],
        "duplicate_policy": "collapse_identical",
        "revision": {"selection": "latest_available", "order_by": [{"column": "revision", "direction": "desc"}],
                     "tombstone_column": "deleted", "tombstone_value": True},
    },
}


def row(**changes):
    base = dict(tenant="a", device="d", id="one", revision=1, deleted=False,
                occurred_at="2026-11-09T09:50:00Z", available_at="2026-11-09T09:51:00Z", value=10)
    base.update(changes)
    return base


class TemporalTests(unittest.TestCase):
    def test_revision_timestamp_order_uses_instants(self):
        source = copy.deepcopy(SOURCE)
        source["columns"] = [{"name": "available_at", "type": "timestamp"}]
        source["record_semantics"]["revision"]["order_by"] = [{"column": "available_at", "direction": "desc"}]
        rows = [row(), row(revision=2, value=20, available_at="2026-11-09T09:51:00.100000Z")]
        self.assertEqual(select_source_rows(source, rows, AS_OF)[0]["value"], 20)

    def test_late_revision_does_not_change_past(self):
        rows = [row(), row(revision=2, value=20, available_at="2026-11-09T10:01:00Z")]
        self.assertEqual(select_source_rows(SOURCE, rows, AS_OF)[0]["value"], 10)

    def test_revision_before_window_filter(self):
        rows = [row(), row(revision=2, occurred_at="2026-11-08T01:00:00Z", available_at="2026-11-09T09:59:00Z")]
        selected = select_source_rows(SOURCE, rows, AS_OF)
        self.assertEqual(len(selected), 1)
        self.assertFalse(in_window(selected[0]["occurred_at"], AS_OF, 3600, "inclusive", "exclusive"))

    def test_retraction_prevents_old_revision_reappearing(self):
        rows = [row(), row(revision=2, deleted=True, available_at="2026-11-09T09:59:00Z")]
        self.assertEqual(select_source_rows(SOURCE, rows, AS_OF), [])

    def test_identical_redelivery_does_not_duplicate(self):
        self.assertEqual(len(select_source_rows(SOURCE, [row(), row()], AS_OF)), 1)

    def test_conflicting_revision_fails(self):
        with self.assertRaises(EvaluationError) as caught:
            select_source_rows(SOURCE, [row(), row(value=20)], AS_OF)
        self.assertEqual(caught.exception.code, "DUPLICATE_CONFLICT")

    def test_tenant_participates_in_identity(self):
        self.assertEqual(len(select_source_rows(SOURCE, [row(), row(tenant="b")], AS_OF)), 2)

    def test_half_open_boundaries(self):
        self.assertTrue(in_window("2026-11-09T09:00:00Z", AS_OF, 3600, "inclusive", "exclusive"))
        self.assertFalse(in_window(AS_OF, AS_OF, 3600, "inclusive", "exclusive"))

    def test_invalid_latest_value_remains_latest(self):
        rows = [row(), row(revision=2, value=None, available_at="2026-11-09T09:59:00Z")]
        self.assertIsNone(select_source_rows(SOURCE, rows, AS_OF)[0]["value"])


def literal(value, kind="integer"):
    return {"kind": "literal", "type": kind, "value": value}


class ExpressionTests(unittest.TestCase):
    def test_timestamp_comparison_uses_instants(self):
        expr = {"kind": "call", "operator": "less_than", "args": [
            literal("2026-01-01T00:00:00Z", "timestamp"),
            literal("2026-01-01T00:00:00.100000Z", "timestamp")]}
        self.assertTrue(expression(expr, {}, {}))

    def test_timestamp_equality_ignores_fraction_padding(self):
        expr = {"kind": "call", "operator": "equal", "args": [
            literal("2026-01-01T00:00:00Z", "timestamp"),
            literal("2026-01-01T00:00:00.000000Z", "timestamp")]}
        self.assertTrue(expression(expr, {}, {}))

    def test_safe_divide_zero_is_null(self):
        expr = {"kind": "call", "operator": "safe_divide", "args": [literal(2), literal(0)]}
        self.assertIsNone(expression(expr, {}, {}))

    def test_divide_zero_is_error(self):
        expr = {"kind": "call", "operator": "divide", "args": [literal(2), literal(0)]}
        with self.assertRaises(EvaluationError):
            expression(expr, {}, {})

    def test_integer_overflow_is_error(self):
        expr = {"kind": "call", "operator": "add", "args": [literal(9223372036854775807), literal(1)]}
        with self.assertRaises(EvaluationError):
            expression(expr, {}, {})

    def test_three_valued_equality(self):
        expr = {"kind": "call", "operator": "equal", "args": [literal(None), literal(None)]}
        self.assertIsNone(expression(expr, {}, {}))

    def test_if_else_does_not_evaluate_unused_branch(self):
        expr = {"kind": "call", "operator": "if_else", "args": [literal(True,"boolean"), literal(7),
                {"kind": "call", "operator": "divide", "args": [literal(1), literal(0)]}]}
        self.assertEqual(expression(expr, {}, {}), 7)


if __name__ == "__main__":
    unittest.main()
