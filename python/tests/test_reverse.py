"""Reverse engineering end to end on local Spark (featlas_reverse).

Legacy SQL (examples/reverse/customer_wide_features.sql) runs unchanged over synthetic sources and
produces the legacy wide table. From it and the table descriptions the drafter writes a spec whose
characterization cases use the legacy outputs as goldens; the core must reproduce them. The spec is
split by domain, every split table is regenerated through the generated Spark SQL, and the
regenerated values must equal the legacy table column by column. A late-arriving row then shows
the one intended divergence: the legacy code is not point-in-time correct, the spec is.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("sqlglot")
yaml = pytest.importorskip("yaml")

from spark_session import LIVE, get_spark  # noqa: E402

import featlas  # noqa: E402
from featlas_databricks.differential import register_source  # noqa: E402
from featlas_databricks.runner import SnapshotRun  # noqa: E402
from featlas_reverse import describe  # noqa: E402
from featlas_reverse.analyze import analyze_sql  # noqa: E402
from featlas_reverse.characterize import legacy_sample  # noqa: E402
from featlas_reverse.draft import draft_spec, to_yaml  # noqa: E402
from featlas_reverse.split import analyze_split, apply_split  # noqa: E402

pytestmark = pytest.mark.skipif(LIVE, reason="local Spark only")

ROOT = Path(__file__).resolve().parents[2]
EX = ROOT / "examples/reverse"
AS_OF = "2026-06-01T00:00:00Z"


def _order(i, tenant, cust, at, status="settled", amount=1000, discount=0, channel="web", pay="card", ship=2, test=False, ingested=None):
    return {"order_id": f"o{i}", "tenant_id": tenant, "customer_id": cust, "ordered_at": at, "ingested_at": ingested or at, "status": status, "amount_cents": amount, "discount_cents": discount, "channel": channel, "payment_method": pay, "shipping_days": ship, "is_test": test}


CUSTOMERS = [
    {"tenant_id": t, "customer_id": c, "registered_at": "2026-01-15T00:00:00Z", "ingested_at": "2026-01-15T00:00:00Z"}
    for t, c in (("tenant_a", "c1"), ("tenant_a", "c2"), ("tenant_b", "c1"))
]
ORDERS = [
    _order(1, "tenant_a", "c1", "2026-05-02T00:00:00Z", amount=1200, channel="web", pay="card", ship=3),
    _order(2, "tenant_a", "c1", "2026-05-10T00:00:00Z", status="returned", amount=800, channel="store", pay="wallet", ship=1),
    _order(3, "tenant_a", "c1", "2026-05-31T23:59:59Z", status="pending", amount=500, discount=50, channel="web", pay="invoice", ship=5),
    _order(4, "tenant_a", "c1", "2026-05-20T00:00:00Z", amount=99999, test=True),  # excluded: test order
    _order(5, "tenant_a", "c1", "2026-05-01T23:59:59Z", amount=7),  # excluded: before the 30-day window
    _order(6, "tenant_a", "c1", "2026-06-01T00:00:00Z", amount=7),  # excluded: exclusive upper bound
    _order(7, "tenant_a", "c2", "2026-05-15T00:00:00Z", status="settled", amount=300, discount=30, channel="store", pay="card", ship=2),
    _order(8, "tenant_b", "c1", "2026-05-16T00:00:00Z", status="cancelled", amount=450, channel="web", pay="wallet", ship=4),
    _order(9, "tenant_b", "c1", "2026-05-17T00:00:00Z", status="settled", amount=650, discount=65, channel="web", pay="card", ship=6),
]


@pytest.fixture(scope="module")
def spark():
    return get_spark()


@pytest.fixture(scope="module")
def inputs():
    sql = (EX / "customer_wide_features.sql").read_text(encoding="utf-8")
    descs = {n: describe.load(EX / f"{n}.table.json") for n in ("orders", "customers", "customer_wide")}
    return sql, analyze_sql(sql), descs


def _register(spark, descs, orders, customers):
    def spec(d):
        return {"columns": [{"physical": c["name"], "logical": c["name"], "type": {"integer": "BIGINT", "number": "DOUBLE", "string": "STRING", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP", "date": "DATE"}[describe.logical_type(c["type"])]} for c in d["columns"]]}

    register_source(spark, "legacy_orders", spec(descs["orders"]), orders)
    register_source(spark, "legacy_customers", spec(descs["customers"]), customers)
    return {"main.sales.orders": "legacy_orders", "main.crm.customers": "legacy_customers"}


def _draft(spark, inputs, orders):
    sql, analysis, descs = inputs
    views = _register(spark, descs, orders, CUSTOMERS)
    legacy = legacy_sample(spark, analysis, views, AS_OF)
    sample = {"as_of": AS_OF, "coverage_end": "2026-06-02T00:00:00Z", "legacy_rows": legacy, "source_rows": {"customers": CUSTOMERS, "orders": orders}}
    out = draft_spec(descs["customer_wide"], analysis, {d["table"]: d for d in (descs["orders"], descs["customers"])}, {"samples": [sample]})
    return out, legacy


def test_draft_reproduces_legacy_outputs(spark, inputs):
    out, legacy = _draft(spark, inputs, ORDERS)
    rep = out["report"]
    assert not [q for q in rep["questions"] if q["blocking"]], rep["questions"]
    assert {a["decision"] for a in rep["assumptions"]} >= {"population.table", "sources.main.sales.orders.available_time"}
    doc = to_yaml(out["project"])
    p = featlas.Project(doc)
    res = p.run_fixtures()
    assert res["summary"]["failed"] == 0, [c for c in res["summary"]["cases"] if c["status"] == "failed"]
    characterized = [c for c in res["summary"]["cases"] if "characterization" in c["case"]]
    aggregates = [o.name for o in inputs[1].outputs if o.kind == "aggregate"]
    assert {c["case"].split("_characterization")[0] for c in characterized} >= set(aggregates), "every aggregate has a legacy-golden case"
    omitted = {a["decision"].split(".")[-1] for a in rep["assumptions"] if a["value"] == "omitted"}
    derived = {o.name for o in inputs[1].outputs if o.kind == "expression"}
    assert len(characterized) + len(omitted) == len(aggregates) + len(derived), "each derived case is asserted or reported as omitted"
    assert len(legacy) == 3


def test_split_by_domain_regenerates_legacy_values(spark, inputs, tmp_path):
    out, legacy = _draft(spark, inputs, ORDERS)
    _, _, descs = inputs
    project = out["project"]
    analysis = analyze_split(project, "customer_wide", descs["customer_wide"])
    best = analysis["strategies"][0]
    assert best["strategy"] == "tag:domain" and best["metrics"]["groups"] == 6
    split = apply_split(project, "customer_wide", best["assignment"])
    doc = to_yaml(split["project"])
    p = featlas.Project(doc)
    assert p.run_fixtures()["summary"]["failed"] == 0
    dep = split["project"]["deployments"][0]["id"]
    gen = p.generate(dep)
    fixtures = {}
    for sid, rows in (("customers", CUSTOMERS), ("orders", ORDERS)):
        fixtures[sid] = rows
    regenerated: dict = {}
    snapshots: dict = {}
    for fs_id, art in gen["artifacts"].items():
        rel = {}
        for source, spec in art["expected_source_schemas"].items():
            register_source(spark, f"fx_{source}", spec, fixtures[source])
            rel[source] = f"fx_{source}"
        statuses = [{"source": s, "state": "available", "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2026-06-02T00:00:00Z"} for s in ("customers", "orders")]
        cov = p.coverage(dep, fs_id, AS_OF, statuses)
        run = SnapshotRun(spark, art, AS_OF, cov, set(), rel, population_source="customers")
        snap = run.run_to_snapshot({"publication_id": "p", "publication_sequence": 1, "materialization_id": "m"})
        snapshots[art["output_table"]] = art["snapshot"]["view"]
        for row in snap:
            key = (row["tenant_id"], row["customer_id"])
            for m in split["mapping"].values():
                if m["group"] == fs_id.split("customer_wide_", 1)[1] and m["column"] in row:
                    regenerated.setdefault(key, {})[m["column"]] = row[m["column"]]
    for lrow in legacy:
        key = (lrow["tenant_id"], lrow["customer_id"])
        for col in split["mapping"]:
            got, want = regenerated[key][col], lrow[col]
            if isinstance(want, float) or isinstance(got, float):
                assert want is not None and got is not None and abs(got - want) <= 1e-9 * max(1, abs(want)), (key, col, got, want)
            else:
                assert got == want, (key, col, got, want)
    assert "CREATE OR REPLACE VIEW `main`.`features`.`customer_wide_featlas_compat`" in split["compatibility_view_sql"]
    # The generated parity SQL itself, run over the legacy rows and the regenerated snapshots.
    from featlas_reverse.parity import mapping_from_project, parity_sql

    spark.createDataFrame([{k: (datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None) if k == "feature_ts" else v) for k, v in r.items() if not k.startswith("fl_state_")} for r in legacy]).createOrReplaceTempView("legacy_wide")
    sql = parity_sql("main.features.customer_wide", "feature_ts", ["tenant_id", "customer_id"], mapping_from_project(split["project"]), AS_OF)
    sql = sql.replace("`main`.`features`.`customer_wide`", "legacy_wide")
    for table, view in snapshots.items():
        sql = sql.replace(table, view)
    result = spark.sql(sql).collect()[0].asDict()
    assert result["rows_compared"] == 3 and result["missing_in_new"] == 0 and result["missing_in_legacy"] == 0, result
    assert all(v == 0 for k, v in result.items() if k.startswith("mismatched_")), result


def test_late_arrival_is_a_reported_legacy_divergence(spark, inputs):
    """An order that arrived after as_of: the legacy SQL counts it, the spec must not."""
    late = _order(10, "tenant_b", "c1", "2026-05-20T00:00:00Z", amount=111, channel="store", pay="card", ingested="2026-06-03T00:00:00Z")
    out, legacy = _draft(spark, inputs, ORDERS + [late])
    p = featlas.Project(to_yaml(out["project"]))
    failed = [c for c in p.run_fixtures()["summary"]["cases"] if c["status"] == "failed"]
    assert failed, "the point-in-time leak must surface as characterization failures"
    assert {c["case"].split("_characterization")[0] for c in failed} >= {"orders_30d", "store_orders_30d"}
    for c in failed:
        assert "tenant_b" in json.dumps(c), "only the entity with the late row diverges"


def test_conditional_sum_with_else_zero_keeps_zero_for_entities_without_matches():
    """SUM(CASE WHEN c THEN x ELSE 0 END): an entity whose rows all fail c is 0, not null."""
    a = analyze_sql("SELECT o.tenant_id, o.customer_id, SUM(CASE WHEN o.status = 'settled' THEN o.amount_cents ELSE 0 END) AS settled "
                    "FROM main.sales.orders o WHERE o.ordered_at >= :as_of - INTERVAL 30 DAYS AND o.ordered_at < :as_of GROUP BY o.tenant_id, o.customer_id")
    [o] = [x for x in a.outputs if x.kind == "aggregate"]
    assert o.operation == "sum" and o.filter is not None and o.coalesce_zero


def test_parity_sql_flags_one_sided_nulls_and_rows_missing_from_one_split_table(spark):
    """Tolerance comparisons must count NULL-vs-value as a mismatch, and an entity absent from
    one split table must be counted even when the union of keys still contains it."""
    from featlas_reverse.parity import parity_sql

    spark.createDataFrame([("t", "a", 1.0, 5), ("t", "b", 2.0, 6)], "tenant_id string, customer_id string, x double, y bigint").createOrReplaceTempView("p_legacy")
    spark.createDataFrame([("t", "a", None), ("t", "b", 2.0)], "tenant_id string, customer_id string, x double").selectExpr("*", "TIMESTAMP '2026-06-01 00:00:00' AS feature_as_of").createOrReplaceTempView("p_new_x")
    spark.createDataFrame([("t", "a", 5)], "tenant_id string, customer_id string, y bigint").selectExpr("*", "TIMESTAMP '2026-06-01 00:00:00' AS feature_as_of").createOrReplaceTempView("p_new_y")
    spark.sql("SELECT *, TIMESTAMP '2026-06-01 00:00:00' AS feature_ts FROM p_legacy").createOrReplaceTempView("p_legacy_ts")
    mapping = {"x": {"table": "main.f.p_new_x", "column": "x"}, "y": {"table": "main.f.p_new_y", "column": "y"}}
    sql = parity_sql("main.f.legacy", "feature_ts", ["tenant_id", "customer_id"], mapping, "2026-06-01T00:00:00Z", tolerances={"x": {"absolute": 0, "relative": 1e-9}})
    for fq, view in (("`main`.`f`.`legacy`", "p_legacy_ts"), ("`main`.`f`.`p_new_x`", "p_new_x"), ("`main`.`f`.`p_new_y`", "p_new_y")):
        sql = sql.replace(fq, view)
    r = spark.sql(sql).collect()[0].asDict()
    assert r["mismatched_x"] == 1, "legacy 1.0 vs regenerated NULL is a mismatch"
    assert r["missing_in_p_new_y"] == 1 and r["missing_in_p_new_x"] == 0
    assert r["missing_in_new"] == 0, "the union alone would have hidden the missing row"


RICH_SQL = """
WITH real_orders AS (
  SELECT order_id, tenant_id, customer_id, ordered_at, ingested_at, status, channel, amount_cents AS amount
  FROM main.sales.orders WHERE is_test = false
),
kept AS (SELECT * FROM real_orders r WHERE r.status <> 'cancelled')
SELECT
  k.tenant_id,
  k.customer_id,
  :as_of AS feature_ts,
  COUNT(*) AS orders_30d,
  COUNT(*) FILTER (WHERE k.ordered_at >= :as_of - INTERVAL 7 DAYS) AS orders_7d,
  SUM(CASE WHEN k.ordered_at >= :as_of - INTERVAL 14 DAYS THEN k.amount END) AS spend_14d,
  TIMESTAMPDIFF(SECOND, MAX(k.ordered_at), :as_of) AS seconds_since_last_order,
  TIMESTAMPDIFF(SECOND, MAX(k.ordered_at) FILTER (WHERE k.channel = 'web'), :as_of) AS seconds_since_last_web_order,
  MAX_BY(k.status, k.ordered_at) AS last_status,
  MIN_BY(k.amount, k.ordered_at) AS first_amount_30d,
  orders_7d / NULLIF(orders_30d, 0) AS share_7d
FROM kept k
WHERE k.ordered_at >= :as_of - INTERVAL 30 DAYS AND k.ordered_at < :as_of
GROUP BY k.tenant_id, k.customer_id
"""


def test_analyzer_inlines_ctes_and_reads_windows_recency_and_lookups():
    a = analyze_sql(RICH_SQL)
    assert a.tables == {"k": "main.sales.orders"} and not a.unsupported, a.unsupported
    by = {o.name: o for o in a.outputs}
    assert by["orders_7d"].window.lookback_seconds == 7 * 86400 and by["orders_7d"].filter is None
    assert by["spend_14d"].window.lookback_seconds == 14 * 86400 and by["spend_14d"].value == {"kind": "column", "input": "k", "column": "amount_cents"}
    assert by["orders_30d"].window is None and a.window.lookback_seconds == 30 * 86400
    assert by["seconds_since_last_order"].kind == "recency" and by["seconds_since_last_order"].event_column == "ordered_at"
    assert by["seconds_since_last_web_order"].filter["operator"] == "equal"
    assert by["last_status"].kind == "lookup" and by["last_status"].order_by == [{"input": "k", "column": "ordered_at", "direction": "desc"}]
    assert by["first_amount_30d"].order_by[0]["direction"] == "asc"
    # CTE filters become shared filters on the base table.
    assert {f["args"][0]["column"] for f in a.filters} == {"is_test", "status"}


@pytest.mark.parametrize("expr, reason", [
    ("DATEDIFF(:as_of, MAX(o.ordered_at))", "calendar-day"),
    ("TIMESTAMPDIFF(MINUTE, MAX(o.ordered_at), :as_of)", "whole seconds"),
    ("COUNT(*) FILTER (WHERE o.ordered_at < :as_of - INTERVAL 7 DAYS)", "windows end at as_of"),
    ("MAX_BY(o.status, o.ordered_at + INTERVAL 1 SECOND)", "order by a column"),
])
def test_analyzer_reports_constructs_without_an_exact_spec_form(expr, reason):
    a = analyze_sql(f"SELECT o.customer_id, {expr} AS x FROM main.sales.orders o "
                    "WHERE o.ordered_at >= :as_of - INTERVAL 30 DAYS AND o.ordered_at < :as_of GROUP BY o.customer_id")
    [u] = [u for u in a.unsupported if u.get("name") == "x"]
    assert reason in u["reason"], u


def test_analyzer_keeps_non_simple_ctes_as_unsupported():
    a = analyze_sql("WITH d AS (SELECT customer_id, COUNT(*) AS n FROM main.sales.orders GROUP BY customer_id) "
                    "SELECT o.customer_id, COUNT(*) AS c FROM main.sales.orders o WHERE o.ordered_at >= :as_of - INTERVAL 1 DAY AND o.ordered_at < :as_of GROUP BY o.customer_id")
    assert [u["name"] for u in a.unsupported if u["construct"] == "CTE"] == ["d"]


def test_draft_with_windows_recency_and_lookups_reproduces_legacy_outputs(spark, inputs):
    _, _, descs = inputs
    analysis = analyze_sql(RICH_SQL)
    views = _register(spark, descs, ORDERS, CUSTOMERS)
    legacy = legacy_sample(spark, analysis, views, AS_OF)
    by_key = {(r["tenant_id"], r["customer_id"]): r for r in legacy}
    a_c1 = by_key[("tenant_a", "c1")]
    assert a_c1["seconds_since_last_order"] == 1 and a_c1["last_status"] == "pending" and a_c1["first_amount_30d"] == 1200
    assert a_c1["orders_7d"] == 1 and a_c1["spend_14d"] == 500
    assert by_key[("tenant_a", "c2")]["fl_state_seconds_since_last_web_order"] == "empty"
    target = {"table": "main.features.customer_rich", "columns": [{"name": n, "type": t} for n, t in (
        ("tenant_id", "string"), ("customer_id", "string"), ("feature_ts", "timestamp"), ("orders_30d", "bigint"), ("orders_7d", "bigint"),
        ("spend_14d", "bigint"), ("seconds_since_last_order", "bigint"), ("seconds_since_last_web_order", "bigint"),
        ("last_status", "string"), ("first_amount_30d", "bigint"), ("share_7d", "double"))]}
    sample = {"as_of": AS_OF, "coverage_end": "2026-06-02T00:00:00Z", "legacy_rows": legacy, "source_rows": {"customers": CUSTOMERS, "orders": ORDERS}}
    out = draft_spec(target, analysis, {d["table"]: d for d in (descs["orders"], descs["customers"])}, {"samples": [sample]})
    assert out["project"] is not None, out["report"]
    feats = {f["id"].split(".", 1)[1]: f for f in out["project"]["features"]}
    assert feats["orders_7d"]["time"]["window"]["lookback_seconds"] == 7 * 86400
    assert feats["share_7d"]["time"]["window"]["lookback_seconds"] == 30 * 86400
    assert feats["seconds_since_last_order"]["transform"]["kind"] == "recency"
    assert feats["last_status"]["transform"] == {**feats["last_status"]["transform"], "kind": "lookup", "on_tie": "error"}
    assert any("MAX_BY" in a["why"] for a in out["report"]["assumptions"])
    v = featlas.validate(to_yaml(out["project"]))
    assert v["passed"], [(d["path"], d["message"]) for d in v["report"]["diagnostics"] if d["severity"] == "error"]
    p = featlas.Project(to_yaml(out["project"]))
    res = p.run_fixtures()["summary"]
    assert res["failed"] == 0, [c for c in res["cases"] if c["status"] == "failed"]
    characterized = {c["case"].split("_characterization")[0] for c in res["cases"] if "characterization" in c["case"] and c["status"] == "passed"}
    assert characterized >= {"orders_7d", "spend_14d", "seconds_since_last_order", "seconds_since_last_web_order", "last_status", "first_amount_30d"}
    # Regenerate on Spark from the drafted spec (Databricks lowering) and compare with the legacy table.
    dep = out["project"]["deployments"][0]["id"]
    gen = p.generate(dep)
    [art] = gen["artifacts"].values()
    rel = {}
    for source, spec in art["expected_source_schemas"].items():
        register_source(spark, f"rx_{source}", spec, {"customers": CUSTOMERS, "orders": ORDERS}[source])
        rel[source] = f"rx_{source}"
    statuses = [{"source": s, "state": "available", "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2026-06-02T00:00:00Z"} for s in ("customers", "orders")]
    run = SnapshotRun(spark, art, AS_OF, p.coverage(dep, "customer_rich", AS_OF, statuses), set(), rel, population_source="customers")
    snap = {(r["tenant_id"], r["customer_id"]): r for r in run.run_to_snapshot({"publication_id": "p", "publication_sequence": 1, "materialization_id": "m"})}
    for key, lrow in by_key.items():
        for col in feats:
            got, want = snap[key][col], lrow[col]
            if isinstance(want, float) or isinstance(got, float):
                assert want is not None and got is not None and abs(got - want) <= 1e-9 * max(1, abs(want)), (key, col, got, want)
            else:
                assert got == want, (key, col, got, want)
