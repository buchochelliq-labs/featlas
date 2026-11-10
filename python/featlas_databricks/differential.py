"""Differential conformance: core-generated Spark SQL vs the core evaluator.

Fixture rows are loaded with their *physical* column names into temporary
views that stand in for bound tables; the generated binding statement then
renames them, so source mappings are exercised in Spark. Coverage states and
consumer selection come from the core. This runs on whatever Spark session is
passed in: a local Apache Spark session gives local evidence only and is not
an Azure Databricks acceptance run.
"""

from __future__ import annotations

import os
import time
from typing import Any

from .core import Core
from .errors import FeatlasError
from .runner import SnapshotRun, format_instant, parse_instant



def utc_process() -> None:
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()


def _convert(value: Any, spark_type: str) -> Any:
    if value is None:
        return None
    if spark_type == "TIMESTAMP":
        return parse_instant(value).replace(tzinfo=None)
    if spark_type == "DATE":
        from datetime import date

        return date.fromisoformat(value)
    if spark_type == "DOUBLE":
        return float(value)
    return value


def register_source(spark, name: str, spec: dict, rows: list[dict]) -> None:
    from pyspark.sql import types as T

    # Explicit type objects (no JVM-side type parsing, so this also works over Spark Connect).
    spark_types = {"BIGINT": T.LongType(), "DOUBLE": T.DoubleType(), "BOOLEAN": T.BooleanType(), "STRING": T.StringType(), "TIMESTAMP": T.TimestampType(), "DATE": T.DateType()}
    fields = [T.StructField(c["physical"], spark_types[c["type"]], True) for c in spec["columns"]]
    StructType = T.StructType
    data = [tuple(_convert(r.get(c["logical"]), c["type"]) for c in spec["columns"]) for r in rows]
    spark.createDataFrame(data, StructType(fields)).createOrReplaceTempView(name)


def _jsonable(v: Any) -> Any:
    from datetime import datetime

    if isinstance(v, datetime):
        return format_instant(v)
    return v


def run_case(spark, core: Core, project: str, generated: dict, case: dict, consumer_project: str | None = None) -> dict:
    """Return {status, reason, spark_rows|spark_code, core}."""
    plan = generated["plan"]
    deployment = plan["bindings"]["deployment"]
    feature = case["feature"]["id"]
    fs_id = next(fs["id"] for fs in plan["logical"]["feature_sets"] if any(f["id"] == feature for f in fs["features"]))
    art = generated["artifacts"][fs_id]
    core_result = core.evaluate_case(project, case["id"])
    expected = case["expected"]
    if case["context"]["mode"] == "historical_serving_replay":
        return _replay_case(spark, core, consumer_project or project, plan, art, case, core_result)
    statuses = case["context"]["source_status"]
    unavailable = {s["source"] for s in statuses if s["state"] == "unavailable"}
    relations = {}
    for source, spec in art["expected_source_schemas"].items():
        if source in unavailable:
            continue
        view = f"fixture_{source}"
        register_source(spark, view, spec, case["source_rows"].get(source, []))
        relations[source] = view
    outcome: dict[str, Any] = {"case": case["id"], "feature": feature, "core": core_result.get("status"), "core_reason": core_result.get("reason", ""), "core_code": (core_result.get("actual_diagnostic") or {}).get("code")}
    pop_source = next(fs for fs in plan["logical"]["feature_sets"] if fs["id"] == fs_id)["population"]["source"]
    if pop_source in unavailable:
        code = "SOURCE_UNAVAILABLE"
        return _diag_outcome(outcome, expected, code, "population source unavailable")
    coverage = core.coverage(project, deployment, fs_id, case["as_of"], statuses)
    run = SnapshotRun(spark, art, case["as_of"], coverage, unavailable, relations, population_source=pop_source)
    try:
        run.prepare()
        run.execute()
        pop = [tuple(r) for r in spark.table(f"fl_population_{fs_id.replace('.', '_').replace('-', '_')}").collect()]
        want_pop = sorted(tuple(row.values()) for row in case["population_rows"])
        if sorted(pop) != want_pop:
            return _diag_outcome(outcome, expected, "POPULATION_MISMATCH", "population differs from fixture population_rows")
        view = next(f["computed"]["view"] for f in art["features"] if f["feature"] == feature)
        rows = [r.asDict() for r in spark.table(view).collect()]
    except FeatlasError as e:
        return _diag_outcome(outcome, expected, e.code, str(e))
    keys = [k["name"] for k in next(fs for fs in plan["logical"]["feature_sets"] if fs["id"] == fs_id)["population"]["keys"]]
    spark_rows = [{"entity_keys": {k: r[k] for k in keys}, "value": _jsonable(r["fl_value"]), "state": r["fl_state"]} for r in rows]
    outcome["spark_rows"] = spark_rows
    if "diagnostic" in expected:
        outcome.update(status="failed", reason=f"Spark produced rows; expected {expected['diagnostic']['code']}")
        return outcome
    fplan = next(f for fs in plan["logical"]["feature_sets"] for f in fs["features"] if f["id"] == feature)
    tol = fplan["output"].get("numeric_tolerance") or {"absolute": 0, "relative": 0}
    problems = compare(spark_rows, expected["rows"], fplan["output"]["type"], tol)
    core_rows = core_result.get("actual_rows") or []
    problems += [f"vs core: {p}" for p in compare(spark_rows, core_rows, fplan["output"]["type"], tol)]
    problems += compare_warnings(run.quality_warnings, core_result.get("quality_warnings") or [])
    outcome["quality_warnings"] = run.quality_warnings
    outcome.update(status="failed" if problems else "passed", reason="; ".join(problems))
    return outcome


def compare_warnings(spark: list[dict], core: list[dict]) -> list[str]:
    """Warning-severity findings must match the core exactly (feature, check, keys, value)."""
    import json

    def key(w: dict) -> str:
        return json.dumps([w["feature"], w["check"], w["check_index"], w["outcome"], w["entity_keys"], w["value"]], sort_keys=True)

    a, b = sorted(map(key, spark)), sorted(map(key, core))
    return [] if a == b else [f"quality warnings differ: spark {a} vs core {b}"]


def _diag_outcome(outcome: dict, expected: dict, code: str, why: str, all_codes: list[str] | None = None) -> dict:
    outcome["spark_code"] = code
    outcome["spark_codes"] = all_codes or [code]
    want = expected.get("diagnostic", {}).get("code")
    # The first reported code must match: both engines follow the same canonical precedence.
    if want and code == want:
        outcome.update(status="passed", reason=f"{code} ({why})")
    else:
        outcome.update(status="failed", reason=f"Spark diagnostic {code} ({why}); expected {want or 'rows'}")
    return outcome


def compare(actual: list[dict], expected: list[dict], ty: str, tol: dict) -> list[str]:
    import json

    key = lambda r: json.dumps(r["entity_keys"], sort_keys=True)  # noqa: E731
    a = {key(r): r for r in actual}
    e = {key(r): r for r in expected}
    if set(a) != set(e):
        return [f"entity keys differ: {sorted(a)} vs {sorted(e)}"]
    out = []
    for k, want in e.items():
        got = a[k]
        if got["state"] != want["state"]:
            out.append(f"{k}: state {got['state']} != {want['state']}")
        gv, wv = got["value"], want["value"]
        if gv is None or wv is None:
            if gv is not wv and not (gv is None and wv is None):
                out.append(f"{k}: value {gv!r} != {wv!r}")
        elif ty == "integer":
            if not (isinstance(gv, int) and not isinstance(gv, bool) and gv == wv):
                out.append(f"{k}: integer {gv!r} != {wv!r}")
        elif ty == "number":
            if abs(float(gv) - float(wv)) > max(tol["absolute"], tol["relative"] * abs(float(wv))):
                out.append(f"{k}: number {gv!r} != {wv!r}")
        elif gv != wv or type(gv) is not type(wv):
            out.append(f"{k}: value {gv!r} != {wv!r}")
    return out


def _replay_case(spark, core: Core, project: str, plan: dict, art: dict, case: dict, core_result: dict) -> dict:
    """Store published values with physical names, read them with the generated consumer SQL, select in the core."""
    ctx = case["context"]
    consumer = ctx["consumer"]
    binding = next(b for b in plan["bindings"]["consumers"] if b["consumer"] == consumer)
    target = next(t for t in plan["bindings"]["outputs"] if t["adapter"] == binding["adapter"] and t["role"] == binding["role"])
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    mc = target["metadata_columns"]
    fm = target["feature_mapping"]
    ekm = target["entity_key_mapping"]
    fplan = {f["id"]: f for fs in plan["logical"]["feature_sets"] for f in fs["features"]}
    value_type = {"integer": LongType(), "string": StringType()}
    fields = [StructField(ekm[k], StringType(), False) for k in ekm]
    for m in fm:
        fields.append(StructField(m["column"], value_type.get(fplan[m["feature"]]["output"]["type"], StringType()), True))
        fields.append(StructField(m["state_column"], StringType(), False))
    fields += [StructField(mc["as_of"], TimestampType()), StructField(mc["published_at"], TimestampType()), StructField(mc["publication_sequence"], LongType()), StructField(mc["publication_id"], StringType()), StructField(mc["materialization_id"], StringType())]
    data = []
    for pv in ctx["published_values"]:
        row = [pv["entity_keys"][k] for k in ekm]
        for m in fm:
            row += [pv["value"], pv["state"]] if m["feature"] == pv["feature"] else [None, "not_published"]
        row += [parse_instant(pv["as_of"]).replace(tzinfo=None), parse_instant(pv["published_at"]).replace(tzinfo=None), pv["sequence"], pv["run_id"], pv["run_id"]]
        data.append(tuple(row))
    spark.createDataFrame(data, StructType(fields)).createOrReplaceTempView("fixture_published")
    keys = [tuple(r[k] for k in ekm) for r in case["population_rows"]]
    spark.createDataFrame(keys, StructType([StructField(ekm[k], StringType()) for k in ekm])).createOrReplaceTempView("fl_request_keys")
    sql = art["consumer_read"]["sql"].replace(art["output_table"], "fixture_published")
    records = [{k: _jsonable(v) for k, v in r.asDict().items()} for r in spark.sql(sql, args={"max_as_of": parse_instant(case["as_of"])}).collect()]
    cons = next(c for c in plan["bindings"]["consumer_contracts"] if c["id"] == consumer)
    rows = []
    for r in case["population_rows"]:
        req = {m["input_field"]: r[m["entity_key"]] for m in cons["entity_inputs"]}
        req[cons["time_inputs"]["as_of"]] = case["as_of"]
        req[cons["time_inputs"]["requested_at"]] = ctx["requested_at"]
        rows.append(req)
    request = {"api_version": "featlas.consumer/v1", "deployment": plan["bindings"]["deployment"], "consumer": consumer, "consumer_version": cons["version"], "rows": rows}
    resp = core.consume(project, request, records)
    outcome = {"case": case["id"], "feature": case["feature"]["id"], "core": core_result.get("status"), "records_read_by_spark": len(records)}
    expected = case["expected"]
    if "error" in resp:
        return _diag_outcome(outcome, expected, resp["error"]["code"], "consumer API")
    diags = [d["code"] for row in resp["response"]["rows"] for d in row["diagnostics"]]
    if diags:
        return _diag_outcome(outcome, expected, diags[0], "consumer API row diagnostic", diags)
    got = [{"entity_keys": row["entity_keys"], "value": v["value"], "state": v["state"]} for row in resp["response"]["rows"] for v in row["values"] if v["feature"] == case["feature"]["id"]]
    outcome["spark_rows"] = got
    if "diagnostic" in expected:
        outcome.update(status="failed", reason=f"consumer returned rows; expected {expected['diagnostic']['code']}")
        return outcome
    problems = compare(got, expected["rows"], fplan[case["feature"]["id"]]["output"]["type"], {"absolute": 0, "relative": 0})
    outcome.update(status="failed" if problems else "passed", reason="; ".join(problems))
    return outcome


