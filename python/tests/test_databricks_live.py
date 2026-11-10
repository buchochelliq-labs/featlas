"""Live Azure/AWS Databricks acceptance: real Unity Catalog tables on serverless compute.

Runs only with FEATLAS_SPARK=databricks (see .github/workflows/databricks-live.yml). Uses the
live project variant bound to catalog ``featlas`` and run-unique table names:

* sources ``featlas.synthetic_raw.device_registry_<run>`` / ``fault_events_<run>`` are seeded
  from a reviewed fixture (synthetic data) and read through the generated read-only bindings,
  pinned with ``VERSION AS OF`` and checked by physical-schema preflight;
* the feature table ``featlas.synthetic_features.device_reliability_<run>`` is created from the
  generated DDL (Unity Catalog PRIMARY KEY ... TIMESERIES constraint included), published with
  one MERGE, evidenced through the change data feed, read through the generated consumer query
  and the core consumer API (source-as-known and replay), retried, crash-recovered and
  identity-checked;
* all run tables are dropped at the end.

A JSON report (runtime versions, digests, evidence) is written to FEATLAS_LIVE_REPORT.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
yaml = pytest.importorskip("yaml")

from spark_session import LIVE, get_spark  # noqa: E402

from featlas_databricks.core import Core  # noqa: E402
from featlas_databricks.differential import _jsonable  # noqa: E402
from featlas_databricks.errors import FeatlasError  # noqa: E402
from featlas_databricks.runner import PublicationAuthorization, SnapshotRun, format_instant, parse_instant  # noqa: E402

pytestmark = pytest.mark.skipif(not LIVE, reason="live Databricks run only (FEATLAS_SPARK=databricks)")

ROOT = Path(__file__).resolve().parents[2]
PROJECT = str(ROOT / "projects/live/01_fault_count.yaml")
RUN = re.sub(r"[^a-z0-9]", "", (os.environ.get("GITHUB_RUN_ID") or uuid.uuid4().hex[:10]).lower())
REPORT: dict = {"run": RUN, "project": "projects/live/01_fault_count.yaml", "deployment": "fault_prod", "steps": {}}


def q(name: str) -> str:
    return ".".join(f"`{p}`" for p in name.split("."))


@pytest.fixture(scope="module")
def env():
    spark = get_spark()
    core = Core()
    gen = core.generate(PROJECT, "fault_prod")
    art = gen["artifacts"]["device_reliability"]
    names = {
        "featlas.synthetic_raw.device_registry": f"featlas.synthetic_raw.device_registry_{RUN}",
        "featlas.synthetic_raw.fault_events": f"featlas.synthetic_raw.fault_events_{RUN}",
        "featlas.synthetic_features.device_reliability": f"featlas.synthetic_features.device_reliability_{RUN}",
    }
    text = json.dumps(art)
    for old, new in names.items():
        text = text.replace(q(old).replace("`", "\\u0060"), q(new).replace("`", "\\u0060")).replace(q(old), q(new))
    art = json.loads(text)
    assert art["output_table"] == q(names["featlas.synthetic_features.device_reliability"]), art["output_table"]
    for schema in ("featlas.synthetic_raw", "featlas.synthetic_features"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(schema)}")
    doc = yaml.safe_load(Path(PROJECT).read_text(encoding="utf-8"))
    case = next(c for c in doc["acceptance_suites"][0]["cases"] if c["id"] == "tenant_isolation")
    from pyspark.sql import types as T

    spark_types = {"BIGINT": T.LongType(), "DOUBLE": T.DoubleType(), "BOOLEAN": T.BooleanType(), "STRING": T.StringType(), "TIMESTAMP": T.TimestampType()}
    for source, spec in art["expected_source_schemas"].items():
        schema = T.StructType([T.StructField(c["physical"], spark_types[c["type"]], True) for c in spec["columns"]])
        rows = [tuple(parse_instant(r[c["logical"]]).replace(tzinfo=None) if c["type"] == "TIMESTAMP" else r[c["logical"]] for c in spec["columns"]) for r in case["source_rows"][source]]
        spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(spec["table"].replace("`", ""))
    coverage = core.coverage(PROJECT, "fault_prod", "device_reliability", case["as_of"], case["context"]["source_status"])
    version = spark.sql("SELECT current_version() AS v").collect()[0]["v"]
    REPORT["runtime"] = {"spark_version": spark.version, "databricks_current_version": version.asDict() if hasattr(version, "asDict") else str(version)}
    try:
        import databricks.connect as dbc  # type: ignore

        REPORT["runtime"]["databricks_connect"] = getattr(dbc, "__version__", "unknown")
    except Exception:  # noqa: BLE001
        pass
    REPORT["manifest"] = {k: gen["manifest"][k] for k in ("plan_digest", "logical_plan_digest", "project_file_sha256")}
    REPORT["tables"] = list(names.values())
    yield {"spark": spark, "core": core, "art": art, "case": case, "coverage": coverage, "tables": list(names.values())}
    for t in names.values():
        spark.sql(f"DROP TABLE IF EXISTS {q(t)}")
    out = os.environ.get("FEATLAS_LIVE_REPORT")
    if out:
        Path(out).write_text(json.dumps(REPORT, indent=2, default=str), encoding="utf-8")


def run(env) -> SnapshotRun:
    return SnapshotRun(env["spark"], env["art"], env["case"]["as_of"], env["coverage"], set(), None, population_source="device_registry")


def auth(pid: str, seq: int, mid: str, create: bool = False) -> PublicationAuthorization:
    return PublicationAuthorization(environment="databricks-free", approved_by="github-actions:databricks-live", allow_create_table=create, publication_id=f"{pid}-{RUN}", publication_sequence=seq, materialization_id=f"{mid}-{RUN}")


def read_records(env) -> list[dict]:
    """Physical read through the generated consumer query (request keys as a temp view)."""
    from pyspark.sql import types as T

    spark, art = env["spark"], env["art"]
    keys = [("tenant_a", "device_1"), ("tenant_b", "device_1")]
    spark.createDataFrame(keys, T.StructType([T.StructField("tenant_id", T.StringType()), T.StructField("device_id", T.StringType())])).createOrReplaceTempView("fl_request_keys")
    rows = spark.sql(art["consumer_read"]["sql"], args={"max_as_of": parse_instant(env["case"]["as_of"])}).collect()
    return [{k: _jsonable(v) for k, v in r.asDict().items()} for r in rows]


def consume(env, consumer: str, requested_at: str) -> list:
    rows = [{"tenant_id": t, "device_id": "device_1", "as_of": env["case"]["as_of"], "requested_at": requested_at} for t in ("tenant_a", "tenant_b")]
    req = {"api_version": "featlas.consumer/v1", "deployment": "fault_prod", "consumer": consumer, "consumer_version": "1.0.0", "rows": rows}
    out = env["core"].consume(PROJECT, req, read_records(env))
    assert "response" in out, out
    result = []
    for r in out["response"]["rows"]:
        v = r["values"][0] if r["values"] else {}
        result.append((r["entity_keys"]["tenant_id"], v.get("value"), (v.get("provenance") or {}).get("publication_id")))
    return result


def inserting_commits(env, pid: str) -> list:
    """Every retained MERGE commit that inserted rows of the publication (change data feed)."""
    spark, art = env["spark"], env["art"]
    stmt = {s["name"]: s["sql"] for s in art["evidence"]}
    versions = [int(r["version"]) for r in spark.sql(stmt["commit_candidates"]).collect() if r["operation"] == "MERGE"]
    return [row for v in versions for row in spark.sql(stmt["commit_history"], args={"publication_id": f"{pid}-{RUN}", "from_version": v, "to_version": v}).collect()]


def test_1_preflight_physical_schemas(env):
    problems = run(env).preflight_sources()
    REPORT["steps"]["preflight"] = problems
    assert problems == []


def test_2_publish_creates_uc_feature_table_and_records_evidence(env):
    ev = run(env).publish(auth("pub-1", 1, "mat-1", create=True))
    ev.pop("statement_log", None)
    REPORT["steps"]["publish"] = ev
    assert ev["rows"] == 2 and ev["retry"] is False
    assert set(ev["source_versions"]) == {"device_registry", "fault_events"}, "sources are pinned with VERSION AS OF"
    published = {r["published_at"] for r in read_records(env)}
    assert published == {ev["commit_timestamp"]}
    pk = env["spark"].sql(f"SHOW CREATE TABLE {env['art']['output_table']}").collect()[0][0]
    REPORT["steps"]["primary_key_declared"] = "PRIMARY KEY" in pk.upper() and "TIMESERIES" in pk.upper()
    assert REPORT["steps"]["primary_key_declared"]


def test_3_consumer_reads_and_replay_visibility(env):
    commit = parse_instant(REPORT["steps"]["publish"]["commit_timestamp"])
    batch = consume(env, "reliability_batch", format_instant(commit))
    assert batch == [("tenant_a", 1, f"pub-1-{RUN}"), ("tenant_b", 2, f"pub-1-{RUN}")]
    before = consume(env, "reliability_replay", format_instant(commit - timedelta(microseconds=1)))
    assert [v for _, v, _ in before] == [None, None]
    at = consume(env, "reliability_replay", format_instant(commit))
    assert [v for _, v, _ in at] == [1, 2]
    REPORT["steps"]["consumer"] = {"batch": batch, "replay_before_commit": before, "replay_at_commit": at}


def test_4_retry_is_idempotent(env):
    ev = run(env).publish(auth("pub-1", 1, "mat-1-retry"))
    assert ev["retry"] is True and ev["materialization_id"] == f"mat-1-{RUN}"
    assert len(inserting_commits(env, "pub-1")) == 1
    REPORT["steps"]["retry"] = {"retry": ev["retry"], "materialization_id": ev["materialization_id"], "inserting_commits": 1}


def test_5_reused_publication_identity_is_rejected(env):
    with pytest.raises(FeatlasError) as e:
        run(env).publish(auth("pub-1", 9, "mat-x"))
    assert e.value.code == "PUBLICATION_IDENTITY_CONFLICT"


def test_6_crash_between_commit_and_evidence_then_recovery(env):
    r = run(env)
    pub = {"publication_id": f"pub-2-{RUN}", "publication_sequence": 2, "materialization_id": f"mat-2-{RUN}"}
    r.run_to_snapshot(pub)
    r._sql("publish_snapshot", env["art"]["publish"]["sql"], dict(r._params(), **pub))  # commit, then "crash"
    late = "2099-01-01T00:00:00Z"
    assert {p for _, _, p in consume(env, "reliability_batch", late)} == {f"pub-2-{RUN}"}
    assert {p for _, _, p in consume(env, "reliability_replay", late)} == {f"pub-1-{RUN}"}
    ev = run(env).publish(auth("pub-2", 2, "mat-2-recover"))
    assert ev["retry"] is True
    assert {p for _, _, p in consume(env, "reliability_replay", late)} == {f"pub-2-{RUN}"}
    REPORT["steps"]["crash_recovery"] = "pending evidence served live, excluded from replay, recovered by retry"


def test_7_target_table_identity_is_checked(env):
    spark, t = env["spark"], env["art"]["output_table"]
    fp = env["art"]["expected_table_properties"]["featlas.feature_set_fingerprint"]
    spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('featlas.feature_set_fingerprint' = 'other')")
    try:
        with pytest.raises(FeatlasError) as e:
            run(env).publish(auth("pub-3", 3, "mat-3"))
        assert e.value.code == "TARGET_TABLE_MISMATCH"
    finally:
        spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('featlas.feature_set_fingerprint' = '{fp}')")
