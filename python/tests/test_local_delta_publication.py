"""Publication protocol on real (open-source, local) Delta Lake tables.

Exercises the generated publish/evidence statements and the host's retry logic: one MERGE
commit per publication, published_at recorded from DESCRIBE HISTORY, replay visibility through
the core consumer API, idempotent retry, identity conflicts, recovery when the process dies
between the commit and the evidence update, and the target-table identity check.

Evidence scope: local Delta Lake 4.0 on Apache Spark 4.0.1. Unity Catalog specifics (three-part
names, PRIMARY KEY ... TIMESERIES constraints, in-commit timestamps) are NOT exercised; the
feature table here is created from the generated DDL with the constraint clause removed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
yaml = pytest.importorskip("yaml")

from spark_session import HAVE_DELTA, get_spark  # noqa: E402

from featlas_databricks.core import Core  # noqa: E402
from featlas_databricks.differential import _jsonable, register_source  # noqa: E402
from featlas_databricks.errors import FeatlasError  # noqa: E402
from featlas_databricks.runner import PublicationAuthorization, SnapshotRun, parse_instant  # noqa: E402

pytestmark = pytest.mark.skipif(not HAVE_DELTA, reason="delta-spark not installed")

ROOT = Path(__file__).resolve().parents[2]
PROJECT = str(ROOT / "projects/01_fault_count.yaml")
LOCAL_TABLE = "featlas_delta_test.device_reliability"


@pytest.fixture(scope="module")
def env():
    spark = get_spark()
    core = Core()
    gen = core.generate(PROJECT, "fault_prod")
    art = json.loads(json.dumps(gen["artifacts"]["device_reliability"]).replace(gen["artifacts"]["device_reliability"]["output_table"], LOCAL_TABLE))
    spark.sql("CREATE DATABASE IF NOT EXISTS featlas_delta_test")
    spark.sql(f"DROP TABLE IF EXISTS {LOCAL_TABLE}")
    ddl = re.sub(r",\n  CONSTRAINT [^\n]*\n\)", "\n)", art["create_table"]["sql"])
    assert "CONSTRAINT" not in ddl
    spark.sql(ddl)
    doc = yaml.safe_load(Path(PROJECT).read_text(encoding="utf-8"))
    case = next(c for c in doc["acceptance_suites"][0]["cases"] if c["id"] == "tenant_isolation")
    relations = {}
    for source, spec in art["expected_source_schemas"].items():
        register_source(spark, f"fixture_{source}", spec, case["source_rows"][source])
        relations[source] = f"fixture_{source}"
    coverage = core.coverage(PROJECT, "fault_prod", "device_reliability", case["as_of"], case["context"]["source_status"])
    return {"spark": spark, "core": core, "art": art, "case": case, "relations": relations, "coverage": coverage}


def run(env) -> SnapshotRun:
    return SnapshotRun(env["spark"], env["art"], env["case"]["as_of"], env["coverage"], set(), env["relations"], population_source="device_registry")


def auth(pid: str, seq: int, mid: str) -> PublicationAuthorization:
    return PublicationAuthorization(environment="local-delta-test", approved_by="test-suite", publication_id=pid, publication_sequence=seq, materialization_id=mid)


def merge_commits(spark) -> list:
    return [r for r in spark.sql(f"DESCRIBE HISTORY {LOCAL_TABLE}").collect() if r["operation"] == "MERGE"]


def read_records(spark) -> list[dict]:
    return [{k: _jsonable(v) for k, v in r.asDict().items()} for r in spark.table(LOCAL_TABLE).collect()]


def consume(env, consumer: str, requested_at: str) -> list:
    rows = [{"tenant_id": t, "device_id": "device_1", "as_of": env["case"]["as_of"], "requested_at": requested_at} for t in ("tenant_a", "tenant_b")]
    req = {"api_version": "featlas.consumer/v1", "deployment": "fault_prod", "consumer": consumer, "consumer_version": "1.0.0", "rows": rows}
    out = env["core"].consume(PROJECT, req, read_records(env["spark"]))
    assert "response" in out, out
    resp = out["response"]
    out = []
    for r in resp["rows"]:
        v = r["values"][0] if r["values"] else {}
        out.append((r["entity_keys"]["tenant_id"], v.get("value"), (v.get("provenance") or {}).get("publication_id")))
    return out


def test_publish_records_commit_evidence_and_replays(env):
    spark = env["spark"]
    ev = run(env).publish(auth("pub-1", 1, "mat-1"))
    assert ev["rows"] == 2 and ev["retry"] is False and ev["operation"] == "MERGE"
    assert len(merge_commits(spark)) == 1
    published = {r["published_at"] for r in read_records(spark)}
    assert published == {ev["commit_timestamp"]}, "published_at is the observed MERGE commit instant"
    assert consume(env, "reliability_batch", ev["commit_timestamp"]) == [("tenant_a", 1, "pub-1"), ("tenant_b", 2, "pub-1")]
    # Replay one microsecond before the observed commit: nothing was visible yet.
    t = parse_instant(ev["commit_timestamp"])
    before = (t.replace(microsecond=t.microsecond - 1) if t.microsecond else t.replace(second=t.second - 1, microsecond=999999)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert consume(env, "reliability_replay", before) == [("tenant_a", None, None), ("tenant_b", None, None)]
    assert consume(env, "reliability_replay", ev["commit_timestamp"])[0][1] == 1


def test_retry_is_idempotent_and_reports_original_identities(env):
    spark = env["spark"]
    ev = run(env).publish(auth("pub-1", 1, "mat-1-retry"))
    assert ev["retry"] is True and ev["materialization_id"] == "mat-1" and ev["retry_materialization_id"] == "mat-1-retry"
    assert ev["source_versions"] is None and ev["rows"] == 2
    assert len(merge_commits(spark)) == 1, "a retry must not write a second commit"
    assert spark.table(LOCAL_TABLE).count() == 2


def test_reused_publication_id_with_other_identity_is_rejected(env):
    with pytest.raises(FeatlasError) as e:
        run(env).publish(auth("pub-1", 7, "mat-x"))
    assert e.value.code == "PUBLICATION_IDENTITY_CONFLICT"


def test_crash_between_commit_and_evidence_then_recovery(env):
    spark = env["spark"]
    r = run(env)
    pub = {"publication_id": "pub-2", "publication_sequence": 2, "materialization_id": "mat-2"}
    r.run_to_snapshot(pub)
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "mat-2")
    try:
        r._sql("publish_snapshot", env["art"]["publish"]["sql"], dict(r._params(), **pub))  # commit, then "crash"
    finally:
        spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
    pending = [x for x in read_records(spark) if x["publication_id"] == "pub-2"]
    assert pending and all(x["published_at"] is None for x in pending)
    late = "2099-01-01T00:00:00Z"
    assert {p for _, _, p in consume(env, "reliability_batch", late)} == {"pub-2"}, "live reads serve the committed snapshot"
    assert {p for _, _, p in consume(env, "reliability_replay", late)} == {"pub-1"}, "replay never uses pending evidence"
    ev = run(env).publish(auth("pub-2", 2, "mat-2-recover"))
    assert ev["retry"] is True and ev["materialization_id"] == "mat-2"
    assert {p for _, _, p in consume(env, "reliability_replay", late)} == {"pub-2"}
    assert len(merge_commits(spark)) == 2


def test_target_table_identity_is_checked(env):
    spark = env["spark"]
    spark.sql(f"ALTER TABLE {LOCAL_TABLE} SET TBLPROPERTIES ('featlas.feature_set_fingerprint' = 'other')")
    try:
        with pytest.raises(FeatlasError) as e:
            run(env).publish(auth("pub-3", 3, "mat-3"))
        assert e.value.code == "TARGET_TABLE_MISMATCH"
    finally:
        spark.sql(f"ALTER TABLE {LOCAL_TABLE} SET TBLPROPERTIES ('featlas.feature_set_fingerprint' = '{env['art']['expected_table_properties']['featlas.feature_set_fingerprint']}')")
