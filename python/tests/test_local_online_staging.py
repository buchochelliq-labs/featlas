"""Online staging table semantics (online_store targets) on local Delta Lake.

The generated MERGE for an online_store target keeps exactly the latest complete snapshot per
entity: a newer publication updates rows, an older one or a retry changes nothing, and an entity
that left the population disappears once a newer snapshot is published. Staging never claims
online visibility: published_at stays NULL until featlas_databricks.online observes the rows at
the store (exercised live only).
"""

from __future__ import annotations

import json
import re
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
yaml = pytest.importorskip("yaml")

from spark_session import HAVE_DELTA, LIVE, get_spark  # noqa: E402

from featlas_databricks.core import Core  # noqa: E402
from featlas_databricks.differential import register_source  # noqa: E402
from featlas_databricks.errors import FeatlasError  # noqa: E402
from featlas_databricks.runner import PublicationAuthorization, SnapshotRun, format_instant, parse_instant  # noqa: E402

pytestmark = pytest.mark.skipif(LIVE or not HAVE_DELTA, reason="local Delta only (delta-spark installed, not a live run)")

ROOT = Path(__file__).resolve().parents[2]
PROJECT = str(ROOT / "projects/live/platform.yaml")
TABLE = "featlas_online_test.device_reliability_online_src"


@pytest.fixture(scope="module")
def env():
    spark = get_spark()
    core = Core()
    gen = core.generate(PROJECT, "fault_online")
    a0 = gen["artifacts"]["device_reliability"]
    art = json.loads(json.dumps(a0).replace(a0["output_table"], TABLE))
    spark.sql("CREATE DATABASE IF NOT EXISTS featlas_online_test")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(re.sub(r",\n  CONSTRAINT [^\n]*\n\)", "\n)", art["create_table"]["sql"]))
    doc = yaml.safe_load(Path(PROJECT).read_text(encoding="utf-8"))
    case = next(c for c in doc["acceptance_suites"][0]["cases"] if c["id"] == "tenant_isolation")
    rel = {}
    for source, spec in art["expected_source_schemas"].items():
        register_source(spark, f"on_{source}", spec, case["source_rows"][source])
        rel[source] = f"on_{source}"
    reg = art["expected_source_schemas"]["device_registry"]
    register_source(spark, "on_device_registry_a", reg, [r for r in case["source_rows"]["device_registry"] if r["tenant_id"] == "tenant_a"])
    return {"spark": spark, "core": core, "art": art, "case": case, "rel": rel, "gen": gen}


def publish(env, as_of: str, pid: str, seq: int, registry: str | None = None):
    statuses = [dict(s, coverage_end="2027-01-01T00:00:00Z") if s.get("coverage_end") else s for s in env["case"]["context"]["source_status"]]
    cov = env["core"].coverage(PROJECT, "fault_online", "device_reliability", as_of, statuses)
    rel = dict(env["rel"], **({"device_registry": registry} if registry else {}))
    run = SnapshotRun(env["spark"], env["art"], as_of, cov, set(), rel, population_source="device_registry")
    return run.publish(PublicationAuthorization("local-online-test", "test-suite", False, pid, seq, f"m-{pid}"))


def rows(env) -> dict:
    return {(r["tenant_id"], r["device_id"]): r.asDict() for r in env["spark"].table(TABLE).collect()}


def test_online_target_artifacts_and_capabilities(env):
    art, gen = env["art"], env["gen"]
    assert art["surface"] == "online_store" and art["online"]["lookup_keys"] == ["tenant_id", "device_id"]
    assert "`feature_as_of` TIMESERIES)" in art["create_table"]["sql"] and "publication_sequence` TIMESERIES" not in art["create_table"]["sql"]
    caps = {(c["role"], c["capability"]): c["status"] for c in gen["capability_report"]["checks"]}
    assert ("online_store", "core.online_publication") in caps and ("online_store", "core.serving_replay") not in caps
    assert "databricks/job.yml" not in gen["files"], "a manual online deployment has no job"


def test_staging_keeps_only_the_latest_snapshot_and_never_claims_online_visibility(env):
    t0 = env["case"]["as_of"]
    t1 = format_instant(parse_instant(t0) + timedelta(days=1))
    ev = publish(env, t0, "p0", 1)
    assert ev["surface"] == "online_store" and ev["visibility"] == "staged_for_online_store"
    first = rows(env)
    assert {r["publication_id"] for r in first.values()} == {"p0"} and all(r["published_at"] is None for r in first.values())
    publish(env, t1, "p1", 2)
    second = rows(env)
    assert set(second) == set(first) and {r["publication_id"] for r in second.values()} == {"p1"}
    # An older snapshot arriving late is refused before computing; a retry of p1 changes nothing.
    with pytest.raises(FeatlasError, match="SNAPSHOT_SUPERSEDED|newer snapshot"):
        publish(env, t0, "p0-late", 0)
    assert publish(env, t1, "p1", 2)["retry"] is True
    assert rows(env) == second
    # A newer complete snapshot without tenant_b removes tenant_b's entities.
    t2 = format_instant(parse_instant(t1) + timedelta(days=1))
    publish(env, t2, "p2", 3, registry="on_device_registry_a")
    third = rows(env)
    assert {k[0] for k in third} == {"tenant_a"} and {r["publication_id"] for r in third.values()} == {"p2"}
