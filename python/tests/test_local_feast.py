"""Feast (open source, local) as the store of a featlas deployment.

The fault-count feature set is computed with the Spark SQL lowering on local Apache Spark and
published to Feast (file offline store, SQLite online store) by featlas_feast. Asserted:

* each publication becomes visible atomically and its published_at is observed through Feast;
* Feast's point-in-time join and the featlas consumer API over the same stored records select the
  same publication and values for every request time: at a publication, between publications,
  before the first one (no value) and beyond the consumer's staleness bound (no value in Feast;
  STALE_FEATURE in featlas);
* a second publication for an as_of already stored is refused (Feast would pick arbitrarily);
* online: after materialize, get_online_features serves the latest snapshot and its
  publication id;
* capability report: Feast offline publication and online publication are local-evidence
  capabilities, online atomicity is unverified, and historical replay is unsupported.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("feast")
yaml = pytest.importorskip("yaml")

from spark_session import LIVE, get_spark  # noqa: E402

from featlas_databricks.core import Core  # noqa: E402
from featlas_databricks.differential import register_source  # noqa: E402
from featlas_databricks.runner import SnapshotRun, format_instant, parse_instant  # noqa: E402
from featlas_feast import FeastStore  # noqa: E402

pytestmark = [
    pytest.mark.skipif(LIVE, reason="local Feast only"),
    pytest.mark.filterwarnings("ignore::FutureWarning"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]

ROOT = Path(__file__).resolve().parents[2]
P = str(ROOT / "projects/live/platform.yaml")
KEYS = [{"tenant_id": t, "device_id": "device_1"} for t in ("tenant_a", "tenant_b")]


@pytest.fixture(scope="module")
def env():
    spark, core = get_spark(), Core()
    doc = yaml.safe_load(Path(P).read_text(encoding="utf-8"))
    case = next(c for c in doc["acceptance_suites"][0]["cases"] if c["id"] == "tenant_isolation")
    statuses = [dict(s, coverage_end="2027-01-01T00:00:00Z") if s.get("coverage_end") else s for s in case["context"]["source_status"]]
    return {"spark": spark, "core": core, "case": case, "statuses": statuses}


def snapshots(env, dep: str, offsets: tuple[int, ...]):
    core, spark = env["core"], env["spark"]
    gen = core.generate(P, dep)
    art = gen["artifacts"]["device_reliability"]
    rel = {}
    for s, spec in art["expected_source_schemas"].items():
        register_source(spark, f"fe_{s}", spec, env["case"]["source_rows"][s])
        rel[s] = f"fe_{s}"
    t0 = parse_instant(env["case"]["as_of"])
    out = []
    for i, dt in enumerate(offsets):
        a = format_instant(t0 + timedelta(seconds=dt))
        run = SnapshotRun(spark, art, a, core.coverage(P, dep, "device_reliability", a, env["statuses"]), set(), rel, population_source="device_registry")
        pub = {"publication_id": f"{dep}-p{i}", "publication_sequence": i + 1, "materialization_id": f"m{i}"}
        out.append((a, pub, run.run_to_snapshot(pub)))
    return gen, out


def consume(env, dep: str, at: str, records: list[dict]) -> dict:
    req = {"api_version": "featlas.consumer/v1", "deployment": dep, "consumer": "reliability_batch", "consumer_version": "1.0.0",
           "rows": [{**k, "as_of": at, "requested_at": format_instant(parse_instant(at) + timedelta(hours=5))} for k in KEYS]}
    resp = env["core"].consume(P, req, records)["response"]
    out = {}
    for r in resp["rows"]:
        v = r["values"][0] if r["values"] else {}
        prov = v.get("provenance") or {}
        out[r["entity_keys"]["tenant_id"]] = (v.get("value"), prov.get("publication_id"))
    return out


def test_offline_publication_and_point_in_time_reads_match_the_core(env, tmp_path):
    gen, pubs = snapshots(env, "fault_feast_offline", (0, 3600, 7200))
    store = FeastStore(gen, "device_reliability", tmp_path)
    applied = store.apply()
    assert applied["ttl_seconds"] == 3600 and applied["entities"] == ["tenant_id", "device_id"]
    for _, pub, rows in pubs:
        ev = store.publish_offline(rows, pub)
        assert ev["published_at"] and ev["rows"] == len(rows) and not ev["retry"]
    assert store.publish_offline(pubs[1][2], pubs[1][1])["retry"] is True
    with pytest.raises(ValueError, match="SAME_AS_OF_REPUBLICATION"):
        store.publish_offline(pubs[1][2], {"publication_id": "another", "publication_sequence": 99})
    records = store.records()
    assert all(r["published_at"] for r in records)
    t = [parse_instant(a) for a, _, _ in pubs]
    probes = {
        "at_t1": format_instant(t[1]),
        "between": format_instant(t[1] + timedelta(minutes=30)),
        "before_first": format_instant(t[0] - timedelta(minutes=1)),
        "stale": format_instant(t[2] + timedelta(hours=2)),
    }
    for label, at in probes.items():
        feast_rows = {r["tenant_id"]: (r.get("device_fault_count_7d"), r.get("publication_id")) for r in store.historical([{**k, "as_of": at} for k in KEYS])}
        core_rows = consume(env, "fault_feast_offline", at, records)
        for tenant in ("tenant_a", "tenant_b"):
            assert feast_rows.get(tenant, (None, None)) == core_rows.get(tenant, (None, None)), (label, tenant, feast_rows, core_rows)
    assert consume(env, "fault_feast_offline", probes["between"], records)["tenant_a"][1] == pubs[1][1]["publication_id"]
    expected = {r["tenant_id"]: r["device_fault_count_7d"] for r in pubs[1][2]}
    assert {k: v[0] for k, v in consume(env, "fault_feast_offline", probes["at_t1"], records).items()} == expected


def test_online_publication_serves_the_latest_snapshot(env, tmp_path):
    gen, pubs = snapshots(env, "fault_feast_online", (0, 3600))
    store = FeastStore(gen, "device_reliability", tmp_path)
    store.apply()
    for a, pub, rows in pubs:
        store.publish_offline(rows, pub)
    a, pub, rows = pubs[-1]
    ev = store.publish_online(pub["publication_id"], a, KEYS)
    assert ev["surface"] == "online_store" and ev["published_at"]
    served = {r["tenant_id"]: r for r in store.online(KEYS)}
    for r in rows:
        assert served[r["tenant_id"]]["device_fault_count_7d"] == r["device_fault_count_7d"]
        assert served[r["tenant_id"]]["publication_id"] == pub["publication_id"]


def test_feast_capabilities_are_reported_honestly(env):
    off = {(c["role"], c["capability"]): c for c in env["core"].generate(P, "fault_feast_offline")["capability_report"]["checks"]}
    on = {(c["role"], c["capability"]): c for c in env["core"].generate(P, "fault_feast_online")["capability_report"]["checks"]}
    assert off[("offline_store", "core.atomic_publication")]["evidence_scope"] == "local_reference"
    assert on[("online_store", "core.atomic_publication")]["status"] == "unverified"
    assert any("batches" in x for x in on[("online_store", "core.atomic_publication")]["limitations"])
    import featlas

    doc = yaml.safe_load(Path(P).read_text(encoding="utf-8"))
    dep = next(d for d in doc["deployments"] if d["id"] == "fault_feast_offline")
    dep["consumer_bindings"].append({"consumer": "reliability_replay", "version": "1.0.0", "adapter": "feast_local", "role": "offline_store"})
    from featlas_reverse.draft import to_yaml

    with pytest.raises(featlas.FeatlasError):
        featlas.Project(to_yaml(doc)).generate("fault_feast_offline")  # replay through Feast is unsupported
