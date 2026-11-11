"""Live platform surfaces on Databricks Free Edition: scheduled job, Online Feature Store and
Feature Serving, and Feature Views.

Runs only with FEATLAS_SPARK=databricks and FEATLAS_PLATFORM=1 (job ``platform`` of
.github/workflows/databricks-live.yml). Every resource carries the run id and is removed at the
end, including on failure. A JSON report is written to FEATLAS_PLATFORM_REPORT; a step the
platform refuses (for example a Free Edition limit) is recorded with the platform's message and
fails its test -- it is never reported as passed.

* ``test_scheduled_job``: the generated job (databricks/job.yml of deployment ``fault_job``) is
  created through the Jobs API with the run's wheel and project in a Unity Catalog volume, run
  once on demand and once by its own schedule (unpaused for one 5-minute slot, then paused);
  each run publishes the slot-derived publication ``fault_job/device_reliability/<slot>`` whose
  values equal an interactive run of the same artifacts at the same as_of.
* ``test_online_publication``: deployment ``fault_online`` stages the snapshot, publishes it to
  the Online Feature Store ``featlas-free`` and records published_at only when a Feature Serving
  endpoint returns the publication for every sampled key; served values equal the staged ones.
* ``test_feature_views_parity``: the features that map to Feature Views are computed by
  ``create_training_set`` at as_of and compared with featlas' own values on the same tables.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
yaml = pytest.importorskip("yaml")

from spark_session import LIVE, apply_default_scopes, apply_mlflow_scoped_creds, get_spark, scopes  # noqa: E402

from featlas_databricks import jobs  # noqa: E402
from featlas_databricks.core import Core  # noqa: E402
from featlas_databricks.differential import _jsonable  # noqa: E402
from featlas_databricks.runner import PublicationAuthorization, SnapshotRun, format_instant, parse_instant  # noqa: E402

pytestmark = pytest.mark.skipif(not (LIVE and os.environ.get("FEATLAS_PLATFORM") == "1"), reason="live platform run only (FEATLAS_SPARK=databricks, FEATLAS_PLATFORM=1)")

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "projects/live/platform.yaml"
RUN = re.sub(r"[^a-z0-9]", "", (os.environ.get("GITHUB_RUN_ID") or uuid.uuid4().hex[:10]).lower())
VOLUME = "featlas.synthetic_raw.featlas_artifacts"
REPORT: dict = {"run": RUN, "project": "projects/live/platform.yaml", "scopes": scopes(), "tests": {}}


def q(name: str) -> str:
    return ".".join(f"`{p}`" for p in name.split("."))


def now() -> datetime:
    return datetime.now(timezone.utc)


class Steps:
    """Per-test step log; the report keeps every step, its timing and any platform refusal."""

    def __init__(self, test: str):
        self.out = REPORT["tests"].setdefault(test, {"steps": [], "result": "running"})

    def __call__(self, name: str, **kw):
        self.out["steps"].append({"step": name, "at": format_instant(now()), **{k: _jsonable(v) for k, v in kw.items()}})

    def fail(self, exc: BaseException):
        self.out["result"] = "failed"
        causes, c = [], exc.__cause__ or exc.__context__
        while c is not None and len(causes) < 3:
            causes.append(f"{type(c).__name__}: {str(c)[:3000]}")
            c = c.__cause__ or c.__context__
        self.out["error"] = {"type": type(exc).__name__, "message": str(exc)[:2000], "detail": str(getattr(exc, "detail", "") or "")[:4000], "causes": causes, "trace": traceback.format_exc(limit=4)[-2000:]}

    def ok(self, **kw):
        self.out["result"] = "passed"
        self.out.update({k: _jsonable(v) for k, v in kw.items()})


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    apply_default_scopes()
    spark = get_spark()
    REPORT["mlflow_creds_patched"] = apply_mlflow_scoped_creds()
    from databricks.sdk import WorkspaceClient  # type: ignore

    w = WorkspaceClient()
    text = SRC.read_text(encoding="utf-8")
    names = {}
    for old in ("synthetic_raw.device_registry", "synthetic_raw.fault_events", "synthetic_features.device_reliability_job", "synthetic_features.device_reliability_online_src"):
        sch, name = old.split(".")
        names[f"featlas.{old}"] = f"featlas.{sch}.{name}_{RUN}"
        text = text.replace(f"      - {sch}\n      name: {name}\n", f"      - {sch}\n      name: {name}_{RUN}\n")
    project = tmp_path_factory.mktemp("platform") / "platform.yaml"
    project.write_text(text, encoding="utf-8")
    for schema in ("featlas.synthetic_raw", "featlas.synthetic_features"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(schema)}")
    doc = yaml.safe_load(text)
    case = next(c for c in doc["acceptance_suites"][0]["cases"] if c["id"] == "tenant_isolation")
    REPORT["runtime"] = {"spark_version": spark.version}
    created = list(names.values())
    yield {"spark": spark, "w": w, "core": Core(), "project": str(project), "text": text, "case": case, "names": names, "created": created}
    for t in created:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {q(t)}")
        except Exception:  # noqa: BLE001
            pass
    out = os.environ.get("FEATLAS_PLATFORM_REPORT")
    if out:
        Path(out).write_text(json.dumps(REPORT, indent=2, default=str), encoding="utf-8")


def seed(env, art: dict, extra_faults: list[dict] | None = None) -> None:
    from pyspark.sql import types as T

    spark_types = {"BIGINT": T.LongType(), "DOUBLE": T.DoubleType(), "BOOLEAN": T.BooleanType(), "STRING": T.StringType(), "TIMESTAMP": T.TimestampType()}
    for source, spec in art["expected_source_schemas"].items():
        rows_in = env["case"]["source_rows"][source] + (extra_faults or [] if source == "fault_events" else [])
        schema = T.StructType([T.StructField(c["physical"], spark_types[c["type"]], True) for c in spec["columns"]])
        rows = [tuple(parse_instant(r[c["logical"]]).replace(tzinfo=None) if c["type"] == "TIMESTAMP" else r[c["logical"]] for c in spec["columns"]) for r in rows_in]
        env["spark"].createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(spec["table"].replace("`", ""))


def wait_run(w, run_id: int, timeout_s: float = 1500) -> dict:
    deadline = time.monotonic() + timeout_s
    while True:
        r = jobs.get_run(w, run_id)
        if r.get("state", {}).get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return r
        if time.monotonic() > deadline:
            raise TimeoutError(f"run {run_id} still {r.get('state')} after {timeout_s}s")
        time.sleep(15)


def summarize(w, r: dict) -> dict:
    tasks = []
    for t in r.get("tasks", []):
        out = {}
        try:
            o = jobs.run_output(w, t["run_id"])
            out = {"logs_tail": (o.get("logs") or "")[-3000:], "error": o.get("error"), "error_trace": (o.get("error_trace") or "")[-1500:]}
        except Exception as exc:  # noqa: BLE001
            out = {"output_error": str(exc)[:300]}
        tasks.append({"task_key": t.get("task_key"), "state": t.get("state"), "attempt": t.get("attempt_number"), **out})
    return {"run_id": r.get("run_id"), "trigger": r.get("trigger"), "state": r.get("state"), "start_time": r.get("start_time"), "end_time": r.get("end_time"), "tasks": tasks}


def probe_feature_store_api(w, step) -> None:
    """Record whether the scoped OAuth token is accepted by the Feature Store REST API, which the
    Feature Engineering client uses for Feature Views and FeatureSpecs."""
    for path in ("/api/2.0/feature-store/feature-tables/search", "/api/2.0/feature-engineering/features"):
        try:
            w.api_client.do("GET", path, query={"max_results": 1})
            step("feature_store_api", path=path, accepted=True)
        except Exception as exc:  # noqa: BLE001 - recorded verbatim
            step("feature_store_api", path=path, accepted=False, error=f"{type(exc).__name__}: {str(exc)[:400]}")


def test_scheduled_job(env):
    step = Steps("scheduled_job")
    spark, w, core = env["spark"], env["w"], env["core"]
    job_id = None
    try:
        gen = core.generate(env["project"], "fault_job")
        art = gen["artifacts"]["device_reliability"]
        bundle = json.loads(gen["files"]["databricks/job.yml"])
        # Recent synthetic faults so the scheduled slots (now) have non-empty windows.
        t = now().replace(microsecond=0)
        extra = [{"event_id": f"live-{i}", "tenant_id": "tenant_a", "device_id": "device_1", "occurred_at": format_instant(t - timedelta(hours=h)), "available_at": format_instant(t - timedelta(hours=h) + timedelta(minutes=1)), "is_fault": True} for i, h in enumerate((1, 2))]
        seed(env, art, extra)
        spark.sql(art["create_table"]["sql"])
        status = f"featlas.synthetic_raw.source_status_{RUN}"
        env["created"].append(status)
        spark.sql(f"CREATE OR REPLACE TABLE {q(status)} (source STRING, state STRING, coverage_start TIMESTAMP, coverage_end TIMESTAMP, observed_at TIMESTAMP)")
        spark.sql(f"INSERT INTO {q(status)} VALUES ('device_registry', 'available', TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2030-01-01 00:00:00', TIMESTAMP'2026-01-01 00:00:00'), ('fault_events', 'available', TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2030-01-01 00:00:00', TIMESTAMP'2026-01-01 00:00:00')")
        # Wheel for the serverless architecture, probed on the target itself.
        # The runtime version names the architecture (e.g. 19.13.x-aarch64-photon-scala2.13); a
        # Python UDF probe would depend on the UDF sandbox, which is not what the job uses.
        version = spark.sql("SELECT current_version() AS v").collect()[0]["v"]
        dbr = (version.asDict() if hasattr(version, "asDict") else dict(version)).get("dbr_version") or ""
        arch = "aarch64" if "aarch64" in dbr or "arm64" in dbr else "x86_64"
        probe = dbr
        wheels = sorted(Path(os.environ["FEATLAS_WHEEL_DIR"]).glob(f"featlas-*{arch}*.whl"))
        assert wheels, f"no {arch} wheel in {os.environ['FEATLAS_WHEEL_DIR']}"
        step("probe", dbr_version=probe, arch=arch, wheel=wheels[0].name)
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {q(VOLUME)}")
        base = f"/Volumes/{VOLUME.replace('.', '/')}/{RUN}"
        w.files.upload(f"{base}/{wheels[0].name}", io.BytesIO(wheels[0].read_bytes()), overwrite=True)
        w.files.upload(f"{base}/platform.yaml", io.BytesIO(env["text"].encode()), overwrite=True)
        step("uploaded", base=base)
        settings = jobs.job_settings(bundle, {"project_path": f"{base}/platform.yaml", "featlas_wheel": f"{base}/{wheels[0].name}", "source_status_table": status})
        settings["name"] = f"{settings['name']} [{RUN}]"
        settings["tags"]["featlas_live_run"] = RUN
        job_id = jobs.create(w, settings)
        step("job_created", job_id=job_id, schedule=settings["schedule"], environments=settings["environments"], timeout_seconds=settings["timeout_seconds"])
        # 1. On demand.
        r1 = summarize(w, wait_run(w, jobs.run_now(w, job_id)))
        step("run_now", **r1)
        assert r1["state"].get("result_state") == "SUCCESS", r1
        # 2. By its own schedule: unpause for one slot, then pause again.
        jobs.set_pause(w, job_id, settings["schedule"], paused=False)
        step("unpaused")
        deadline = time.monotonic() + 420
        periodic = None
        while periodic is None and time.monotonic() < deadline:
            time.sleep(20)
            periodic = next((x for x in jobs.list_runs(w, job_id) if x.get("trigger") == "PERIODIC"), None)
        jobs.set_pause(w, job_id, settings["schedule"], paused=True)
        step("paused", saw_periodic=periodic is not None)
        assert periodic is not None, "no scheduled (PERIODIC) run within 7 minutes of unpausing"
        r2 = summarize(w, wait_run(w, periodic["run_id"]))
        step("scheduled_run", **r2)
        assert r2["state"].get("result_state") == "SUCCESS", r2
        # Published slots: identity, completeness, observed evidence, equality with interactive.
        pubs = [r.asDict() for r in spark.sql(f"SELECT publication_id, feature_as_of, count(*) AS n, count(published_at) AS evidenced FROM {art['output_table']} GROUP BY 1, 2 ORDER BY 2").collect()]
        step("publications", rows=pubs)
        assert pubs and all(p["n"] == p["evidenced"] for p in pubs)
        for p in pubs:
            slot = format_instant(p["feature_as_of"].replace(tzinfo=timezone.utc))
            assert p["publication_id"] == f"fault_job/device_reliability/{slot}"
        for r, label in ((r1, "run_now"), (r2, "scheduled")):
            slot = jobs_slot(r["start_time"], 300)
            assert any(format_instant(p["feature_as_of"].replace(tzinfo=timezone.utc)) == slot for p in pubs), (label, slot, pubs)
        last = format_instant(pubs[-1]["feature_as_of"].replace(tzinfo=timezone.utc))
        statuses = [{"source": s, "state": "available", "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2030-01-01T00:00:00Z"} for s in ("device_registry", "fault_events")]
        interactive = SnapshotRun(spark, art, last, core.coverage(env["project"], "fault_job", "device_reliability", last, statuses), set(), None, population_source="device_registry").run_to_snapshot({"publication_id": "x", "publication_sequence": 0, "materialization_id": "x"})
        published = {(r["tenant_id"], r["device_id"]): r.asDict() for r in spark.sql(f"SELECT * FROM {art['output_table']} WHERE feature_as_of = :t", args={"t": parse_instant(last).replace(tzinfo=None)}).collect()}
        col = "device_fault_count_7d"
        diffs = [(k, published[(k["tenant_id"], k["device_id"])].get(col), k.get(col)) for k in interactive if published[(k["tenant_id"], k["device_id"])].get(col) != k.get(col)]
        step("job_equals_interactive", slot=last, rows=len(interactive), diffs=diffs)
        assert not diffs
        assert published[("tenant_a", "device_1")]["device_fault_count_7d"] == 2, "the two seeded faults in the last 7 days"
        step.ok(job_id=job_id, slots=[format_instant(p["feature_as_of"].replace(tzinfo=timezone.utc)) for p in pubs])
    except BaseException as exc:
        step.fail(exc)
        raise
    finally:
        if job_id is not None:
            try:
                jobs.delete(w, job_id)
                step("job_deleted", job_id=job_id)
            except Exception as exc:  # noqa: BLE001
                step("job_delete_failed", error=str(exc)[:300])


def jobs_slot(start_ms: int, grid: int) -> str:
    """The as_of a run started at ``start_ms`` computes (trigger time floored to the grid)."""
    from featlas_databricks.cli import as_of_from_trigger

    return as_of_from_trigger(format_instant(datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)), grid, 0)


def test_online_publication(env):
    step = Steps("online_publication")
    spark, core = env["spark"], env["core"]
    pub = None
    try:
        from databricks.feature_engineering import FeatureEngineeringClient  # type: ignore

        from featlas_databricks.online import OnlineAuthorization, OnlinePublisher

        gen = core.generate(env["project"], "fault_online")
        art = gen["artifacts"]["device_reliability"]
        art["online"]["serving_endpoint"] = f"featlas-dr-{RUN}"[:63]
        seed(env, art)
        case = env["case"]
        cov = core.coverage(env["project"], "fault_online", "device_reliability", case["as_of"], case["context"]["source_status"])
        run = SnapshotRun(spark, art, case["as_of"], cov, set(), None, population_source="device_registry")
        staged = run.publish(PublicationAuthorization("databricks-free", "github-actions:databricks-platform", True, f"online-{RUN}", 1, f"m-{RUN}"))
        step("staged", table=staged["table"], rows=staged["rows"], visibility=staged["visibility"])
        assert staged["visibility"] == "staged_for_online_store"
        keys = [{k: r[k] for k in art["online"]["lookup_keys"]} for r in spark.table(staged["table"].replace("`", "")).collect()]
        pub = OnlinePublisher(spark, art, fe=FeatureEngineeringClient(), workspace=env["w"])
        ev = pub.publish(staged, OnlineAuthorization("databricks-free", "github-actions:databricks-platform", allow_create=True), keys)
        step("online_visible", **{k: v for k, v in ev.items() if k != "steps"}, publisher_steps=ev["steps"])
        served = {tuple(r[k] for k in art["online"]["lookup_keys"]): r for r in pub.lookup(keys)}
        stored = {tuple(r[k] for k in art["online"]["lookup_keys"]): r.asDict() for r in spark.table(staged["table"].replace("`", "")).collect()}
        diffs = [(k, c, served[k].get(c), stored[k].get(c)) for k in stored for c in art["online"]["feature_columns"] if served[k].get(c) != stored[k].get(c)]
        step("served_equals_staged", keys=len(stored), diffs=diffs)
        assert not diffs
        assert all(r["published_at"] is not None for r in stored.values()), "published_at records the observed online instant"
        step.ok(published_at=ev["published_at"], staged_commit_timestamp=ev["staged_commit_timestamp"])
    except BaseException as exc:
        step.fail(exc)
        raise
    finally:
        if pub is not None:
            step("teardown", done=pub.teardown(delete_store=True))


def test_feature_views_parity(env):
    step = Steps("feature_views")
    spark, core = env["spark"], env["core"]
    probe_feature_store_api(env["w"], step)
    sys.path.insert(0, str(Path(__file__).parent))
    import fv_project

    try:
        from databricks.feature_engineering import FeatureEngineeringClient  # type: ignore

        from featlas_databricks import feature_views as fvm

        events, devices, table = (f"featlas.synthetic_raw.fv_events_{RUN}", f"featlas.synthetic_raw.fv_devices_{RUN}", f"featlas.synthetic_features.fv_device_events_{RUN}")
        env["created"] += [events, devices]
        p, report = fv_project.project(events, devices, table)
        path = Path(env["project"]).with_name("fv.yaml")
        path.write_text(fv_project.yaml_text(p), encoding="utf-8")
        dep = p["deployments"][0]["id"]
        gen = core.generate(str(path), dep)
        mapping = gen["feature_views"]["device_events"]
        step("mapping", features=[{k: f[k] for k in ("column", "api", "reasons")} for f in mapping["features"]])
        as_of = "2026-06-01T12:00:00Z"
        at = parse_instant(as_of).replace(tzinfo=None)
        m = lambda minutes: at - timedelta(minutes=minutes)  # noqa: E731
        ev_rows = [  # (event_id, tenant, device, occurred_at, is_fault, severity, code)
            ("e1", "t1", "d1", m(50), True, 2.0, "A"), ("e2", "t1", "d1", m(30), False, 1.0, "B"), ("e3", "t1", "d1", m(10), True, None, "A"),
            ("e4", "t1", "d2", m(70), True, 9.0, "C"),  # outside the 1-hour window: d2 has an empty window
            ("e5", "t2", "d1", m(59), True, 4.5, "A"), ("e6", "t2", "d1", m(1), True, 0.5, "D"),
            ("e7", "t2", "d1", at, True, 100.0, "E"),  # at as_of: excluded (window upper bound exclusive)
        ]
        spark.createDataFrame(ev_rows, "event_id string, tenant_id string, device_id string, occurred_at timestamp, is_fault boolean, severity double, code string").write.format("delta").mode("overwrite").saveAsTable(events)
        spark.createDataFrame([("t1", "d1", m(9000)), ("t1", "d2", m(9000)), ("t2", "d1", m(9000))], "tenant_id string, device_id string, registered_at timestamp").write.format("delta").mode("overwrite").saveAsTable(devices)
        art = gen["artifacts"]["device_events"]
        statuses = [{"source": s, "state": "available", "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2030-01-01T00:00:00Z"} for s in art["expected_source_schemas"]]
        ours = SnapshotRun(spark, art, as_of, core.coverage(str(path), dep, "device_events", as_of, statuses), set(), None, population_source=p["entities"][0]["population"]["source"]).run_to_snapshot({"publication_id": "x", "publication_sequence": 0, "materialization_id": "x"})
        fe = FeatureEngineeringClient()
        feats = fvm.build(mapping)
        keys = [{"tenant_id": r["tenant_id"], "device_id": r["device_id"]} for r in ours]
        theirs = fvm.training_values(fe, spark, feats, keys, at, "occurred_at")
        cmp = fvm.compare(mapping, theirs, ours, ["tenant_id", "device_id"])
        step("compare", result=cmp, featlas=ours, feature_views=theirs)
        bad = {k: v["mismatches"] for k, v in cmp.items() if v["mismatches"]}
        assert not bad, bad
        step.ok(compared={k: v["rows_compared"] for k, v in cmp.items()}, empty_window_served_as_null={k: v["empty_window_served_as_null"] for k, v in cmp.items()})
    except BaseException as exc:
        step.fail(exc)
        raise
