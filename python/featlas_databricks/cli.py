"""``featlas-databricks``: run a core-generated snapshot on an authorized Databricks target.

Default mode is a dry run that computes the snapshot and contract assertions
without writing. Publication requires ``--publish`` plus explicit environment,
approver and identity arguments; table creation additionally requires
``--allow-create-table``. Credentials are resolved by the Databricks SDK
unified-auth profile named in the adapter configuration; none are accepted here.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile

from .core import Core
from .errors import FeatlasError
from .runner import PublicationAuthorization, SnapshotRun, format_instant, parse_instant


def as_of_from_trigger(trigger: str, grid_seconds: int, offset_seconds: int) -> str:
    """The scheduled as_of for a job trigger time: the trigger floored to the deployment's
    interval grid (epoch + offset + k * grid). Retries of the same run get the same as_of."""
    t = parse_instant(trigger)
    secs = int(t.timestamp())
    k = (secs - offset_seconds) // grid_seconds
    from datetime import datetime, timezone

    return format_instant(datetime.fromtimestamp(k * grid_seconds + offset_seconds, tz=timezone.utc))


def statuses_from_table(spark, table: str, as_of: str) -> list[dict]:
    """Latest source-status evidence per source observed at or before as_of (read-only)."""
    rows = spark.sql(
        f"SELECT source, state, coverage_start, coverage_end FROM {table} WHERE observed_at <= :as_of "
        "QUALIFY row_number() OVER (PARTITION BY source ORDER BY observed_at DESC) = 1",
        args={"as_of": parse_instant(as_of)},
    ).collect()
    out = []
    for r in rows:
        s = {"source": r["source"], "state": r["state"]}
        if r["state"] == "available":
            s.update(coverage_start=format_instant(r["coverage_start"]), coverage_end=format_instant(r["coverage_end"]))
        out.append(s)
    return out


def _spark(profile: str):
    try:
        from databricks.connect import DatabricksSession  # type: ignore
    except ImportError as exc:  # pragma: no cover - live-only path
        raise SystemExit("databricks-connect is not installed; install the 'databricks' extra pinned to the verified version") from exc
    return DatabricksSession.builder.profile(profile).getOrCreate()  # pragma: no cover


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="featlas-databricks")
    ap.add_argument("project")
    ap.add_argument("--deployment", required=True)
    ap.add_argument("--feature-set", required=True)
    when = ap.add_mutually_exclusive_group(required=True)
    when.add_argument("--as-of", help="UTC instant ending in Z")
    when.add_argument("--as-of-from-trigger", help="job trigger time; as_of is it floored to --grid-seconds/--offset-seconds")
    ap.add_argument("--grid-seconds", type=int)
    ap.add_argument("--offset-seconds", type=int, default=0)
    status = ap.add_mutually_exclusive_group(required=True)
    status.add_argument("--status", help="JSON file of source-status evidence records")
    status.add_argument("--status-table", help="table of source-status evidence (source, state, coverage_start, coverage_end, observed_at)")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--environment")
    ap.add_argument("--approved-by")
    ap.add_argument("--publication-id")
    ap.add_argument("--publication-sequence", type=int)
    ap.add_argument("--materialization-id")
    ap.add_argument("--allow-create-table", action="store_true")
    ap.add_argument("--evidence-out")
    args = ap.parse_args(argv)

    core = Core()
    gen = core.generate(args.project, args.deployment, tempfile.mkdtemp(prefix="featlas-gen-"))
    art = gen["artifacts"][args.feature_set]
    adapter = next(a for a in gen["plan"]["bindings"]["adapters"] if a["id"] == gen["plan"]["bindings"]["execution"]["compute"])
    if args.as_of_from_trigger:
        if not args.grid_seconds:
            raise SystemExit("--as-of-from-trigger needs --grid-seconds")
        args.as_of = as_of_from_trigger(args.as_of_from_trigger, args.grid_seconds, args.offset_seconds)
    spark = _spark(adapter["configuration"]["connection_profile"])
    statuses = statuses_from_table(spark, args.status_table, args.as_of) if args.status_table else json.load(open(args.status, encoding="utf-8"))
    unavailable = {s["source"] for s in statuses if s["state"] == "unavailable"}
    coverage = core.coverage(args.project, args.deployment, args.feature_set, args.as_of, statuses)
    pop = next(fs["population"]["source"] for fs in gen["plan"]["logical"]["feature_sets"] if fs["id"] == args.feature_set)
    run = SnapshotRun(spark, art, args.as_of, coverage, unavailable, population_source=pop)
    problems = run.preflight_sources()
    if problems:
        print(json.dumps({"preflight": "failed", "problems": problems}, indent=2))
        return 1
    retention = run.retention_preflight()
    if retention:
        print(json.dumps({"preflight": "failed", "problems": retention}, indent=2))
        return 1
    if args.publish and not args.publication_id:
        # Scheduled runs: the identity is a function of (deployment, feature set, as_of), so a
        # retried or duplicated run of the same slot is recognised as a retry, never a second write.
        args.publication_id = f"{args.deployment}/{args.feature_set}/{args.as_of}"
        args.publication_sequence = int(parse_instant(args.as_of).timestamp())
    try:
        if not args.publish:
            rows = run.run_to_snapshot({"publication_id": "dry-run", "publication_sequence": 0, "materialization_id": "dry-run"})
            print(json.dumps({"dry_run": True, "rows": len(rows), "statement_log": run.log, "plan_digest": gen["manifest"]["plan_digest"]}, indent=2, default=str))
            return 0
        auth = PublicationAuthorization(args.environment or "", args.approved_by or "", args.allow_create_table, args.publication_id or "", args.publication_sequence or 0, args.materialization_id or "")
        evidence = run.publish(auth)
        evidence.update(plan_digest=gen["manifest"]["plan_digest"], project_file_sha256=gen["manifest"]["project_file_sha256"], generator=art["generator"])
        out = json.dumps(evidence, indent=2, default=str)
        if args.evidence_out:
            open(args.evidence_out, "w", encoding="utf-8").write(out)
        print(out)
        return 0
    except FeatlasError as e:
        print(json.dumps({"error": {"code": e.code, "message": str(e), "detail": e.detail}}, indent=2, default=str))
        return 1


if __name__ == "__main__":
    sys.exit(main())
