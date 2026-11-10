"""``featlas-reverse`` command line (JSON in, JSON/YAML out; read-only)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml


def _json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="featlas-reverse", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("analyze", help="analyze feature SQL")
    a.add_argument("sql")
    d = sub.add_parser("draft", help="draft a spec from a table description, its SQL and source descriptions")
    d.add_argument("--table", required=True, help="legacy feature table description (JSON)")
    d.add_argument("--sql", required=True)
    d.add_argument("--source", action="append", default=[], help="source table description (JSON); repeatable")
    d.add_argument("--decisions", help="decisions JSON (answers to the report's questions)")
    d.add_argument("--out", required=True, help="spec YAML to write")
    d.add_argument("--report", required=True, help="drafting report JSON to write")
    s = sub.add_parser("split-analyze", help="evaluate split strategies for a wide feature set")
    s.add_argument("spec")
    s.add_argument("--feature-set", required=True)
    s.add_argument("--table", help="legacy table description (JSON) for column tags")
    s.add_argument("--max-groups", type=int, default=6)
    s.add_argument("--min-size", type=int, default=2)
    x = sub.add_parser("split-apply", help="apply an assignment {feature_id: group}")
    x.add_argument("spec")
    x.add_argument("--feature-set", required=True)
    x.add_argument("--assignment", required=True, help="JSON file {feature_id: group}, or a split-analyze output plus --strategy")
    x.add_argument("--strategy", help="take the assignment of this strategy from a split-analyze output")
    x.add_argument("--colocate-dependencies", action="store_true")
    x.add_argument("--out", required=True)
    x.add_argument("--mapping", required=True, help="legacy column -> new table JSON to write")
    p = sub.add_parser("parity", help="parity SQL between the legacy table and the regenerated table(s)")
    p.add_argument("spec")
    p.add_argument("--legacy-table", required=True)
    p.add_argument("--legacy-as-of-column", required=True)
    p.add_argument("--key", action="append", required=True)
    p.add_argument("--as-of", required=True)
    sub.add_parser("mcp", help="serve the read-only MCP tools on stdio")
    args = ap.parse_args(argv)

    from . import describe
    from .analyze import analyze_sql

    if args.cmd == "mcp":
        from .mcp_server import serve

        serve()
        return 0
    if args.cmd == "analyze":
        print(json.dumps(analyze_sql(Path(args.sql).read_text(encoding="utf-8")).to_dict(), indent=2))
        return 0
    if args.cmd == "draft":
        import featlas

        from .draft import draft_spec, to_yaml

        out = draft_spec(describe.load(args.table), analyze_sql(Path(args.sql).read_text(encoding="utf-8")), {d["table"]: d for d in map(describe.load, args.source)}, _json(args.decisions) if args.decisions else {})
        Path(args.report).write_text(json.dumps(out["report"], indent=2) + "\n", encoding="utf-8")
        if out["project"] is None:
            print(f"Blocked: see questions in {args.report}", file=sys.stderr)
            return 2
        doc = to_yaml(out["project"])
        Path(args.out).write_text(doc, encoding="utf-8")
        v = featlas.validate(doc)
        print(json.dumps({"spec": args.out, "report": args.report, "valid": v["passed"], "assumptions": len(out["report"]["assumptions"]), "questions": len(out["report"]["questions"]), "unsupported": len(out["report"]["unsupported"])}))
        return 0 if v["passed"] else 1
    spec = yaml.safe_load(Path(args.spec).read_text(encoding="utf-8"))
    if args.cmd == "split-analyze":
        from .split import analyze_split

        res = analyze_split(spec, args.feature_set, describe.load(args.table) if args.table else None, args.max_groups, args.min_size)
        print(json.dumps(res, indent=2))
        return 0
    if args.cmd == "split-apply":
        import featlas

        from .draft import to_yaml
        from .split import apply_split

        raw = _json(args.assignment)
        assignment = next(s["assignment"] for s in raw["strategies"] if s["strategy"] == args.strategy) if args.strategy else raw
        res = apply_split(spec, args.feature_set, assignment, colocate_dependencies=args.colocate_dependencies)
        doc = to_yaml(res["project"])
        Path(args.out).write_text(doc, encoding="utf-8")
        Path(args.mapping).write_text(json.dumps({"mapping": res["mapping"], "compatibility_view_sql": res["compatibility_view_sql"]}, indent=2) + "\n", encoding="utf-8")
        v = featlas.validate(doc)
        print(json.dumps({"spec": args.out, "groups": res["groups"], "valid": v["passed"]}))
        return 0 if v["passed"] else 1
    if args.cmd == "parity":
        from .parity import mapping_from_project, parity_sql

        print(parity_sql(args.legacy_table, args.legacy_as_of_column, args.key, mapping_from_project(spec), args.as_of))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
