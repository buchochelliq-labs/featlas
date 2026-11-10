"""MCP stdio server for reverse engineering and regeneration (JSON-RPC 2.0, one message per line).

Run: ``python -m featlas_reverse.mcp_server`` (or ``featlas-reverse mcp``).

Every tool is side-effect free: documents, SQL and table descriptions are passed inline, results
are returned inline; nothing is read from or written to disk, a warehouse or a catalog. Running
the generated SQL or the parity SQL is the caller's job, with its own authorized connection.
"""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any, Callable

import yaml

import featlas

from . import describe
from .analyze import analyze_sql
from .draft import draft_spec, to_yaml
from .parity import mapping_from_project, parity_sql
from .split import analyze_split, apply_split

PROTOCOL = "2025-06-18"
SERVER = {"name": "featlas-reverse", "version": "0.1.0"}


def _project_dict(document: str) -> dict:
    return yaml.safe_load(document)


def _validation(doc: str, fmt: str = "yaml") -> dict:
    r = featlas.validate(doc, fmt)
    out = {"passed": r["passed"], "diagnostics": [{k: d.get(k) for k in ("severity", "code", "path", "message")} for d in r["report"]["diagnostics"]]}
    if r["passed"]:
        fx = featlas.Project(doc, fmt).run_fixtures()["summary"]
        out["fixtures"] = {k: fx[k] for k in ("executed", "passed", "failed", "not_executed")}
        out["failed_cases"] = [{"case": c["case"], "reason": c["reason"]} for c in fx["cases"] if c["status"] == "failed"]
    return out


def t_analyze(a: dict) -> dict:
    return analyze_sql(a["sql"], a.get("dialect", "databricks")).to_dict()


def t_draft(a: dict) -> dict:
    an = analyze_sql(a["sql"], a.get("dialect", "databricks"))
    sources = {d["table"]: describe.load(d) for d in a.get("sources", [])}
    out = draft_spec(describe.load(a["target_table"]), an, sources, a.get("decisions") or {})
    if out["project"] is None:
        return {"project_yaml": None, "report": out["report"]}
    doc = to_yaml(out["project"])
    return {"project_yaml": doc, "report": out["report"], "validation": _validation(doc)}


def t_validate(a: dict) -> dict:
    return _validation(a["document"], a.get("format", "yaml"))


def t_generate(a: dict) -> dict:
    p = featlas.Project(a["document"], a.get("format", "yaml"))
    g = p.generate(a["deployment"])
    return {
        "execution_ready": g["manifest"]["execution_ready"],
        "capability_report": g["capability_report"],
        "files": g["files"] if a.get("include_files", True) else sorted(g["files"]),
        "manifest": g["manifest"],
    }


def t_split_analyze(a: dict) -> dict:
    legacy = describe.load(a["legacy_table"]) if a.get("legacy_table") else None
    return analyze_split(_project_dict(a["document"]), a["feature_set"], legacy, a.get("max_groups", 6), a.get("min_size", 2))


def t_split_apply(a: dict) -> dict:
    res = apply_split(_project_dict(a["document"]), a["feature_set"], a["assignment"], a.get("tables"), a.get("colocate_dependencies", False))
    doc = to_yaml(res["project"])
    return {"project_yaml": doc, "groups": res["groups"], "mapping": res["mapping"], "compatibility_view_sql": res["compatibility_view_sql"], "validation": _validation(doc)}


def t_parity(a: dict) -> dict:
    mapping = a.get("mapping") or mapping_from_project(_project_dict(a["document"]), a.get("deployment"))
    return {"sql": parity_sql(a["legacy_table"], a["legacy_as_of_column"], a["keys"], mapping, a["as_of"], a.get("tolerances")), "mapping": mapping}


S = {"type": "string"}
O = {"type": "object"}
TOOLS: dict[str, tuple[Callable[[dict], Any], str, dict, list[str]]] = {
    "reverse_analyze_sql": (t_analyze, "Analyze feature-table SQL (Databricks dialect): entity keys, window, shared filters, and per output column its aggregate/filter/derived expression in spec form; untranslatable constructs are listed under 'unsupported'.", {"sql": S, "dialect": S}, ["sql"]),
    "reverse_draft_spec": (t_draft, "Draft a featlas spec (YAML) from a legacy feature table description, its SQL and the source table descriptions. Returns the project, a report of assumptions/questions/unsupported and the legacy-column mapping, and the validation + fixture result. Answer 'questions' via 'decisions' and call again.", {"target_table": O, "sql": S, "sources": {"type": "array", "items": O}, "decisions": O, "dialect": S}, ["target_table", "sql", "sources"]),
    "spec_validate": (t_validate, "Validate a spec document (parse, schema, references/types) and run its acceptance fixtures with the reference evaluator.", {"document": S, "format": S}, ["document"]),
    "spec_generate": (t_generate, "Plan a deployment and return generated Databricks SQL artifacts (in memory), the capability report and manifest. Nothing is executed or published.", {"document": S, "deployment": S, "format": S, "include_files": {"type": "boolean"}}, ["document", "deployment"]),
    "split_analyze": (t_split_analyze, "Evaluate ways to split one wide feature set into smaller feature tables (column tags such as domain/business_unit/owner, filter family, value family, window, source). Returns per-strategy groups and metrics plus a heuristic recommendation; the caller decides.", {"document": S, "feature_set": S, "legacy_table": O, "max_groups": {"type": "integer"}, "min_size": {"type": "integer"}}, ["document", "feature_set"]),
    "split_apply": (t_split_apply, "Apply an explicit assignment {feature_id: group} to a feature set: one feature set, output table and consumer per group. Returns the new spec, the legacy column -> new table mapping, a compatibility view reassembling the wide shape, and validation.", {"document": S, "feature_set": S, "assignment": O, "tables": O, "colocate_dependencies": {"type": "boolean"}}, ["document", "feature_set", "assignment"]),
    "parity_sql": (t_parity, "Read-only SQL comparing the legacy wide table with the regenerated table(s) at one as_of: missing rows on either side and per-column mismatches (null-safe, with optional tolerances).", {"legacy_table": S, "legacy_as_of_column": S, "keys": {"type": "array", "items": S}, "as_of": S, "document": S, "deployment": S, "mapping": O, "tolerances": O}, ["legacy_table", "legacy_as_of_column", "keys", "as_of"]),
}


def _tools_list() -> list[dict]:
    return [{"name": n, "description": d, "inputSchema": {"type": "object", "properties": props, "required": req, "additionalProperties": False}, "annotations": {"readOnlyHint": True, "openWorldHint": False}} for n, (_, d, props, req) in TOOLS.items()]


def handle(msg: dict) -> dict | None:
    mid = msg.get("id")
    method = msg.get("method")
    if mid is None:  # notification
        return None
    try:
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": mid, "result": {"protocolVersion": PROTOCOL, "capabilities": {"tools": {}}, "serverInfo": SERVER, "instructions": "featlas reverse engineering: analyze legacy feature SQL, draft a spec, split wide feature sets, validate, generate and build parity SQL. Read-only."}}
        if method == "ping":
            return {"jsonrpc": "2.0", "id": mid, "result": {}}
        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": mid, "result": {"tools": _tools_list()}}
        if method == "tools/call":
            name = msg["params"]["name"]
            if name not in TOOLS:
                return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32602, "message": f"Unknown tool {name}"}}
            args = msg["params"].get("arguments") or {}
            _, _, props, req = TOOLS[name]
            unknown = set(args) - set(props)
            missing = [r for r in req if r not in args]
            if unknown or missing:
                return {"jsonrpc": "2.0", "id": mid, "result": {"isError": True, "content": [{"type": "text", "text": json.dumps({"error": "INVALID_ARGUMENTS", "unknown": sorted(unknown), "missing": missing})}]}}
            try:
                result = TOOLS[name][0](args)
                return {"jsonrpc": "2.0", "id": mid, "result": {"content": [{"type": "text", "text": json.dumps(result, indent=1, default=str)}], "structuredContent": result if isinstance(result, dict) else {"result": result}}}
            except (featlas.FeatlasError, ValueError, KeyError) as e:
                code = getattr(e, "code", type(e).__name__)
                return {"jsonrpc": "2.0", "id": mid, "result": {"isError": True, "content": [{"type": "text", "text": json.dumps({"error": code, "message": str(e), "detail": getattr(e, "detail", None)}, default=str)}]}}
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"Method {method} not found"}}
    except Exception:  # noqa: BLE001
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32603, "message": traceback.format_exc(limit=3)}}


def serve(stdin=sys.stdin, stdout=sys.stdout) -> None:
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            stdout.write(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}) + "\n")
            stdout.flush()
            continue
        resp = handle(msg)
        if resp is not None:
            stdout.write(json.dumps(resp, default=str) + "\n")
            stdout.flush()


if __name__ == "__main__":
    serve()
