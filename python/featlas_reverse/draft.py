"""Draft a featlas project (Feature Engineering Specification 0.2.0) from a feature table.

Inputs: the legacy feature table's description, the SQL that builds it (analyzed by
``analyze.analyze_sql``), descriptions of the tables it reads, and ``decisions`` — answers to the
questions the code cannot answer (which table defines the entity population, which column is the
availability time, who owns what). Every default the drafter takes is listed in
``report["assumptions"]``; every blocking gap is in ``report["questions"]``. A draft with open
questions is still written so a reviewer can see it, but it is marked ``status: draft``.

Acceptance cases are never invented. The drafter emits a structural case (empty population ->
no rows) and, when ``decisions["samples"]`` is given, *characterization* cases whose expected
values are the legacy table's own outputs for sampled entities (an independent implementation;
SEM-TEST-002), flagged as such.
"""

from __future__ import annotations

import copy
import re
from typing import Any

from . import describe
from .analyze import Analysis, Output

ROW_KINDS = ("aggregate", "recency", "lookup")
CORE_CAPS = ["core.aggregate", "core.expression", "core.revision_reconstruction", "core.as_of_join", "core.source_as_known"]
AVAILABILITY_HINTS = ("available_at", "ingested_at", "_ingested_at", "loaded_at", "_loaded_at", "_commit_timestamp", "arrival_ts", "received_at", "created_at_ingest")
REGISTRY_HINTS = ("registry", "dim_", "_dim", "customers", "accounts", "devices", "users", "entities")


def _ident(s: str) -> str:
    s = re.sub(r"[^a-z0-9_]+", "_", s.lower()).strip("_")
    return s if re.match(r"[a-z]", s) else f"x_{s}"


def _table_parts(fq: str) -> tuple[list[str], str]:
    parts = fq.split(".")
    return parts[:-1], parts[-1]


class Report:
    def __init__(self):
        self.assumptions: list[dict] = []
        self.questions: list[dict] = []
        self.unsupported: list[dict] = []
        self.mapping: dict[str, Any] = {}

    def assume(self, key: str, value: Any, why: str):
        self.assumptions.append({"decision": key, "value": value, "why": why})
        return value

    def ask(self, key: str, question: str, options: list | None = None, blocking: bool = True):
        self.questions.append({"decision": key, "question": question, "options": options or [], "blocking": blocking})

    def to_dict(self):
        return {"assumptions": self.assumptions, "questions": self.questions, "unsupported": self.unsupported, "mapping": self.mapping}


def _decide(decisions: dict, path: str, default: Any, why: str, report: Report) -> Any:
    cur: Any = decisions
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return report.assume(path, default, why)
        cur = cur[p]
    return cur


def _source_columns(desc: dict, used: set[str]) -> list[dict]:
    out = []
    for c in desc["columns"]:
        if c["name"] not in used:
            continue
        out.append({
            "name": c["name"],
            "type": describe.logical_type(c["type"]),
            "description": c.get("comment") or f"{desc['table']}.{c['name']} (no column comment in the source table; review).",
            "nullable": bool(c.get("nullable", True)),
            "unit": c.get("tags", {}).get("unit"),
            "classification": c.get("tags", {}).get("classification", "internal"),
        })
    return out


def _source(sid: str, desc: dict, used: set[str], event_time: str, available: str, pk: list[str], earliest: str, owner: str) -> dict:
    cols = _source_columns(desc, used | {event_time, available} | set(pk))
    return {
        "id": sid,
        "version": "1.0.0",
        "name": desc["table"],
        "description": desc.get("comment") or f"Reverse-engineered binding of {desc['table']}.",
        "owner": desc.get("owner") or owner,
        "columns": cols,
        "primary_key": pk,
        "record_semantics": {"kind": "append_only", "duplicate_policy": "reject", "logical_key": pk},
        "time": {"event_time": event_time, "available_time": available, "timezone": "UTC"},
        "quality": {
            "null_key_policy": "reject",
            "invalid_row_policy": "reject",
            "max_lateness_seconds": 86400,
            "completeness": {"mode": "external_status", "description": "Completeness must be supplied by a source-status observation; an empty table proves no coverage."},
        },
        "retention": {"raw_seconds": 31536000, "revision_history_seconds": 31536000, "earliest_reproducible_at": earliest},
        "lineage": {
            "description": f"Physical table {desc['table']} read by the legacy feature code.",
            "code": None,
            "upstream_columns": [{"column": c["name"], "origin": f"{desc['table']}.{c['name']}", "description": c["description"]} for c in cols],
        },
    }


def _guess_available(desc: dict) -> str | None:
    names = [c["name"] for c in desc["columns"]]
    return next((h for h in AVAILABILITY_HINTS if h in names), None)


def _rename_inputs(e: Any, alias_map: dict[str, str]) -> Any:
    if isinstance(e, dict):
        out = {k: _rename_inputs(v, alias_map) for k, v in e.items()}
        if out.get("kind") == "column" and out.get("input") in alias_map:
            out["input"] = alias_map[out["input"]]
        return out
    if isinstance(e, list):
        return [_rename_inputs(x, alias_map) for x in e]
    return e


def _cols_of(e: Any, acc: set) -> set:
    if isinstance(e, dict):
        if e.get("kind") == "column":
            acc.add(e["column"])
        for v in e.values():
            _cols_of(v, acc)
    elif isinstance(e, list):
        for v in e:
            _cols_of(v, acc)
    return acc


def draft_spec(target: dict, analysis: Analysis, sources: dict[str, dict], decisions: dict | None = None) -> dict:
    """Return ``{"project": <dict>, "report": <dict>}``. Validate the project with ``featlas``."""
    decisions = decisions or {}
    r = Report()
    r.unsupported = list(analysis.unsupported)
    target = describe.load(target)
    sources = {k: describe.load(v) for k, v in sources.items()}
    fact_table = analysis.tables[analysis.fact_alias]
    if analysis.joins:
        r.unsupported.append({"construct": "JOIN", "sql": str(analysis.joins), "advice": "Joins become spec relationships; declare the relationship (cardinality, valid time) and add it to the draft by hand. Joined columns are not drafted."})
    if fact_table not in sources:
        r.ask(f"sources.{fact_table}", f"Provide a table description for {fact_table} (column types are needed).")
        return {"project": None, "report": r.to_dict()}
    owner = _decide(decisions, "project.owner", target.get("owner") or "featlas.reverse-engineering", "Owner of the legacy table (or a placeholder)", r)
    fact_sid = _ident(_decide(decisions, f"source_ids.{fact_table}", _table_parts(fact_table)[1], "Logical source id from the table name", r))
    fact = sources[fact_table]
    keys = [c for a, c in analysis.group_by]
    if not keys:
        r.ask("entity.keys", "The SQL has no GROUP BY; which columns identify one feature-table row?")
        return {"project": None, "report": r.to_dict()}
    entity_id = _ident(_decide(decisions, "entity.id", re.sub(r"_id$", "", keys[-1]), "Entity named after its most specific key", r))
    tenant = _decide(decisions, "entity.tenant_key", next((k for k in keys if re.search(r"tenant|org|account", k)), None), "Tenant key detected by name (null = single-tenant)", r)
    # Each row-level output uses its own window when it narrows the shared WHERE window.
    row_outputs = [o for o in analysis.outputs if o.kind in ROW_KINDS]
    windows = {o.name: o.window or analysis.window for o in row_outputs}
    if not row_outputs or any(v is None for v in windows.values()):
        r.ask("window", "No event-time window was found in WHERE (col >= :as_of - INTERVAL ... AND col < :as_of) or in the output's FILTER/CASE. What window does each feature use?", [o.name for o in row_outputs if windows[o.name] is None])
        return {"project": None, "report": r.to_dict()}
    if any(v.lookback_seconds is None for v in windows.values()):
        r.ask("window.lookback", "The window has no lower bound; unbounded history is not a portable feature window. Give a lookback.", [n for n, v in windows.items() if v.lookback_seconds is None])
        return {"project": None, "report": r.to_dict()}
    event_columns = {(v.alias, v.event_column) for v in windows.values()}
    if len(event_columns) != 1:
        r.ask("window.event_time", f"Outputs bound different event-time columns {sorted(event_columns)}; a source has one event time. Which column is it?")
        return {"project": None, "report": r.to_dict()}
    w = analysis.window or next(iter(windows.values()))
    for o in analysis.outputs:
        for n in o.notes:
            r.assume(f"outputs.{o.name}", "legacy semantics", n)
    # --- population source: one row per entity
    pop_table = decisions.get("population", {}).get("table")
    if pop_table is None:
        cands = [t for t, d in sources.items() if t != fact_table and sorted(d.get("primary_key", [])) == sorted(keys)]
        cands.sort(key=lambda t: not any(h in t for h in REGISTRY_HINTS))
        if cands:
            pop_table = r.assume("population.table", cands[0], "A provided table whose primary key equals the entity keys")
        else:
            r.ask("population.table", f"Which table lists every {entity_id} entity (one row per {', '.join(keys)}, with a registration/creation time)? The fact table cannot be the population: it has many rows per entity.", list(sources))
            return {"project": None, "report": r.to_dict()}
    pop = sources[pop_table]
    pop_sid = _ident(_decide(decisions, f"source_ids.{pop_table}", _table_parts(pop_table)[1], "Logical source id from the table name", r))
    pop_event = decisions.get("population", {}).get("event_time") or next((c["name"] for c in pop["columns"] if re.search(r"(registered|created|signup|joined|opened)_(at|ts|time)", c["name"])), None)
    if pop_event is None:
        r.ask("population.event_time", f"Which {pop_table} column says when the entity joined the population?", [c["name"] for c in pop["columns"]])
        return {"project": None, "report": r.to_dict()}
    def avail_for(t: str, d: dict, event: str) -> str:
        explicit = decisions.get("sources", {}).get(t, {}).get("available_time")
        if explicit:
            return explicit
        g = _guess_available(d)
        if g:
            return r.assume(f"sources.{t}.available_time", g, "Column name suggests ingestion/availability time")
        r.ask(f"sources.{t}.available_time", f"{t} has no availability (ingestion) timestamp. Without one, late-arriving rows leak into history: replay is not reproducible. Using the event time instead is an assumption.", blocking=False)
        return r.assume(f"sources.{t}.available_time", event, "No availability column: rows assumed visible at their event time (NOT point-in-time safe)")
    fact_avail = avail_for(fact_table, fact, w.event_column)
    pop_avail = avail_for(pop_table, pop, pop_event)
    fact_pk = decisions.get("sources", {}).get(fact_table, {}).get("primary_key") or fact.get("primary_key") or []
    if not fact_pk:
        r.ask(f"sources.{fact_table}.primary_key", f"What uniquely identifies a row of {fact_table}? Duplicates are rejected by the spec unless declared.")
        return {"project": None, "report": r.to_dict()}
    earliest = _decide(decisions, "earliest_as_of", "2026-01-01T00:00:00Z", "Earliest reproducible instant; set to the start of retained source history", r)
    fs_id = _ident(_decide(decisions, "feature_set.id", _table_parts(target["table"])[1], "Feature set named after the legacy table", r))
    alias = _ident(analysis.fact_alias if len(analysis.fact_alias) > 2 else fact_sid)
    amap = {analysis.fact_alias: alias}
    shared_filter = None
    for f in analysis.filters:
        f = _rename_inputs(f, amap)
        shared_filter = f if shared_filter is None else {"kind": "call", "operator": "and", "args": [shared_filter, f]}
    used_cols = set(keys) | {w.event_column} | _cols_of(analysis.filters, set())
    features: list[dict] = []
    fid = lambda name: f"{entity_id}.{_ident(name)}"  # noqa: E731
    key_lineage = [{"source": fact_sid, "column": k, "roles": ["entity_key"], "description": f"Entity key {k}."} for k in keys]
    pop_lineage = [{"source": pop_sid, "column": k, "roles": ["entity_key", "population"], "description": f"Population key {k}."} for k in keys] + [
        {"source": pop_sid, "column": pop_event, "roles": ["event_time", "population"], "description": "Population membership time."},
        {"source": pop_sid, "column": pop_avail, "roles": ["availability_time", "population"], "description": "Population availability time."},
    ]
    lookback_of: dict[str, int] = {}
    window_of: dict[str, dict] = {}
    for o in analysis.outputs:
        if o.kind not in ROW_KINDS + ("expression",):
            continue
        col = describe.column(target, o.name)
        try:
            out_ty = describe.logical_type(col["type"]) if col else None
        except ValueError:
            out_ty = None
        if o.kind in ROW_KINDS:
            ow = windows[o.name]
            window_of[o.name] = {"lookback_seconds": ow.lookback_seconds, "lower": ow.lower, "upper": ow.upper or "exclusive"}
            value = _rename_inputs(o.value, amap)
            vcols = _cols_of(value, set())
            fcols = _cols_of(o.filter, set())
            ocols = {x["column"] for x in o.order_by or []}
            used_cols |= vcols | fcols | ocols
            filt = _rename_inputs(o.filter, amap)
            if shared_filter is not None:
                filt = shared_filter if filt is None else {"kind": "call", "operator": "and", "args": [shared_filter, filt]}
            common = {"input": alias, "joins": [], "group_by": [{"input": alias, "column": k, "entity_key": k} for k in keys], "filter": filt}
            if o.kind == "recency":
                out_ty = "integer"
                empty = {"action": "null"}
                transform = {"kind": "recency", **common, "event_time": o.event_column, "unit": "second", "rounding": "floor"}
                missing_null_input = "propagate"
            elif o.kind == "lookup":
                if out_ty is None:
                    out_ty = describe.logical_type(describe.column(fact, next(iter(vcols)))["type"]) if vcols else "string"
                empty = {"action": "null"}
                transform = {"kind": "lookup", **common, "value": value, "order_by": [{**x, "input": amap.get(x["input"], x["input"])} for x in o.order_by], "on_tie": "error"}
                missing_null_input = "propagate"
            else:
                if out_ty is None:
                    vt = describe.logical_type(describe.column(fact, next(iter(vcols)))["type"]) if vcols else "integer"
                    out_ty = {"count": "integer", "count_distinct": "integer", "mean": "number"}.get(o.operation, vt)
                if o.operation in ("count", "count_distinct") or o.coalesce_zero:
                    empty = {"action": "value", "value": 0 if out_ty == "integer" else 0.0}
                else:
                    empty = {"action": "null"}
                transform = {
                    "kind": "aggregate", **common, "operation": o.operation,
                    "value": value if o.operation not in ("count", "count_distinct") else None,
                    "distinct_by": [value] if o.operation == "count_distinct" else [], "nulls": "ignore",
                }
                missing_null_input = "ignore"
            ordering = ocols - {w.event_column}
            lineage_sources = key_lineage + [
                {"source": fact_sid, "column": w.event_column, "roles": ["event_time"] + (["ordering"] if w.event_column in ocols else []), "description": "Event time bounding the window."},
                {"source": fact_sid, "column": fact_avail, "roles": ["availability_time"], "description": "Availability cutoff."},
            ] + [{"source": fact_sid, "column": c, "roles": ["value"], "description": f"{'Looked-up' if o.kind == 'lookup' else 'Aggregated'} value {c}."} for c in sorted(vcols)] + [
                {"source": fact_sid, "column": c, "roles": ["ordering"], "description": f"Lookup ordering {c}."} for c in sorted(ordering)
            ] + [
                {"source": fact_sid, "column": c, "roles": ["filter"], "description": f"Filter column {c}."} for c in sorted(fcols | _cols_of(shared_filter, set()))
            ] + pop_lineage
            inputs = [{"alias": alias, "source": fact_sid, "columns": sorted(set(keys) | {w.event_column, fact_avail} | vcols | fcols | ocols | _cols_of(shared_filter, set()) | set(fact_pk))}]
            deps: list = []
        else:
            transform = {"kind": "expression", "expression": _rename_feature_refs(o.expression, fid)}
            out_ty = out_ty or "number"
            empty = {"action": "null"}
            lineage_sources = pop_lineage
            inputs = []
            deps = [{"feature": fid(d), "version": "1.0.0", "alignment": "same_entity_as_of"} for d in o.depends_on]
            missing_null_input = "propagate"
            # A derived feature spans the widest window of its inputs.
            window_of[o.name] = max((window_of[d] for d in o.depends_on if d in window_of), key=lambda x: x["lookback_seconds"], default={"lookback_seconds": w.lookback_seconds, "lower": w.lower, "upper": w.upper or "exclusive"})
        lookback_of[o.name] = window_of[o.name]["lookback_seconds"]
        desc = (col or {}).get("comment") or f"Reverse-engineered from `{o.sql}` in the legacy feature code."
        feature = {
            "id": fid(o.name), "version": "1.0.0", "name": o.name.replace("_", " ").capitalize(),
            "description": desc, "owner": owner, "entity": entity_id,
            "inputs": inputs, "dependencies": deps,
            "time": {
                "timezone": "UTC", "knowledge_mode": "source_as_known",
                "window": window_of[o.name],
                "availability": {"cutoff": "as_of", "upper": "inclusive"},
                "publication": {"delay_seconds": 0},
            },
            "transform": transform,
            "output": {"type": out_ty, "nullable": True, "unit": (col or {}).get("tags", {}).get("unit") or ("second" if o.kind == "recency" else None), "description": desc, "constraints": {"minimum": 0} if o.kind == "recency" else {}, **({"numeric_tolerance": {"absolute": 0, "relative": 1e-9 if out_ty == "number" else 0}} if out_ty in ("integer", "number") else {})},
            "lineage": {"sources": lineage_sources, "dependencies": [{"feature": d["feature"], "version": "1.0.0", "description": "Input of the derived expression."} for d in deps], "code": None},
            "missingness": {"empty": empty, "null_input": missing_null_input, "insufficient_history": "null", "source_unavailable": "null", "execution_failure": "error"},
            "quality": {"checks": [{"kind": "finite", "severity": "error"}] if out_ty in ("integer", "number") else [], "on_violation": "error"},
            "history": {"required_seconds": lookback_of[o.name], "coverage": "contiguous", "earliest_as_of": earliest},
            "acceptance": {"suites": [f"{fs_id}_characterization"], "description": "Structural and characterization cases drafted by featlas_reverse; goldens of characterization cases are the legacy table's outputs."},
        }
        features.append(feature)
        r.mapping[o.name] = {"feature": feature["id"], "version": "1.0.0", "legacy_sql": o.sql}
    for o in analysis.outputs:
        if o.kind == "key":
            r.mapping[o.name] = {"entity_key": o.columns[0][1]}
        elif o.kind == "as_of":
            r.mapping[o.name] = {"as_of": True}
    for u in analysis.unsupported:
        if u.get("name"):
            r.mapping[u["name"]] = {"unsupported": u.get("reason", "")}
    if not features:
        r.ask("features", "No output could be translated; see unsupported.")
        return {"project": None, "report": r.to_dict()}
    entity = {
        "id": entity_id, "version": "1.0.0", "name": entity_id.replace("_", " ").capitalize(),
        "description": f"One {entity_id} as identified by {', '.join(keys)} in {target['table']}.", "owner": owner,
        "keys": [{"name": k, "type": describe.logical_type(describe.column(pop, k)["type"]), "source_column": k, "is_tenant": k == tenant, "description": f"{k} (from {pop_table})."} for k in keys],
        "population": {"source": pop_sid, "mode": "as_of_keys", "event_time_upper": "inclusive", "availability_upper": "inclusive", "deletion_policy": "exclude_deleted"},
    }
    srcs = [
        _source(pop_sid, pop, set(keys), pop_event, pop_avail, pop.get("primary_key") or keys, earliest, owner),
        _source(fact_sid, fact, used_cols | set(fact_pk), w.event_column, fact_avail, fact_pk, earliest, owner),
    ]
    feature_set = {"id": fs_id, "version": "1.0.0", "name": fs_id.replace("_", " ").capitalize(), "description": f"Reverse-engineered from {target['table']}.", "owner": owner, "entity": entity_id, "features": [{"id": f["id"], "version": "1.0.0"} for f in features], "publication": "atomic"}
    project = {
        "spec_version": "0.2.0", "kind": "FeatureEngineeringProject",
        "metadata": {"id": _ident(_decide(decisions, "project.id", f"{fs_id}_reverse", "Project id", r)), "name": f"{target['table']} (reverse-engineered)", "version": "0.1.0", "description": f"Specification drafted by featlas_reverse from {target['table']} and its code; review every assumption in the drafting report before marking it reviewed.", "owners": [owner], "status": "draft", "tags": ["reverse-engineered"]},
        "entities": [entity], "sources": srcs, "relationships": [], "features": features, "feature_sets": [feature_set],
        "consumers": [_consumer(fs_id, entity_id, keys, owner)],
        "deployments": [],
        "acceptance_suites": [_suite(fs_id, features, keys, pop_sid, fact_sid, earliest, w, decisions, r, {x["id"]: [c["name"] for c in x["columns"]] for x in srcs})],
    }
    project["deployments"] = [_deployment(decisions, fs_id, entity, srcs, {pop_sid: pop_table, fact_sid: fact_table}, features, target, owner, r)]
    return {"project": project, "report": r.to_dict()}


def _rename_feature_refs(e: Any, fid) -> Any:
    if isinstance(e, dict):
        out = {k: _rename_feature_refs(v, fid) for k, v in e.items()}
        if out.get("kind") == "feature":
            out["feature"] = fid(out["feature"])
        return out
    if isinstance(e, list):
        return [_rename_feature_refs(x, fid) for x in e]
    return e


def _consumer(fs_id: str, entity_id: str, keys: list[str], owner: str) -> dict:
    return {
        "id": f"{fs_id}_batch", "version": "1.0.0", "name": f"{fs_id} batch", "description": "Batch reads of the regenerated feature table (source-as-known).", "owner": owner,
        "feature_set": {"feature_set": fs_id, "version": "1.0.0"},
        "entity_inputs": [{"entity_key": k, "input_field": k} for k in keys],
        "time_inputs": {"as_of": "as_of", "requested_at": "requested_at"},
        "label": None,
        "selection": {"mode": "source_as_known", "tie_breaker": "sequence"},
        "max_staleness_seconds": 86400, "missing_value_policy": "preserve_null",
    }


def _status(sid: str, start: str, end: str) -> dict:
    return {"source": sid, "state": "available", "coverage_start": start, "coverage_end": end}


def _suite(fs_id, features, keys, pop_sid, fact_sid, earliest, w, decisions, r: Report, source_columns: dict[str, list[str]]) -> dict:
    """Structural case (empty population) for every feature, plus characterization samples."""
    as_of = decisions.get("structural_as_of") or "2026-06-01T00:00:00Z"
    caps = CORE_CAPS
    cases = []
    for f in features:
        cases.append({
            "id": f"{f['id'].split('.', 1)[1]}_empty_population", "name": f"{f['name']}: empty population",
            "description": "Structural case: no registered entity yields no rows (not a zero).",
            "feature": {"id": f["id"], "version": "1.0.0"}, "as_of": as_of, "population_rows": [],
            "source_rows": {pop_sid: [], fact_sid: []},
            "context": {"mode": "source_as_known", "capabilities": caps, "source_status": [_status(pop_sid, earliest, "2026-12-31T00:00:00Z"), _status(fact_sid, earliest, "2026-12-31T00:00:00Z")]},
            "expected": {"rows": []},
        })
    samples = decisions.get("samples") or []
    if not samples:
        r.ask("samples", "No characterization samples: provide, for a few entities and one as_of, the source rows and the legacy table's output row (see the skill). Without them only structural cases exist.", blocking=False)
    for i, s in enumerate(samples):
        for f in features:
            legacy = f["id"].split(".", 1)[1]
            rows = []
            for row in s["legacy_rows"]:
                if legacy not in row:
                    continue
                state = row.get(f"fl_state_{legacy}")
                if state is None:
                    # Derived features: the state follows from the inputs' (D4); assert only when every
                    # input was ok, otherwise leave the feature out of this characterization case.
                    deps = [d["feature"].split(".", 1)[1] for d in f.get("dependencies", [])]
                    if not deps or any(row.get(f"fl_state_{d}") != "ok" for d in deps):
                        continue
                    state = "ok"
                rows.append({"entity_keys": {k: row[k] for k in keys}, "value": row.get(legacy), "state": state})
            if len(rows) != len(s["legacy_rows"]):
                r.assume(f"samples[{i}].{legacy}", "omitted", "Expected state is not derivable from the legacy code for every sampled entity")
                continue
            cases.append({
                "id": f"{legacy}_characterization_{i}", "name": f"{f['name']}: legacy characterization {i}",
                "description": "Characterization: the expected values are the legacy feature table's outputs for these entities at this as_of (SEM-TEST-002: independent implementation). Review states (ok/empty) by hand.",
                "feature": {"id": f["id"], "version": "1.0.0"}, "as_of": s["as_of"],
                "population_rows": [{k: row[k] for k in keys} for row in s["legacy_rows"]],
                # Sample rows are projected onto the drafted source contract (only the columns the code reads).
                "source_rows": {sid: [{c: row.get(c) for c in source_columns[sid]} for row in s["source_rows"][sid]] for sid in (pop_sid, fact_sid)},
                "context": {"mode": "source_as_known", "capabilities": caps, "source_status": [_status(pop_sid, earliest, s.get("coverage_end", s["as_of"])), _status(fact_sid, earliest, s.get("coverage_end", s["as_of"]))]},
                "expected": {"rows": rows},
            })
    return {"id": f"{fs_id}_characterization", "version": "1.0.0", "name": f"{fs_id} characterization", "description": "Drafted by featlas_reverse: structural cases plus characterization cases from the legacy table.", "runner": "reference", "cases": cases}


def _deployment(decisions, fs_id, entity, srcs, physical, features, target, owner, r: Report) -> dict:
    default_target = target["table"] + "_featlas"
    out_table = _decide(decisions, "target.table", default_target, "Regenerated table written next to the legacy one (the legacy table is kept for parity checks)", r)
    ns, name = _table_parts(out_table)
    keys = [k["name"] for k in entity["keys"]]
    caps = CORE_CAPS + ["core.source_read", "core.atomic_publication"]
    roles = {"core.source_read": "catalog", "core.atomic_publication": "offline_store"}
    return {
        "id": _ident(_decide(decisions, "deployment.id", f"{fs_id}_databricks", "Deployment id", r)), "version": "1.0.0", "name": f"{fs_id} on Databricks",
        "description": "Drafted by featlas_reverse; nothing is provisioned or published by this document.", "owner": owner,
        "environment": _decide(decisions, "deployment.environment", "development", "Environment label", r),
        "adapters": [{"id": "databricks_platform", "implementation": "dev.featlas:databricks", "version": "0.1.0", "roles": ["compute", "catalog", "offline_store", "orchestrator"], "configuration": {"connection_profile": "default", "feature_api": "uc_feature_tables"}}],
        "execution": {"compute": "databricks_platform", "catalog": "databricks_platform", "orchestrator": None},
        "source_bindings": [{
            "source": s["id"], "version": "1.0.0", "adapter": "databricks_platform", "read_only": True,
            "location": {"kind": "table", "namespace": _table_parts(physical[s["id"]])[0], "name": _table_parts(physical[s["id"]])[1], "format": "delta"},
            "column_mapping": {c["name"]: c["name"] for c in s["columns"]},
        } for s in srcs],
        "output_targets": [{
            "feature_set": fs_id, "version": "1.0.0", "adapter": "databricks_platform", "role": "offline_store", "write_mode": "merge",
            "location": {"kind": "table", "namespace": ns, "name": name, "format": "delta"},
            "entity_key_mapping": {k: k for k in keys},
            "feature_mapping": [{"feature": f["id"], "version": "1.0.0", "column": f["id"].split(".", 1)[1], "state_column": f["id"].split(".", 1)[1] + "_state"} for f in features],
            "metadata_columns": {"as_of": "feature_as_of", "published_at": "published_at", "publication_sequence": "publication_sequence", "publication_id": "publication_id", "materialization_id": "materialization_id"},
        }],
        "schedule": {"mode": "manual"},
        "refresh": {"mode": "full", "lookback_seconds": max(f["time"]["window"]["lookback_seconds"] for f in features if f["time"].get("window")), "late_arrival_policy": "recompute_affected"},
        "publication": {"atomicity": "feature_set", "visibility": "after_success", "observed_timestamp_required": True, "surface": "offline_snapshot"},
        "backfill": {"allowed": False},
        "runtime": {"sdk_policy": "pin_and_probe_at_implementation", "required_capabilities": [{"adapter": "databricks_platform", "role": roles.get(c, "compute"), "capability": c, "reason": "Required by the drafted features."} for c in caps]},
        "slo": {"max_compute_seconds": 3600, "max_staleness_seconds": 86400, "max_error_rate": 0.0},
        "budget": {"currency": "USD", "max_daily_cost": 10, "max_scan_bytes": 107374182400, "max_output_rows": 100000000, "max_memory_mib": 16384, "max_concurrent_runs": 1},
        "monitoring": {"metrics": ["freshness", "row_count", "null_rate", "quality_failures"], "alert_owner": owner},
        "access": {"read_principal": "featlas.reader", "write_principal": "featlas.publisher", "required_permissions": [{"resource": s["id"], "privilege": "read"} for s in srcs] + [{"resource": fs_id, "privilege": "write"}], "purpose": "Regenerate the reverse-engineered feature table."},
        "output_retention_seconds": 31536000,
        "consumer_bindings": [{"consumer": f"{fs_id}_batch", "version": "1.0.0", "adapter": "databricks_platform", "role": "offline_store"}],
        "portability": {"mode": "portable", "native_bindings": []},
    }


def to_yaml(project: dict) -> str:
    """YAML in the restricted profile (no anchors/tags; strings quoted where YAML 1.1 would retype them)."""
    import yaml

    class Dumper(yaml.SafeDumper):
        def ignore_aliases(self, data):  # noqa: D401
            return True

    def str_rep(d, s):
        style = "'" if re.fullmatch(r"(?i)(y|n|yes|no|on|off|true|false|null|~|[-+]?[\d._:]+([eE][-+]?\d+)?|\d{4}-\d\d-\d\d.*)", s) or s == "" else None
        return d.represent_scalar("tag:yaml.org,2002:str", s, style=style)

    Dumper.add_representer(str, str_rep)
    return yaml.dump(copy.deepcopy(project), Dumper=Dumper, sort_keys=False, allow_unicode=True, width=100)
