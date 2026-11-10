"""Split one wide feature set into several smaller feature tables.

``analyze_split`` computes, for every candidate strategy, the groups it would produce and
deterministic metrics; it recommends one but does not decide — the agent (see
``.claude/skills/featlas-split-wide-table``) weighs the metrics against what it knows about the
organisation and calls ``apply_split`` with an explicit assignment.

Strategies evaluated:

* ``tag:<name>`` — a column tag on the legacy table (``domain``, ``business_unit``, ``owner``...);
* ``filter_family`` — features sharing the same filter columns (e.g. every ``channel`` split);
* ``value_family`` — features aggregating the same source column;
* ``window`` — by lookback;
* ``source`` — by the set of source tables read.

Derived (expression) features may depend on features published by another split table: the
generated code recomputes such inputs from the same sources and as_of inside the dependent table's
job (no cross-table read, no skew between tables). Each crossing edge is counted
(``cross_set_dependencies``) because it duplicates compute; ``apply_split(...,
colocate_dependencies=True)`` instead rejects assignments that separate a feature from its inputs.
"""

from __future__ import annotations

import copy
import itertools
import re
from collections import defaultdict
from typing import Any

from . import describe


def _cols(e: Any, acc: set) -> set:
    if isinstance(e, dict):
        if e.get("kind") == "column":
            acc.add(e["column"])
        for v in e.values():
            _cols(v, acc)
    elif isinstance(e, list):
        for v in e:
            _cols(v, acc)
    return acc


def feature_profile(project: dict, legacy: dict | None, feature_set: str) -> list[dict]:
    """Per-feature facts used by the strategies (tags come from the legacy table description)."""
    fs = next(f for f in project["feature_sets"] if f["id"] == feature_set)
    members = {m["id"] for m in fs["features"]}
    out = []
    shared = None
    for f in project["features"]:
        if f["id"] not in members:
            continue
        t = f["transform"]
        legacy_col = f["id"].split(".", 1)[1]
        tags = {}
        if legacy:
            c = describe.column(legacy, legacy_col)
            tags = dict((c or {}).get("tags", {}))
        filt = _cols(t.get("filter"), set()) if t["kind"] == "aggregate" else set()
        out.append({
            "feature": f["id"], "legacy_column": legacy_col, "kind": t["kind"], "operation": t.get("operation"),
            "tags": tags, "owner": f.get("owner"),
            "sources": sorted({i["source"] for i in f.get("inputs", [])}),
            "value_columns": sorted(_cols(t.get("value"), set()) | _cols(t.get("distinct_by"), set())) if t["kind"] == "aggregate" else [],
            "filter_columns": sorted(filt),
            "lookback_seconds": (f["time"].get("window") or {}).get("lookback_seconds"),
            "depends_on": [d["feature"] for d in f.get("dependencies", [])],
        })
    # Filter columns shared by every aggregate (e.g. is_test) carry no family signal.
    aggs = [p for p in out if p["kind"] == "aggregate"]
    if aggs:
        shared = set.intersection(*(set(p["filter_columns"]) for p in aggs))
        for p in out:
            p["family_filter_columns"] = sorted(set(p["filter_columns"]) - shared)
    return out


def _jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a | b else 1.0


def _metrics(groups: dict[str, str], prof: list[dict]) -> dict:
    by_group: dict[str, list[dict]] = defaultdict(list)
    for p in prof:
        by_group[groups[p["feature"]]].append(p)
    sizes = sorted(len(v) for v in by_group.values())
    cohesion = []
    owners_single = 0
    for members in by_group.values():
        if len(members) > 1:
            cohesion.append(sum(_jaccard(set(a["value_columns"] + a["family_filter_columns"]), set(b["value_columns"] + b["family_filter_columns"])) for a, b in itertools.combinations(members, 2)) / (len(members) * (len(members) - 1) / 2))
        bus = {m["tags"].get("business_unit") or m["owner"] for m in members}
        owners_single += len(bus) == 1
    crossing = [{"feature": p["feature"], "depends_on": d, "from_group": groups[d], "to_group": groups[p["feature"]]} for p in prof for d in p["depends_on"] if d in groups and groups[d] != groups[p["feature"]]]
    return {
        "groups": len(by_group), "sizes": sizes, "smallest": sizes[0], "largest": sizes[-1],
        "singletons": sum(1 for s in sizes if s == 1),
        "cross_set_dependencies": len(crossing), "recomputed": crossing,
        "cohesion": round(sum(cohesion) / len(cohesion), 3) if cohesion else 1.0,
        "single_owner_groups": owners_single,
    }


def _strategy(name: str, key_fn, prof: list[dict]) -> dict:
    raw = {}
    unassigned = []
    for p in prof:
        k = key_fn(p)
        if k is None:
            unassigned.append(p["feature"])
            k = "unassigned"
        raw[p["feature"]] = re.sub(r"[^a-z0-9_]+", "_", str(k).lower()).strip("_") or "unassigned"
    # Derived features with no own key follow their dependencies.
    for p in prof:
        if p["kind"] == "expression" and raw[p["feature"]] == "unassigned" and p["depends_on"]:
            raw[p["feature"]] = raw.get(p["depends_on"][0], "unassigned")
            if p["feature"] in unassigned:
                unassigned.remove(p["feature"])
    m = _metrics(raw, prof)
    m["unassigned"] = unassigned
    return {"strategy": name, "assignment": raw, "metrics": m}


def _score(s: dict, n: int, max_groups: int, min_size: int) -> tuple[float, list[str]]:
    m = s["metrics"]
    why = []
    score = 0.0
    if m["groups"] < 2:
        return -1e9, ["does not split"]
    if m["groups"] > max_groups:
        score -= 2 * (m["groups"] - max_groups)
        why.append(f"{m['groups']} groups exceed the target maximum {max_groups}")
    if m["unassigned"]:
        score -= 3 * len(m["unassigned"])
        why.append(f"{len(m['unassigned'])} feature(s) lack the grouping attribute")
    if m["smallest"] < min_size:
        score -= 1.5 * sum(1 for x in m["sizes"] if x < min_size)
        why.append(f"{sum(1 for x in m['sizes'] if x < min_size)} group(s) smaller than {min_size}")
    score -= 0.25 * m["cross_set_dependencies"]
    if m["cross_set_dependencies"]:
        why.append(f"{m['cross_set_dependencies']} derived input(s) recomputed in another table's job")
    score += 2.0 * m["cohesion"] + 1.5 * (m["single_owner_groups"] / m["groups"])
    why.append(f"cohesion {m['cohesion']}, {m['single_owner_groups']}/{m['groups']} groups with one owner/business unit")
    return round(score, 3), why


def analyze_split(project: dict, feature_set: str, legacy: dict | None = None, max_groups: int = 6, min_size: int = 2) -> dict:
    prof = feature_profile(project, legacy, feature_set)
    strategies = []
    tag_names = sorted({k for p in prof for k in p["tags"]})
    for t in tag_names:
        strategies.append(_strategy(f"tag:{t}", lambda p, t=t: p["tags"].get(t), prof))
    strategies.append(_strategy("filter_family", lambda p: "_".join(p.get("family_filter_columns") or []) or ("totals" if p["kind"] == "aggregate" else None), prof))
    strategies.append(_strategy("value_family", lambda p: "_".join(p["value_columns"]) or ("counts" if p["kind"] == "aggregate" else None), prof))
    strategies.append(_strategy("window", lambda p: f"{p['lookback_seconds'] // 86400}d" if p["lookback_seconds"] else None, prof))
    strategies.append(_strategy("source", lambda p: "_".join(p["sources"]) or None, prof))
    for s in strategies:
        s["score"], s["reasons"] = _score(s, len(prof), max_groups, min_size)
    strategies.sort(key=lambda s: -s["score"])
    return {
        "feature_set": feature_set, "features": len(prof), "profile": prof, "strategies": strategies,
        "recommended": strategies[0]["strategy"] if strategies and strategies[0]["score"] > -1e8 else None,
        "decision_required": "The recommendation is a heuristic over the metrics; the agent must confirm it against ownership, consumers and SLAs (see the split skill) and call apply_split.",
    }


def apply_split(project: dict, feature_set: str, assignment: dict[str, str], tables: dict[str, str] | None = None, colocate_dependencies: bool = False) -> dict:
    """Return ``{"project", "mapping", "compatibility_view_sql"}`` with one feature set per group."""
    if set(assignment) - {m["id"] for f in project["feature_sets"] if f["id"] == feature_set for m in f["features"]}:
        raise ValueError("Assignment names features that are not members of the feature set")
    p = copy.deepcopy(project)
    fs = next(f for f in p["feature_sets"] if f["id"] == feature_set)
    members = [m["id"] for m in fs["features"]]
    missing = [m for m in members if m not in assignment]
    if missing:
        raise ValueError(f"Assignment misses {missing}")
    feats = {f["id"]: f for f in p["features"]}
    for fid in members:
        for d in feats[fid].get("dependencies", []):
            if colocate_dependencies and assignment.get(d["feature"]) != assignment[fid]:
                raise ValueError(f"{fid} depends on {d['feature']}, which is assigned to another group ({assignment.get(d['feature'])} vs {assignment[fid]}); a derived feature must live with its inputs")
    groups = sorted(set(assignment[m] for m in members))
    dep = p["deployments"][0]
    target = next(t for t in dep["output_targets"] if t["feature_set"] == feature_set)
    base_ns, base_name = target["location"]["namespace"], target["location"]["name"]
    tables = tables or {}
    new_sets, new_targets, new_consumers, new_bindings, mapping = [], [], [], [], {}
    consumer = next(c for c in p["consumers"] if c["feature_set"]["feature_set"] == feature_set)
    for g in groups:
        gid = f"{feature_set}_{g}"
        ids = [m for m in members if assignment[m] == g]
        fqn = tables.get(g) or ".".join(base_ns + [f"{base_name}_{g}"])
        ns, name = fqn.split(".")[:-1], fqn.split(".")[-1]
        new_sets.append({**fs, "id": gid, "name": f"{fs['name']} — {g}", "description": f"{fs['description']} Split group '{g}'.", "features": [{"id": i, "version": "1.0.0"} for i in ids]})
        t = copy.deepcopy(target)
        t.update(feature_set=gid, location={**target["location"], "namespace": ns, "name": name}, feature_mapping=[m for m in target["feature_mapping"] if m["feature"] in ids])
        new_targets.append(t)
        c = copy.deepcopy(consumer)
        c.update(id=f"{gid}_batch", name=f"{gid} batch", feature_set={"feature_set": gid, "version": "1.0.0"})
        new_consumers.append(c)
        new_bindings.append({"consumer": c["id"], "version": "1.0.0", "adapter": target["adapter"], "role": target["role"]})
        for m in t["feature_mapping"]:
            mapping[m["column"]] = {"table": fqn, "column": m["column"], "state_column": m["state_column"], "feature": m["feature"], "group": g}
    p["feature_sets"] = [f for f in p["feature_sets"] if f["id"] != feature_set] + new_sets
    dep["output_targets"] = [t for t in dep["output_targets"] if t["feature_set"] != feature_set] + new_targets
    p["consumers"] = [c for c in p["consumers"] if c["id"] != consumer["id"]] + new_consumers
    dep["consumer_bindings"] = [b for b in dep["consumer_bindings"] if b["consumer"] != consumer["id"]] + new_bindings
    dep["access"]["required_permissions"] = [x for x in dep["access"]["required_permissions"] if x["resource"] != feature_set] + [{"resource": s["id"], "privilege": "write"} for s in new_sets]
    keys = list(target["entity_key_mapping"].values())
    asof = target["metadata_columns"]["as_of"]
    view = _compat_view(fs["id"], target, new_targets, mapping, keys, asof)
    return {"project": p, "mapping": mapping, "compatibility_view_sql": view, "groups": groups}


def _q(x: str) -> str:
    return f"`{x}`"


def _fq(t: dict) -> str:
    return ".".join(_q(x) for x in t["location"]["namespace"] + [t["location"]["name"]])


def _compat_view(fs_id, wide_target, targets, mapping, keys, asof) -> str:
    """A view with the wide table's columns, assembled from the split tables (for consumers that
    still read the wide shape during migration). Every split table must publish the same as_of."""
    first = targets[0]
    sel = [f"t0.{_q(k)}" for k in keys] + [f"t0.{_q(asof)}"]
    joins = []
    for i, t in enumerate(targets):
        if i:
            on = " AND ".join(f"t{i}.{_q(k)} = t0.{_q(k)}" for k in keys + [asof])
            joins.append(f"FULL OUTER JOIN {_fq(t)} t{i} ON {on}")
        for m in t["feature_mapping"]:
            sel.append(f"t{i}.{_q(m['column'])}")
    ns, name = wide_target["location"]["namespace"], wide_target["location"]["name"] + "_compat"
    return f"-- Compatibility view: the wide {fs_id} shape over the split tables.\nCREATE OR REPLACE VIEW {'.'.join(_q(x) for x in ns + [name])} AS\nSELECT {', '.join(sel)}\nFROM {_fq(first)} t0\n" + "\n".join(joins)
