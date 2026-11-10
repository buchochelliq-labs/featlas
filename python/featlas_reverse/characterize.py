"""Characterization samples: the legacy table's own outputs as acceptance goldens.

``legacy_sample`` runs the legacy SQL (unchanged) in a Spark session over small source samples
registered as temp views, plus one companion query from the same FROM/WHERE/GROUP BY that counts,
per aggregate output, the rows its filter keeps. That count decides the expected state
(``empty`` when no row qualifies, else ``ok``) from the legacy code itself, independently of the
featlas core. The result is ``decisions["samples"]`` for ``draft.draft_spec``.

Only entities present in the legacy output are sampled: legacy tables usually omit entities with
no events, while the specification's population includes them (reported as an assumption).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import sqlglot
from sqlglot import exp

from .analyze import Analysis


def _bind(sql: str, analysis: Analysis, views: dict[str, str]) -> str:
    tree = sqlglot.parse_one(sql, read=analysis.dialect)
    for t in tree.find_all(exp.Table):
        fq = ".".join(p for p in (t.catalog, t.db, t.name) if p)
        if fq in views:
            t.set("catalog", None)
            t.set("db", None)
            t.set("this", exp.to_identifier(views[fq]))
    return tree.sql(dialect=analysis.dialect)


def eligibility_sql(analysis: Analysis) -> str:
    tree = sqlglot.parse_one(analysis.sql, read=analysis.dialect)
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    keys = [exp.column(c, table=a) for a, c in analysis.group_by]
    counts = []
    for o in analysis.outputs:
        if o.kind != "aggregate":
            continue
        cond = o.filter_sql or "TRUE"
        counts.append(f"COUNT(CASE WHEN {cond} THEN 1 END) AS `fl_eligible_{o.name}`")
    sel = ", ".join([k.sql(dialect=analysis.dialect) for k in keys] + counts)
    where = select.args.get("where")
    from_ = select.args.get("from_") or select.args.get("from")
    joins = " ".join(j.sql(dialect=analysis.dialect) for j in select.args.get("joins") or [])
    group = ", ".join(k.sql(dialect=analysis.dialect) for k in keys)
    return f"SELECT {sel} {from_.sql(dialect=analysis.dialect)} {joins} {where.sql(dialect=analysis.dialect) if where else ''} GROUP BY {group}"


def _iso(v: Any) -> Any:
    if isinstance(v, datetime):
        v = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ").replace(".000000Z", "Z")
    return v


def legacy_sample(spark, analysis: Analysis, views: dict[str, str], as_of: str, param: str = "as_of") -> list[dict]:
    """Rows of the legacy table at ``as_of`` with ``fl_state_<column>`` for each aggregate output."""
    at = datetime.fromisoformat(as_of.replace("Z", "+00:00")).replace(tzinfo=None)
    rows = [r.asDict() for r in spark.sql(_bind(analysis.sql, analysis, views), args={param: at}).collect()]
    elig = {tuple(r[c] for _, c in analysis.group_by): r.asDict() for r in spark.sql(_bind(eligibility_sql(analysis), analysis, views), args={param: at}).collect()}
    out = []
    by = {o.name: o for o in analysis.outputs}
    for r in rows:
        key = tuple(r[c] for _, c in analysis.group_by)
        e = elig.get(key, {})
        row = {k: _iso(v) for k, v in r.items()}
        for name, o in by.items():
            if o.kind == "aggregate":
                row[f"fl_state_{name}"] = "ok" if e.get(f"fl_eligible_{name}", 0) else "empty"
        out.append(row)
    return out
