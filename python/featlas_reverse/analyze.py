"""Static analysis of feature-table SQL (Databricks dialect) into candidate features.

Supported shape (one SELECT, the usual way a batch feature table is built)::

    SELECT <entity keys>, [<as_of>,] <aggregate or expression> AS <feature>, ...
    FROM <fact table> [alias] [JOIN <dimension> ON <equi-join>]
    WHERE <event time> >= <anchor> - INTERVAL n <unit> AND <event time> < <anchor> [AND <filters>]
    GROUP BY <entity keys>

Aggregates: COUNT(*), COUNT(x), COUNT(DISTINCT x), SUM, AVG, MIN, MAX, with filters written as
``FILTER (WHERE c)``, ``CASE WHEN c THEN x END`` or WHERE conjuncts; outputs that combine other
outputs (``a / b``, ``a / NULLIF(b, 0)``, ``COALESCE(...)``) become derived expression features.

Nothing is guessed silently: anything outside this shape is returned in ``unsupported`` with the
SQL fragment, and the caller (an agent or a person) must translate it or drop it.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

UNIT_SECONDS = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 86400, "WEEK": 604800}


@dataclass
class Window:
    alias: str
    event_column: str
    lookback_seconds: int | None
    lower: str  # inclusive | exclusive
    upper: str
    anchor: str


@dataclass
class Output:
    name: str
    kind: str  # key | as_of | aggregate | expression | passthrough
    sql: str
    operation: str | None = None
    input: str | None = None
    value: dict | None = None
    filter: dict | None = None
    expression: dict | None = None
    depends_on: list[str] = field(default_factory=list)
    columns: list[tuple[str, str]] = field(default_factory=list)  # (alias, column)
    coalesce_zero: bool = False
    # SQL (in the legacy dialect) of the rows this aggregate considers, for characterization.
    filter_sql: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class Analysis:
    sql: str
    dialect: str
    tables: dict[str, str]  # alias -> fully qualified table
    fact_alias: str
    joins: list[dict]
    group_by: list[tuple[str, str]]
    window: Window | None
    filters: list[dict]  # shared WHERE conjuncts as spec expressions
    outputs: list[Output]
    unsupported: list[dict]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["outputs"] = [asdict(o) for o in self.outputs]
        return d


class Unsupported(Exception):
    pass


def _alias_name(t: exp.Table) -> str:
    return t.alias_or_name


def _table_name(t: exp.Table) -> str:
    return ".".join(p for p in (t.catalog, t.db, t.name) if p)


def _col(c: exp.Column, default_alias: str) -> tuple[str, str]:
    return (c.table or default_alias, c.name)


def _literal(e: exp.Expression) -> dict:
    if isinstance(e, exp.Boolean):
        return {"kind": "literal", "type": "boolean", "value": bool(e.this)}
    if isinstance(e, exp.Null):
        raise Unsupported("bare NULL literal (spec literals are typed)")
    if isinstance(e, exp.Literal):
        if e.is_string:
            return {"kind": "literal", "type": "string", "value": e.this}
        text = e.this
        if re.fullmatch(r"-?\d+", text):
            return {"kind": "literal", "type": "integer", "value": int(text)}
        return {"kind": "literal", "type": "number", "value": float(text)}
    if isinstance(e, exp.Neg) and isinstance(e.this, exp.Literal):
        v = _literal(e.this)
        v["value"] = -v["value"]
        return v
    raise Unsupported(f"literal {e.sql()}")


BINARY = {
    exp.EQ: "equal",
    exp.NEQ: "not_equal",
    exp.LT: "less_than",
    exp.LTE: "less_or_equal",
    exp.GT: "greater_than",
    exp.GTE: "greater_or_equal",
    exp.And: "and",
    exp.Or: "or",
    exp.Add: "add",
    exp.Sub: "subtract",
    exp.Mul: "multiply",
    exp.Div: "divide",
}


def to_expression(e: exp.Expression, default_alias: str, columns: list, feature_ref=None) -> dict:
    """SQL expression -> spec expression AST (``$defs/expression``). Raises Unsupported."""
    if isinstance(e, exp.Paren):
        return to_expression(e.this, default_alias, columns, feature_ref)
    if isinstance(e, exp.Column):
        if feature_ref and not e.table and e.name in feature_ref:
            return feature_ref[e.name]
        a, c = _col(e, default_alias)
        columns.append((a, c))
        return {"kind": "column", "input": a, "column": c}
    if isinstance(e, (exp.Literal, exp.Boolean, exp.Neg)):
        return _literal(e)
    for cls, op in BINARY.items():
        if type(e) is cls:
            return {"kind": "call", "operator": op, "args": [to_expression(e.this, default_alias, columns, feature_ref), to_expression(e.expression, default_alias, columns, feature_ref)]}
    if isinstance(e, exp.Not):
        inner = e.this
        if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
            return {"kind": "call", "operator": "not", "args": [{"kind": "call", "operator": "is_null", "args": [to_expression(inner.this, default_alias, columns, feature_ref)]}]}
        return {"kind": "call", "operator": "not", "args": [to_expression(inner, default_alias, columns, feature_ref)]}
    if isinstance(e, exp.Is) and isinstance(e.expression, exp.Null):
        return {"kind": "call", "operator": "is_null", "args": [to_expression(e.this, default_alias, columns, feature_ref)]}
    if isinstance(e, exp.In) and e.expressions:
        left = to_expression(e.this, default_alias, columns, feature_ref)
        ors = [{"kind": "call", "operator": "equal", "args": [left, _literal(v)]} for v in e.expressions]
        out = ors[0]
        for o in ors[1:]:
            out = {"kind": "call", "operator": "or", "args": [out, o]}
        return out
    if isinstance(e, exp.Coalesce):
        args = [e.this] + list(e.expressions)
        return {"kind": "call", "operator": "coalesce", "args": [to_expression(a, default_alias, columns, feature_ref) for a in args]}
    if isinstance(e, exp.Abs):
        return {"kind": "call", "operator": "absolute", "args": [to_expression(e.this, default_alias, columns, feature_ref)]}
    if isinstance(e, exp.If):
        return {"kind": "call", "operator": "if_else", "args": [to_expression(e.this, default_alias, columns, feature_ref), to_expression(e.args["true"], default_alias, columns, feature_ref), to_expression(e.args["false"], default_alias, columns, feature_ref)]}
    if isinstance(e, exp.Case) and len(e.args.get("ifs") or []) == 1 and e.args.get("default") is not None:
        w = e.args["ifs"][0]
        return {"kind": "call", "operator": "if_else", "args": [to_expression(w.this, default_alias, columns, feature_ref), to_expression(w.args["true"], default_alias, columns, feature_ref), to_expression(e.args["default"], default_alias, columns, feature_ref)]}
    raise Unsupported(f"{type(e).__name__}: {e.sql()}")


def _conjuncts(e: exp.Expression | None) -> list[exp.Expression]:
    if e is None:
        return []
    if isinstance(e, exp.Paren):
        return _conjuncts(e.this)
    if isinstance(e, exp.And):
        return _conjuncts(e.this) + _conjuncts(e.expression)
    return [e]


def _interval_seconds(e: exp.Expression) -> int | None:
    if isinstance(e, exp.Interval):
        unit = (e.args.get("unit").name if e.args.get("unit") is not None else "").upper().rstrip("S")
        n = e.this.this if isinstance(e.this, exp.Literal) else None
        if n is not None and unit in UNIT_SECONDS and re.fullmatch(r"\d+", str(n)):
            return int(n) * UNIT_SECONDS[unit]
    if isinstance(e, (exp.DateSub,)) and isinstance(e.expression, exp.Literal):
        unit = (e.args.get("unit").name if e.args.get("unit") is not None else "DAY").upper().rstrip("S")
        return int(e.expression.this) * UNIT_SECONDS.get(unit, 86400)
    return None


def _is_anchor(e: exp.Expression) -> str | None:
    if isinstance(e, (exp.Placeholder, exp.Parameter)):
        return e.sql()
    if isinstance(e, (exp.CurrentTimestamp, exp.CurrentDate)):
        return e.sql()
    if isinstance(e, exp.Cast):
        return _is_anchor(e.this)
    if isinstance(e, exp.Anonymous) and e.name.lower() in ("now", "current_timestamp"):
        return e.sql()
    return None


def _window_bound(c: exp.Expression, fact: str):
    """Recognise `col >= anchor - INTERVAL` / `col < anchor` (either side). Returns a tuple."""
    flips = {exp.GT: exp.LT, exp.GTE: exp.LTE, exp.LT: exp.GT, exp.LTE: exp.GTE}
    if type(c) not in flips:
        return None
    left, right, cls = c.this, c.expression, type(c)
    if not isinstance(left, exp.Column):
        left, right, cls = right, left, flips[cls]
    if not isinstance(left, exp.Column):
        return None
    col = _col(left, fact)
    if isinstance(right, exp.Sub) and _is_anchor(right.this):
        secs = _interval_seconds(right.expression)
        if secs is not None and cls in (exp.GT, exp.GTE):
            return ("lower", col, secs, "inclusive" if cls is exp.GTE else "exclusive", _is_anchor(right.this))
    if isinstance(right, exp.DateSub) and _is_anchor(right.this):
        secs = _interval_seconds(right)
        if secs is not None and cls in (exp.GT, exp.GTE):
            return ("lower", col, secs, "inclusive" if cls is exp.GTE else "exclusive", _is_anchor(right.this))
    anchor = _is_anchor(right)
    if anchor and cls in (exp.LT, exp.LTE):
        return ("upper", col, None, "inclusive" if cls is exp.LTE else "exclusive", anchor)
    return None


AGG = {exp.Sum: "sum", exp.Avg: "mean", exp.Min: "min", exp.Max: "max"}


def _strip_alias(e: exp.Expression) -> tuple[str | None, exp.Expression]:
    if isinstance(e, exp.Alias):
        return e.alias, e.this
    if isinstance(e, exp.Column):
        return e.name, e
    return None, e


def _aggregate(name: str, e: exp.Expression, fact: str, sql: str) -> Output | None:
    """One aggregate output, or None when `e` is not a (possibly filtered/coalesced) aggregate."""
    coalesce_zero = False
    if isinstance(e, exp.Coalesce) and len(e.expressions) == 1 and isinstance(e.expressions[0], exp.Literal) and e.expressions[0].this in ("0", "0.0"):
        coalesce_zero, e = True, e.this
    filt = None
    if isinstance(e, exp.Filter):
        filt, e = e.expression.this if isinstance(e.expression, exp.Where) else e.expression, e.this
    cols: list = []
    out = Output(name=name, kind="aggregate", sql=sql, input=fact, coalesce_zero=coalesce_zero)
    if isinstance(e, exp.Count):
        arg = e.this
        distinct = isinstance(arg, exp.Distinct)
        if distinct:
            arg = arg.expressions[0] if len(arg.expressions) == 1 else None
            if arg is None:
                raise Unsupported("COUNT(DISTINCT a, b)")
        if isinstance(arg, exp.Star) or arg is None:
            out.operation = "count"
        else:
            if isinstance(arg, exp.Case):
                cond, val = _case_filter(arg)
                filt = cond if filt is None else exp.and_(filt, cond)
                arg = val
            if isinstance(arg, exp.Literal) and not distinct:
                out.operation = "count"
            elif distinct:
                out.operation = "count_distinct"
                out.value = to_expression(arg, fact, cols)
            else:
                # COUNT(x) counts non-null x: a count with an explicit not-null filter.
                out.operation = "count"
                nn = exp.Not(this=exp.Is(this=arg.copy(), expression=exp.Null()))
                filt = nn if filt is None else exp.and_(filt, nn)
    elif type(e) in AGG:
        out.operation = AGG[type(e)]
        arg = e.this
        if isinstance(arg, exp.Case):
            cond, val = _case_filter(arg, allow_zero_else=out.operation == "sum")
            filt = cond if filt is None else exp.and_(filt, cond)
            if out.operation == "sum" and isinstance(arg.args.get("default"), exp.Literal):
                # SUM(CASE WHEN c THEN x ELSE 0 END): rows failing c add 0, so an entity whose rows
                # all fail c is 0, not null: that is a filtered sum with empty value 0.
                out.coalesce_zero = True
            arg = val
        out.value = to_expression(arg, fact, cols)
    else:
        return None
    if filt is not None:
        out.filter = to_expression(filt, fact, cols)
        out.filter_sql = filt.sql(dialect="databricks")
    out.columns = cols
    return out


def _case_filter(case: exp.Case, allow_zero_else: bool = False):
    ifs = case.args.get("ifs") or []
    default = case.args.get("default")
    if len(ifs) != 1:
        raise Unsupported(f"CASE with {len(ifs)} branches inside an aggregate: {case.sql()}")
    if default is not None and not isinstance(default, exp.Null):
        if not (allow_zero_else and isinstance(default, exp.Literal) and default.this in ("0", "0.0")):
            raise Unsupported(f"CASE ... ELSE {default.sql()} inside an aggregate changes empty/zero semantics: {case.sql()}")
    return ifs[0].this, ifs[0].args["true"]


def analyze_sql(sql: str, dialect: str = "databricks") -> Analysis:
    tree = sqlglot.parse_one(sql, read=dialect)
    unsupported: list[dict] = []
    if tree.find(exp.With) is not None:
        unsupported.append({"construct": "CTE", "sql": tree.args["with"].sql(dialect=dialect)[:400], "advice": "Inline the CTE into one SELECT (or analyze each CTE that builds a feature separately)."})
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        raise ValueError("No SELECT statement found")
    frm = select.args.get("from_") or select.args.get("from")
    if frm is None or not isinstance(frm.this, exp.Table):
        raise ValueError("FROM must name a table (subqueries are not analyzed; inline them)")
    fact_t = frm.this
    fact = _alias_name(fact_t)
    tables = {fact: _table_name(fact_t)}
    joins = []
    for j in select.args.get("joins") or []:
        if not isinstance(j.this, exp.Table):
            unsupported.append({"construct": "JOIN subquery", "sql": j.sql(dialect=dialect)[:400]})
            continue
        a = _alias_name(j.this)
        tables[a] = _table_name(j.this)
        pairs = []
        for c in _conjuncts(j.args.get("on")):
            if isinstance(c, exp.EQ) and isinstance(c.this, exp.Column) and isinstance(c.expression, exp.Column):
                pairs.append([_col(c.this, fact), _col(c.expression, fact)])
            else:
                unsupported.append({"construct": "non-equi join condition", "sql": c.sql(dialect=dialect)})
        joins.append({"alias": a, "table": tables[a], "kind": (j.args.get("side") or "inner").lower(), "on": pairs})
    group = [_col(g, fact) for g in (select.args.get("group").expressions if select.args.get("group") else []) if isinstance(g, exp.Column)]
    lower = upper = None
    filters = []
    for c in _conjuncts(select.args.get("where").this if select.args.get("where") else None):
        b = _window_bound(c, fact)
        if b and b[0] == "lower":
            lower = b
        elif b and b[0] == "upper":
            upper = b
        else:
            try:
                filters.append(to_expression(c, fact, []))
            except Unsupported as u:
                unsupported.append({"construct": "WHERE predicate", "sql": c.sql(dialect=dialect), "reason": str(u)})
    window = None
    if lower or upper:
        col = (lower or upper)[1]
        window = Window(alias=col[0], event_column=col[1], lookback_seconds=lower[2] if lower else None, lower=lower[3] if lower else "inclusive", upper=upper[3] if upper else "exclusive", anchor=(lower or upper)[4])
        if lower and upper and lower[1] != upper[1]:
            unsupported.append({"construct": "window", "sql": "lower and upper bounds use different columns", "advice": "Use one event-time column."})
    outputs: list[Output] = []
    named: dict[str, dict] = {}
    gset = {c for _, c in group}
    for item in select.expressions:
        name, e = _strip_alias(item)
        sql_text = item.sql(dialect=dialect)
        if name is None:
            unsupported.append({"construct": "unnamed output", "sql": sql_text})
            continue
        if isinstance(e, exp.Column) and e.name in gset:
            outputs.append(Output(name=name, kind="key", sql=sql_text, columns=[_col(e, fact)]))
            continue
        if _is_anchor(e):
            outputs.append(Output(name=name, kind="as_of", sql=sql_text))
            continue
        try:
            agg = _aggregate(name, e, fact, sql_text)
            if agg:
                outputs.append(agg)
                named[name] = {"kind": "feature", "feature": name, "version": "1.0.0"}
                continue
            # Derived: an expression over earlier outputs; NULLIF(b, 0) as divisor -> safe_divide.
            e2 = e.transform(lambda n: n.this if isinstance(n, exp.Nullif) and isinstance(n.expression, exp.Literal) and n.expression.this in ("0", "0.0") else n)
            safe = e2 is not e and any(isinstance(n, exp.Nullif) for n in e.walk())
            refs = {n.name for n in e2.find_all(exp.Column)}
            if refs and refs <= set(named):
                ex = to_expression(e2, fact, [], feature_ref=named)
                if safe and ex.get("operator") == "divide":
                    ex["operator"] = "safe_divide"
                outputs.append(Output(name=name, kind="expression", sql=sql_text, expression=ex, depends_on=sorted(refs)))
                named[name] = {"kind": "feature", "feature": name, "version": "1.0.0"}
                continue
            raise Unsupported(f"{type(e).__name__} is neither an aggregate nor an expression over named outputs")
        except Unsupported as u:
            unsupported.append({"construct": "output", "name": name, "sql": sql_text, "reason": str(u)})
    if select.args.get("having"):
        unsupported.append({"construct": "HAVING", "sql": select.args["having"].sql(dialect=dialect), "advice": "HAVING removes entities; express it as a consumer filter or drop it."})
    return Analysis(sql, dialect, tables, fact, joins, group, window, filters, outputs, unsupported)
