"""Static analysis of feature-table SQL (Databricks dialect) into candidate features.

Supported shape (one SELECT, the usual way a batch feature table is built)::

    SELECT <entity keys>, [<as_of>,] <aggregate or expression> AS <feature>, ...
    FROM <fact table> [alias] [JOIN <dimension> ON <equi-join>]
    WHERE <event time> >= <anchor> - INTERVAL n <unit> AND <event time> < <anchor> [AND <filters>]
    GROUP BY <entity keys>

Aggregates: COUNT(*), COUNT(x), COUNT(DISTINCT x), SUM, AVG, MIN, MAX, with filters written as
``FILTER (WHERE c)``, ``CASE WHEN c THEN x END`` or WHERE conjuncts; a window bound inside such a
filter (``ts >= :as_of - INTERVAL 7 DAYS``) is that output's own window. Recency:
``TIMESTAMPDIFF(SECOND, MAX(ts), :as_of)`` (or a ``unix_timestamp`` difference). Lookups:
``MAX_BY(v, ts)`` / ``MIN_BY(v, ts)``. Outputs that combine other outputs (``a / b``,
``a / NULLIF(b, 0)``, ``COALESCE(...)``) become derived expression features. Simple CTEs
(``SELECT cols/exprs/* FROM t [WHERE p]``, chained or not) are inlined first.

Nothing is guessed silently: anything outside this shape is returned in ``unsupported`` with the
SQL fragment, and the caller (an agent or a person) must translate it or drop it.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

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
    kind: str  # key | as_of | aggregate | recency | lookup | expression
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
    # Per-output window, when the output narrows the shared WHERE window with its own bound
    # (``FILTER (WHERE ts >= :as_of - INTERVAL 7 DAYS)`` or the same condition in a CASE).
    window: Window | None = None
    event_column: str | None = None  # recency: the event-time column whose latest value is aged
    order_by: list[dict] | None = None  # lookup: [{"input", "column", "direction"}]
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


def _epoch_seconds_of(e: exp.Expression) -> exp.Expression | None:
    """`unix_timestamp(x)` or `CAST(x AS BIGINT|LONG)` -> x (both floor a timestamp to seconds)."""
    if isinstance(e, exp.Paren):
        return _epoch_seconds_of(e.this)
    if isinstance(e, (exp.StrToUnix, exp.UnixSeconds)) or (isinstance(e, exp.Anonymous) and e.name.lower() in ("unix_timestamp", "unix_seconds")):
        return e.this if not isinstance(e, exp.Anonymous) else (e.expressions[0] if len(e.expressions) == 1 else None)
    if isinstance(e, exp.Cast) and e.to.this in (exp.DataType.Type.BIGINT, exp.DataType.Type.INT):
        return e.this
    return None


def _recency(e: exp.Expression) -> tuple[exp.Expression, list[str]] | None:
    """Elapsed time from the latest eligible event to as_of. Returns (the MAX(...) part, notes)."""
    if isinstance(e, exp.Paren):
        return _recency(e.this)
    if isinstance(e, exp.TimestampDiff) and _is_anchor(e.this) and e.expression.find(exp.Max):
        unit = (e.args.get("unit").name if e.args.get("unit") is not None else "").upper()
        if unit != "SECOND":
            raise Unsupported(f"TIMESTAMPDIFF({unit}, ...): recency is defined in whole seconds (floor); express other units as a derived expression over a seconds recency, or drop it")
        return e.expression, []
    if isinstance(e, exp.DateDiff) and (_is_anchor(e.this) or _is_anchor(e.expression)) and e.find(exp.Max):
        raise Unsupported("DATEDIFF counts calendar-day boundaries (or whole units) between dates, which is not elapsed time: no spec recency or expression reproduces it exactly; use TIMESTAMPDIFF(SECOND, MAX(ts), :as_of) or drop it")
    if isinstance(e, exp.Sub):
        end, start = _epoch_seconds_of(e.this), _epoch_seconds_of(e.expression)
        if end is not None and start is not None and _is_anchor(end) and start.find(exp.Max):
            return start, ["unix_timestamp/CAST-to-BIGINT floors each side before subtracting; the spec floors the difference. Equal when event times are whole seconds (otherwise the legacy value can be 1 second larger)."]
    return None


def _split_window(filt: exp.Expression | None, fact: str) -> tuple[exp.Expression | None, Window | None]:
    """Move `ts >= anchor - INTERVAL` / `ts < anchor` conjuncts of an output filter into a window."""
    if filt is None:
        return None, None
    lower = upper = None
    rest = []
    for c in _conjuncts(filt):
        b = _window_bound(c, fact)
        if b and b[0] == "lower":
            if lower is not None:
                raise Unsupported(f"two lower window bounds in one output filter: {filt.sql()}")
            lower = b
        elif b and b[0] == "upper":
            upper = b
        else:
            if any(_is_anchor(n) for n in c.walk()):
                raise Unsupported(f"filter compares with the as_of anchor in a way that is not a window lower bound (windows end at as_of): {c.sql()}")
            rest.append(c)
    window = None
    if lower or upper:
        col = (lower or upper)[1]
        if lower and upper and lower[1] != upper[1]:
            raise Unsupported("output window bounds use different columns")
        window = Window(alias=col[0], event_column=col[1], lookback_seconds=lower[2] if lower else None, lower=lower[3] if lower else "inclusive", upper=upper[3] if upper else "", anchor=(lower or upper)[4])
    out = None
    for c in rest:
        out = c if out is None else exp.and_(out, c)
    return out, window


def _aggregate(name: str, e: exp.Expression, fact: str, sql: str) -> Output | None:
    """One aggregate/recency/lookup output, or None when `e` is none of them."""
    rec = _recency(e)
    if rec is not None:
        inner, notes = rec
        out = _aggregate(name, inner, fact, sql)
        if out is None or out.operation != "max" or out.coalesce_zero or not (out.value or {}).get("kind") == "column":
            raise Unsupported(f"recency must age MAX(<event time column>) [FILTER (WHERE ...)]: {inner.sql()}")
        out.kind, out.operation, out.event_column, out.value = "recency", None, out.value["column"], None
        out.notes += notes
        return out
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
    elif isinstance(e, (exp.ArgMax, exp.ArgMin)):
        if coalesce_zero:
            raise Unsupported(f"COALESCE(MAX_BY/MIN_BY(...), 0) turns an empty window into a value: {sql}")
        order = e.expression
        if not isinstance(order, exp.Column):
            raise Unsupported(f"{'MAX_BY' if isinstance(e, exp.ArgMax) else 'MIN_BY'} must order by a column: {e.sql()}")
        out.kind = "lookup"
        out.value = to_expression(e.this, fact, cols)
        oa, oc = _col(order, fact)
        cols.append((oa, oc))
        out.order_by = [{"input": oa, "column": oc, "direction": "desc" if isinstance(e, exp.ArgMax) else "asc"}]
        out.notes.append("Legacy MAX_BY/MIN_BY skips rows whose ordering value is null and picks an arbitrary row on ties; the spec lookup rejects both (NULL_ORDER_KEY, LOOKUP_TIE). Equal when the ordering column is non-null and unique per entity.")
    else:
        return None
    if filt is not None:
        out.filter_sql = filt.sql(dialect="databricks")
        filt, out.window = _split_window(filt, fact)
        if filt is not None:
            out.filter = to_expression(filt, fact, cols)
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


def _simple_cte(q: exp.Expression) -> str | None:
    """Why a CTE body cannot be inlined, or None when it is `SELECT <cols/exprs/*> FROM t [WHERE p]`."""
    if not isinstance(q, exp.Select):
        return "not a plain SELECT (set operation or nested query)"
    for arg in ("joins", "group", "having", "qualify", "limit", "order", "distinct", "windows"):
        if q.args.get(arg):
            return f"has {arg.upper()}"
    frm = q.args.get("from_") or q.args.get("from")
    if frm is None or not isinstance(frm.this, exp.Table):
        return "FROM is not a table"
    for p in q.expressions:
        if p.find(exp.AggFunc) or p.find(exp.Window):
            return "projects an aggregate or window function"
        if not isinstance(p, (exp.Star, exp.Column)) and not (isinstance(p, exp.Alias)):
            return f"unnamed projection {p.sql()}"
    return None


def _inline_ctes(tree: exp.Expression, dialect: str, unsupported: list[dict]) -> exp.Expression:
    """Replace references to simple CTEs by their table, projections and WHERE.

    A reference ``FROM cte c`` becomes ``FROM <table> c``; the CTE's WHERE is conjoined to the outer
    WHERE (to the ON clause for a joined CTE); ``expr AS name`` projections replace outer
    references to ``name``. Anything else stays a CTE and is reported unsupported.
    """
    wkey = "with_" if "with_" in tree.args else "with"
    w = tree.args.get(wkey)
    if w is None:
        return tree
    # Detach the WITH clause so only the main SELECT's references are rewritten.
    tree.set(wkey, None)
    defs: dict[str, tuple[exp.Table, exp.Expression | None, dict[str, exp.Expression]]] = {}
    kept = []
    for cte in w.expressions:
        q = cte.this.copy()
        why = _simple_cte(q)
        if why is None and w.args.get("recursive"):
            why = "recursive CTE"
        if why is not None:
            kept.append(cte)
            unsupported.append({"construct": "CTE", "name": cte.alias, "sql": cte.sql(dialect=dialect)[:400], "reason": why, "advice": "Only `SELECT columns/expressions/* FROM table [WHERE ...]` CTEs are inlined; rewrite this one into the main SELECT or analyze it separately."})
            continue
        frm = (q.args.get("from_") or q.args.get("from")).this
        inner_alias = frm.alias_or_name
        proj: dict[str, exp.Expression] = {}
        # Chained CTE: resolve the inner reference first; its renamed projections carry through.
        if not frm.db and frm.name in defs:
            base, base_where, base_proj = defs[frm.name]
            table = base.copy()
            inherited = {k: _requalify(v.copy(), base.alias_or_name, inner_alias) for k, v in base_proj.items()}
            q = _substitute(q, inner_alias, inherited)
            proj.update(inherited)
            where = q.args["where"].this if q.args.get("where") else None
            if base_where is not None:
                bw = _requalify(base_where.copy(), base.alias_or_name, inner_alias)
                where = bw if where is None else exp.and_(bw, where)
        else:
            table = frm.copy()
            where = q.args["where"].this if q.args.get("where") else None
        for p in q.expressions:
            if isinstance(p, exp.Alias) and not (isinstance(p.this, exp.Column) and p.this.name == p.alias):
                proj[p.alias] = p.this
        table.set("alias", exp.TableAlias(this=exp.to_identifier(inner_alias)))
        defs[cte.alias] = (table, where, {k: _qualify(v.copy(), inner_alias) for k, v in proj.items()})
        if where is not None:
            defs[cte.alias] = (table, _qualify(where.copy(), inner_alias), defs[cte.alias][2])
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    refs = [t for t in select.find_all(exp.Table) if not t.db and t.name in defs]
    for t in refs:
        table, where, proj = defs[t.name]
        outer = t.alias_or_name
        new = table.copy()
        new.set("alias", exp.TableAlias(this=exp.to_identifier(outer)))
        is_from = isinstance(t.parent, exp.From)
        t.replace(new)
        _substitute(select, outer, {k: _requalify(v.copy(), table.alias_or_name, outer) for k, v in proj.items()}, unqualified=is_from)
        if where is not None:
            cond = _requalify(where.copy(), table.alias_or_name, outer)
            if is_from:
                old = select.args.get("where")
                select.set("where", exp.Where(this=cond if old is None else exp.and_(cond, old.this)))
            else:
                j = new.parent
                j.set("on", cond if j.args.get("on") is None else exp.and_(j.args["on"], cond))
    if kept:
        w.set("expressions", kept)
        tree.set(wkey, w)
    return tree


def _qualify(e: exp.Expression, alias: str) -> exp.Expression:
    for c in list(e.find_all(exp.Column)):
        if not c.table:
            c.set("table", exp.to_identifier(alias))
    return e if not isinstance(e, exp.Column) or e.table else exp.column(e.name, table=alias)


def _requalify(e: exp.Expression, old: str, new: str) -> exp.Expression:
    for c in list(e.find_all(exp.Column)):
        if c.table == old:
            c.set("table", exp.to_identifier(new))
    return e


def _substitute(node: exp.Expression, alias: str, proj: dict[str, exp.Expression], unqualified: bool = True) -> exp.Expression:
    """Replace references to projected names (``alias.name`` or bare ``name``) by their expression.

    A reference projected as-is by the outer SELECT keeps its name (``SELECT cid`` -> ``SELECT
    c.customer_id AS cid``)."""
    if not proj:
        return node
    for c in list(node.find_all(exp.Column)):
        if c.name in proj and (c.table == alias or (unqualified and not c.table)):
            repl = proj[c.name].copy()
            if not isinstance(repl, (exp.Column, exp.Literal, exp.Func)):
                repl = exp.paren(repl)
            if isinstance(c.parent, exp.Select) and c.arg_key == "expressions":
                repl = exp.alias_(repl, c.name)
            c.replace(repl)
    return node


def _reconcile_windows(outputs: list[Output], shared: Window | None, unsupported: list[dict]) -> list[Output]:
    """Intersect each output's own window with the shared WHERE window (rows must pass both)."""
    kept = []
    for o in outputs:
        ow = o.window
        try:
            if ow is not None and shared is not None:
                if (ow.alias, ow.event_column) != (shared.alias, shared.event_column):
                    raise Unsupported(f"output window on {ow.alias}.{ow.event_column} but the shared window is on {shared.alias}.{shared.event_column}; a source has one event time")
                if ow.lookback_seconds is None or (shared.lookback_seconds is not None and shared.lookback_seconds < ow.lookback_seconds):
                    if ow.lookback_seconds is not None:
                        o.notes.append(f"Own lower bound ({ow.lookback_seconds}s) is wider than the shared WHERE window ({shared.lookback_seconds}s); the shared window applies.")
                    ow.lookback_seconds, ow.lower = shared.lookback_seconds, shared.lower
                elif ow.lookback_seconds == shared.lookback_seconds and "exclusive" in (ow.lower, shared.lower):
                    ow.lower = "exclusive"
                if not ow.upper or shared.upper == "exclusive":
                    ow.upper = shared.upper
            elif ow is not None and not ow.upper:
                ow.upper = "exclusive"
            if o.kind == "recency":
                w = ow or shared
                if w is not None and (o.input, o.event_column) != (w.alias, w.event_column):
                    raise Unsupported(f"recency ages {o.event_column} but the window is on {w.event_column}; the spec ages the source's event time")
        except Unsupported as u:
            unsupported.append({"construct": "output", "name": o.name, "sql": o.sql, "reason": str(u)})
            continue
        if o.kind == "expression" and not set(o.depends_on) <= {k.name for k in kept}:
            unsupported.append({"construct": "output", "name": o.name, "sql": o.sql, "reason": "depends on an output that could not be translated"})
            continue
        kept.append(o)
    return kept


def _valid_time_bound(c: exp.Expression, fact: str, dim: str):
    """`f.ts >= d.vf` -> ("from", (f,ts), vf); `d.vt IS NULL OR f.ts < d.vt` -> ("to", (f,ts), vt)."""
    def side(x):
        return _col(x, fact) if isinstance(x, exp.Column) else None
    if isinstance(c, (exp.GTE, exp.LTE)):
        lt, rt = side(c.this), side(c.expression)
        if lt and rt:
            ts, vf = (lt, rt) if isinstance(c, exp.GTE) else (rt, lt)
            if ts[0] != dim and vf[0] == dim:
                return ("from", ts, vf[1])
        return None
    if isinstance(c, exp.Paren):
        return _valid_time_bound(c.this, fact, dim)
    if isinstance(c, exp.Or):
        parts = [c.this.this if isinstance(c.this, exp.Paren) else c.this, c.expression.this if isinstance(c.expression, exp.Paren) else c.expression]
        nulls = [x for x in parts if isinstance(x, exp.Is) and isinstance(x.expression, exp.Null)]
        lts = [x for x in parts if isinstance(x, (exp.LT, exp.GT))]
        if len(nulls) == 1 and len(lts) == 1:
            vt_null = side(nulls[0].this)
            x = lts[0]
            lt, rt = side(x.this), side(x.expression)
            if vt_null and lt and rt:
                ts, vt = (lt, rt) if isinstance(x, exp.LT) else (rt, lt)
                if vt == vt_null and vt[0] == dim and ts[0] != dim:
                    return ("to", ts, vt[1])
    return None


def analyze_sql(sql: str, dialect: str = "databricks") -> Analysis:
    tree = sqlglot.parse_one(sql, read=dialect)
    unsupported: list[dict] = []
    tree = _inline_ctes(tree, dialect, unsupported)
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
        valid: dict = {}
        ok = True
        for c in _conjuncts(j.args.get("on")):
            if isinstance(c, exp.EQ) and isinstance(c.this, exp.Column) and isinstance(c.expression, exp.Column):
                left, right = _col(c.this, fact), _col(c.expression, fact)
                if left[0] == a:  # keep the fact (left) side first
                    left, right = right, left
                pairs.append([left, right])
                continue
            vt = _valid_time_bound(c, fact, a)
            if vt is not None and vt[0] not in valid:
                valid[vt[0]] = vt[1:]
                continue
            ok = False
            unsupported.append({"construct": "non-equi join condition", "sql": c.sql(dialect=dialect), "advice": "Only equi-join keys and a half-open valid-time probe (fact.ts >= d.valid_from AND (d.valid_to IS NULL OR fact.ts < d.valid_to)) are translated."})
        join = {"alias": a, "table": tables[a], "kind": (j.args.get("side") or "inner").lower(), "on": pairs, "valid_time": None, "translatable": ok and bool(pairs)}
        if valid:
            if set(valid) == {"from", "to"} and valid["from"][0] == valid["to"][0]:
                join["valid_time"] = {"probe": valid["from"][0], "valid_from": valid["from"][1], "valid_to": valid["to"][1]}
            else:
                join["translatable"] = False
                unsupported.append({"construct": "valid-time join", "sql": j.sql(dialect=dialect)[:400], "reason": "needs both fact.ts >= d.valid_from and (d.valid_to IS NULL OR fact.ts < d.valid_to) on the same fact column"})
        if join["kind"] not in ("left", "inner"):
            join["translatable"] = False
            unsupported.append({"construct": "JOIN", "sql": j.sql(dialect=dialect)[:400], "reason": f"{join['kind'].upper()} JOIN: relationships are left or inner"})
        joins.append(join)
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
    outputs = _reconcile_windows(outputs, window, unsupported)
    if select.args.get("having"):
        unsupported.append({"construct": "HAVING", "sql": select.args["having"].sql(dialect=dialect), "advice": "HAVING removes entities; express it as a consumer filter or drop it."})
    return Analysis(sql, dialect, tables, fact, joins, group, window, filters, outputs, unsupported)
