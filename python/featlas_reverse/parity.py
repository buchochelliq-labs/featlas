"""SQL that proves (or disproves) that regenerated feature tables reproduce the legacy table.

``parity_sql`` compares the legacy wide table with the regenerated table(s) at one as_of: rows
present on only one side, and per column the number of rows whose values differ beyond the
feature's declared tolerance (null-safe). Run it read-only; a clean result is
``missing_in_new = missing_in_legacy = 0`` and every ``mismatched_*`` = 0.

Differences are expected — and are findings, not bugs — where the legacy code was not
point-in-time correct (it read rows that arrived after as_of) or treated empty windows
differently; the drafting report lists those assumptions.
"""

from __future__ import annotations


def _q(x: str) -> str:
    return f"`{x}`"


def _fq(t: str) -> str:
    return ".".join(_q(p) for p in t.split("."))


def parity_sql(legacy_table: str, legacy_as_of_column: str, keys: list[str], mapping: dict, as_of_literal: str, tolerances: dict | None = None, new_as_of_column: str = "feature_as_of") -> str:
    """``mapping``: legacy column -> {"table": fq new table, "column": new column} (as returned by
    ``split.apply_split`` or built from a draft's output target)."""
    tolerances = tolerances or {}
    tables = sorted({m["table"] for m in mapping.values()})
    alias = {t: f"n{i}" for i, t in enumerate(tables)}
    at = f"TIMESTAMP '{as_of_literal}'"
    new_cte = []
    for t in tables:
        cols = [m for m in mapping.values() if m["table"] == t]
        sel = ", ".join([_q(k) for k in keys] + [_q(m["column"]) for m in cols])
        new_cte.append(f"{alias[t]} AS (SELECT {sel} FROM {_fq(t)} WHERE {_q(new_as_of_column)} = {at})")
    key_list = ", ".join(_q(k) for k in keys)
    # Keys of every regenerated table (each must carry the same population).
    new_keys = " UNION ".join(f"SELECT {key_list} FROM {alias[t]}" for t in tables)
    legacy_cols = ", ".join([_q(k) for k in keys] + [_q(c) for c in mapping])
    joins = "\n".join(f"LEFT JOIN {alias[t]} ON " + " AND ".join(f"{alias[t]}.{_q(k)} = k.{_q(k)}" for k in keys) for t in tables)
    checks = []
    for legacy_col, m in mapping.items():
        n = f"{alias[m['table']]}.{_q(m['column'])}"
        o = f"l.{_q(legacy_col)}"
        tol = tolerances.get(legacy_col)
        if tol:
            same = f"(({o} IS NULL AND {n} IS NULL) OR abs(CAST({o} AS DOUBLE) - CAST({n} AS DOUBLE)) <= greatest({tol.get('absolute', 0)}, {tol.get('relative', 0)} * abs(CAST({o} AS DOUBLE))))"
        else:
            same = f"({o} <=> {n})"
        checks.append(f"  sum(CASE WHEN l.{_q(keys[0])} IS NOT NULL AND k.{_q(keys[0])} IS NOT NULL AND NOT {same} THEN 1 ELSE 0 END) AS {_q('mismatched_' + legacy_col)}")
    return (
        f"-- Parity of {legacy_table} against {', '.join(tables)} at {as_of_literal} (read-only).\n"
        f"WITH l AS (SELECT {legacy_cols} FROM {_fq(legacy_table)} WHERE {_q(legacy_as_of_column)} = {at}),\n"
        + ",\n".join(new_cte)
        + f",\nk AS ({new_keys}),\nall_keys AS (SELECT {key_list} FROM l UNION SELECT {key_list} FROM k)\n"
        "SELECT\n"
        f"  count(*) AS rows_compared,\n"
        f"  sum(CASE WHEN l.{_q(keys[0])} IS NULL THEN 1 ELSE 0 END) AS missing_in_legacy,\n"
        f"  sum(CASE WHEN k.{_q(keys[0])} IS NULL THEN 1 ELSE 0 END) AS missing_in_new,\n"
        + ",\n".join(checks)
        + f"\nFROM all_keys a\nLEFT JOIN l ON " + " AND ".join(f"l.{_q(x)} = a.{_q(x)}" for x in keys)
        + "\nLEFT JOIN k ON " + " AND ".join(f"k.{_q(x)} = a.{_q(x)}" for x in keys)
        + "\n" + joins.replace("= k.", "= a.")
    )


def mapping_from_project(project: dict, deployment: str | None = None) -> dict:
    """legacy column -> new table/column for every output target of a (draft or split) project."""
    dep = project["deployments"][0] if deployment is None else next(d for d in project["deployments"] if d["id"] == deployment)
    out = {}
    for t in dep["output_targets"]:
        fq = ".".join(t["location"]["namespace"] + [t["location"]["name"]])
        for m in t["feature_mapping"]:
            out[m["feature"].split(".", 1)[1]] = {"table": fq, "column": m["column"]}
    return out
