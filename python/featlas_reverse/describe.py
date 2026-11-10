"""Table descriptions: the physical facts the drafter needs about a table.

A description is JSON (``featlas.table-description/v1``)::

    {"table": "main.sales.orders", "comment": "...", "owner": "sales-data",
     "primary_key": ["order_id"], "timeseries_key": null,
     "tags": {"domain": "sales"},
     "columns": [{"name": "order_id", "type": "string", "nullable": false,
                  "comment": "...", "tags": {"domain": "sales", "business_unit": "retail"}}]}

``types`` are Spark/Databricks SQL type names (``bigint``, ``decimal(18,2)``, ``timestamp``...).
``from_spark`` builds one from a live table (read-only: DESCRIBE, SHOW TBLPROPERTIES,
information_schema tags); everything else is supplied by hand or by an agent.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

SCHEMA = "featlas.table-description/v1"


def load(path_or_dict: str | Path | dict) -> dict:
    d = path_or_dict if isinstance(path_or_dict, dict) else json.loads(Path(path_or_dict).read_text(encoding="utf-8"))
    if "table" not in d or "columns" not in d:
        raise ValueError("A table description needs 'table' and 'columns'")
    d.setdefault("schema", SCHEMA)
    d.setdefault("primary_key", [])
    d.setdefault("tags", {})
    for c in d["columns"]:
        c.setdefault("nullable", True)
        c.setdefault("comment", "")
        c.setdefault("tags", {})
    return d


def column(desc: dict, name: str) -> dict | None:
    return next((c for c in desc["columns"] if c["name"] == name), None)


def logical_type(spark_type: str) -> str:
    t = spark_type.strip().lower()
    if t in ("bigint", "int", "integer", "smallint", "tinyint", "long", "short", "byte"):
        return "integer"
    if t.startswith("decimal"):
        m = re.match(r"decimal\((\d+),\s*(\d+)\)", t)
        return "integer" if m and m.group(2) == "0" else "number"
    if t in ("double", "float", "real"):
        return "number"
    if t in ("string", "varchar", "char") or t.startswith(("varchar", "char")):
        return "string"
    if t == "boolean":
        return "boolean"
    if t.startswith("timestamp"):
        return "timestamp"
    if t == "date":
        return "date"
    raise ValueError(f"No logical type for {spark_type!r} (arrays, maps and structs are not feature types)")


def from_spark(spark: Any, table: str) -> dict:
    """Read-only description of a Unity Catalog table (columns, comments, properties, tags, PK)."""
    cols = []
    for r in spark.sql(f"DESCRIBE TABLE {table}").collect():
        name = r["col_name"]
        if not name or name.startswith("#"):
            break
        cols.append({"name": name, "type": r["data_type"], "comment": r["comment"] or "", "nullable": True, "tags": {}})
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    ddl = spark.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]
    pk = []
    m = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", ddl, re.IGNORECASE)
    ts = None
    if m:
        for part in m.group(1).split(","):
            part = part.strip().strip("`")
            if part.upper().endswith(" TIMESERIES"):
                part = part[: -len(" TIMESERIES")].strip().strip("`")
                ts = part
            pk.append(part)
    for c in cols:
        if re.search(rf"`?{re.escape(c['name'])}`?\s+\S+[^,]*NOT NULL", ddl):
            c["nullable"] = False
    desc = {"schema": SCHEMA, "table": table, "comment": props.get("comment", ""), "primary_key": pk, "timeseries_key": ts, "properties": props, "tags": {}, "columns": cols}
    cat, sch, name = (table.split(".") + ["", "", ""])[:3]
    try:
        rows = spark.sql(
            f"SELECT column_name, tag_name, tag_value FROM `{cat}`.information_schema.column_tags WHERE schema_name = '{sch}' AND table_name = '{name}'"
        ).collect()
        for r in rows:
            c = column(desc, r["column_name"])
            if c is not None:
                c["tags"][r["tag_name"]] = r["tag_value"]
    except Exception:  # noqa: BLE001 - tags are optional metadata
        pass
    return desc
