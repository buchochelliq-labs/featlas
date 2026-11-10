"""The featlas-reverse MCP server over stdio: draft -> split -> parity -> generate, inline only."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("sqlglot")
pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[2]
EX = ROOT / "examples/reverse"


class Client:
    def __init__(self):
        self.p = subprocess.Popen([sys.executable, "-m", "featlas_reverse.mcp_server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, cwd=ROOT / "python")
        self.n = 0

    def call(self, method, params=None):
        self.n += 1
        self.p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.n, "method": method, "params": params or {}}) + "\n")
        self.p.stdin.flush()
        return json.loads(self.p.stdout.readline())

    def tool(self, name, **args):
        r = self.call("tools/call", {"name": name, "arguments": args})["result"]
        assert not r.get("isError"), r
        return r["structuredContent"]

    def close(self):
        self.p.stdin.close()
        self.p.wait(timeout=10)


def test_agent_workflow_over_mcp():
    c = Client()
    try:
        init = c.call("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}})
        assert init["result"]["serverInfo"]["name"] == "featlas-reverse"
        tools = {t["name"] for t in c.call("tools/list")["result"]["tools"]}
        assert tools == {"reverse_analyze_sql", "reverse_draft_spec", "spec_validate", "spec_generate", "split_analyze", "split_apply", "parity_sql"}
        load = lambda n: json.loads((EX / f"{n}.table.json").read_text())  # noqa: E731
        sql = (EX / "customer_wide_features.sql").read_text()
        d = c.tool("reverse_draft_spec", target_table=load("customer_wide"), sql=sql, sources=[load("orders"), load("customers")])
        assert d["validation"]["passed"] and d["validation"]["fixtures"]["failed"] == 0
        assert d["report"]["mapping"]["return_rate_30d"]["feature"] == "customer.return_rate_30d"
        s = c.tool("split_analyze", document=d["project_yaml"], feature_set="customer_wide", legacy_table=load("customer_wide"))
        assert s["recommended"] == "tag:domain"
        best = next(x for x in s["strategies"] if x["strategy"] == s["recommended"])
        a = c.tool("split_apply", document=d["project_yaml"], feature_set="customer_wide", assignment=best["assignment"])
        assert a["validation"]["passed"] and len(a["groups"]) == 6
        par = c.tool("parity_sql", document=a["project_yaml"], legacy_table="main.features.customer_wide", legacy_as_of_column="feature_ts", keys=["tenant_id", "customer_id"], as_of="2026-06-01T00:00:00Z")
        assert "missing_in_new" in par["sql"] and "mismatched_return_rate_30d" in par["sql"]
        g = c.tool("spec_generate", document=a["project_yaml"], deployment="customer_wide_databricks", include_files=False)
        assert g["execution_ready"] is False and any("customer_wide_revenue" in f for f in g["files"])
        bad = c.call("tools/call", {"name": "split_apply", "arguments": {"document": d["project_yaml"], "feature_set": "customer_wide", "assignment": {}}})["result"]
        assert bad["isError"]
    finally:
        c.close()
