"""A feature set for the Feature Views mapping, drafted by featlas_reverse (no invented goldens).

The source's availability time is declared to be its event time (decision recorded in the
drafting report), which is the condition under which a Feature View can compute a featlas
feature exactly. The SQL deliberately mixes features that map (count, filtered sum, max, mean,
latest value) with features that must stay on the Unity Catalog feature table (distinct count,
recency, a derived ratio).
"""

from __future__ import annotations

import copy

from featlas_reverse import describe
from featlas_reverse.analyze import analyze_sql
from featlas_reverse.draft import draft_spec, to_yaml

SQL = """
SELECT e.tenant_id, e.device_id, :as_of AS feature_ts,
  COUNT(*) AS events_1h,
  SUM(e.severity) FILTER (WHERE e.is_fault = true) AS fault_severity_1h,
  MAX(e.severity) AS max_severity_1h,
  AVG(e.severity) AS mean_severity_1h,
  MAX_BY(e.severity, e.occurred_at) AS last_severity_1h,
  COUNT(DISTINCT e.code) AS distinct_codes_1h,
  TIMESTAMPDIFF(SECOND, MAX(e.occurred_at), :as_of) AS seconds_since_event,
  fault_severity_1h / NULLIF(events_1h, 0) AS fault_severity_per_event_1h
FROM {events} e
WHERE e.occurred_at >= :as_of - INTERVAL 1 HOUR AND e.occurred_at < :as_of
GROUP BY e.tenant_id, e.device_id
"""

EVENTS = {"primary_key": ["event_id"], "columns": [
    {"name": "event_id", "type": "string", "nullable": False}, {"name": "tenant_id", "type": "string", "nullable": False},
    {"name": "device_id", "type": "string", "nullable": False}, {"name": "occurred_at", "type": "timestamp", "nullable": False},
    {"name": "is_fault", "type": "boolean"}, {"name": "severity", "type": "double"}, {"name": "code", "type": "string"}]}
DEVICES = {"primary_key": ["tenant_id", "device_id"], "columns": [
    {"name": "tenant_id", "type": "string", "nullable": False}, {"name": "device_id", "type": "string", "nullable": False},
    {"name": "registered_at", "type": "timestamp", "nullable": False}]}
MAPPED = {"events_1h", "fault_severity_1h", "max_severity_1h", "mean_severity_1h", "last_severity_1h"}
UNMAPPED = {"distinct_codes_1h", "seconds_since_event", "fault_severity_per_event_1h"}


def project(events: str, devices: str, features_table: str) -> tuple[dict, dict]:
    """(project dict with deployment feature_api: feature_views, drafting report)."""
    target = {"table": features_table, "columns": [{"name": n, "type": t} for n, t in (
        ("tenant_id", "string"), ("device_id", "string"), ("feature_ts", "timestamp"), ("events_1h", "bigint"),
        ("fault_severity_1h", "double"), ("max_severity_1h", "double"), ("mean_severity_1h", "double"),
        ("last_severity_1h", "double"), ("distinct_codes_1h", "bigint"), ("seconds_since_event", "bigint"),
        ("fault_severity_per_event_1h", "double"))]}
    sources = {events: {"table": events, **copy.deepcopy(EVENTS)}, devices: {"table": devices, **copy.deepcopy(DEVICES)}}
    decisions = {
        "population": {"table": devices, "event_time": "registered_at"},
        "sources": {events: {"available_time": "occurred_at"}, devices: {"available_time": "registered_at"}},
        "feature_set.id": "device_events", "earliest_as_of": "2026-01-01T00:00:00Z",
    }
    decisions["feature_set"] = {"id": "device_events"}
    out = draft_spec(describe.load(target), analyze_sql(SQL.format(events=events)), sources, decisions)
    p = out["project"]
    assert p is not None, out["report"]
    p["deployments"][0]["adapters"][0]["configuration"]["feature_api"] = "feature_views"
    return p, out["report"]


def yaml_text(p: dict) -> str:
    return to_yaml(p)
