"""Run a core-generated label dataset (SEM-LABEL-001/002) in a Spark session.

The core decides which requests are mature and proven complete (``label_requests``); this host
registers the eligible ones, reconstructs the label source as known at each distinct dataset
cutoff and reads the generated values query. It returns the core's ``LabelDataset`` shape and
reports the first failure in request order, exactly as the core does. No semantics live here.
"""

from __future__ import annotations

from typing import Any

from .errors import FeatlasError
from .runner import SnapshotRun, format_instant, parse_instant


class LabelRun(SnapshotRun):
    """A SnapshotRun over label artifacts whose ``:as_of`` is the current dataset cutoff."""

    def __init__(self, spark, label_artifacts: dict, requests: dict, source_relations: dict[str, str] | None = None):
        a = dict(label_artifacts)
        a.setdefault("snapshot", {"view": None})
        a.setdefault("features", [])
        super().__init__(spark, a, as_of="1970-01-01T00:00:00Z", coverage={}, unavailable_sources=set(), source_relations=source_relations)
        self.requests = requests

    def _register_requests(self) -> None:
        from pyspark.sql import types as T

        types = {"BIGINT": T.LongType(), "STRING": T.StringType(), "TIMESTAMP": T.TimestampType(), "DOUBLE": T.DoubleType(), "BOOLEAN": T.BooleanType(), "DATE": T.DateType()}
        cols = self.artifacts["request_columns"]
        schema = T.StructType([T.StructField(n, types[t], True) for n, t in cols])
        ts = {"fl_as_of": "as_of", "fl_dataset_cutoff": "dataset_cutoff", "fl_horizon_end": "horizon_end"}
        data = []
        for r in self.requests["eligible"]:
            row = []
            for n, _ in cols:
                if n == "fl_request_index":
                    row.append(r["request_index"])
                elif n in ts:
                    row.append(parse_instant(r[ts[n]]).replace(tzinfo=None))
                else:
                    row.append(r["entity_keys"][n])
            data.append(tuple(row))
        self.spark.createDataFrame(data, schema).createOrReplaceTempView(self.artifacts["requests_view"])

    def run(self) -> dict:
        a = self.artifacts
        out: dict[str, Any] = {
            "consumer": self.requests["consumer"],
            "consumer_version": self.requests["consumer_version"],
            "label_source": self.requests["label_source"],
            "rows": [],
            "excluded": self.requests["excluded"],
        }
        eligible = self.requests["eligible"]
        if not eligible:
            return out
        self.as_of = eligible[0]["dataset_cutoff"]
        self.prepare()  # session + read-only source binding (stages are re-run per cutoff below)
        self._register_requests()
        values: dict[int, dict] = {}
        done: set[str] = set()
        for r in eligible:
            cutoff = r["dataset_cutoff"]
            if cutoff not in done:
                # Reconstruct the label source as known at this cutoff, then compute its rows.
                done.add(cutoff)
                self.as_of = cutoff
                for st in a["stages"]:
                    self._run(st, self._params())
                for c in a["checks"]:
                    self._check(c)
                for v in self._sql(a["values"]["name"], a["values"]["sql"], self._params()).collect():
                    values[int(v["request_index"])] = v.asDict()
            v = values[r["request_index"]]
            if v["fl_error"]:
                raise FeatlasError(v["fl_error"], f"Row {r['request_index']}: label value failed ({v['fl_error']})", "SEM-LABEL-001", {"request_index": r["request_index"]})
            value = v["fl_value"]
            out["rows"].append(
                {
                    "request_index": r["request_index"],
                    "entity_keys": r["entity_keys"],
                    "as_of": r["as_of"],
                    "dataset_cutoff": cutoff,
                    "value": format_instant(value) if hasattr(value, "tzinfo") else value,
                    "state": v["fl_state"],
                    "eligible_rows": int(v["eligible_rows"]),
                }
            )
        return out
