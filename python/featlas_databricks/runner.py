"""Execute core-generated Databricks artifacts in a caller-provided Spark session.

Order mirrors the canonical plan: session settings → read-only source binding
(skipping unavailable sources) → reconstruction/population views → feature
views (computed or degraded variant as decided by the core's coverage
states) → zero-row assertions → snapshot → (authorized) publication and
commit evidence. The runner never decides coverage, selection or values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .errors import FeatlasError, map_spark_exception

_PARAM = re.compile(r"(?<![:\w]):([a-z_][a-z0-9_]*)")


def parse_instant(text: str) -> datetime:
    """Parse a core-formatted UTC instant (``...Z``, up to 6 fractional digits)."""
    if not text.endswith("Z"):
        raise ValueError(f"expected UTC instant ending in Z: {text!r}")
    base = text[:-1]
    if "." in base:
        head, frac = base.split(".")
        base = f"{head}.{frac.ljust(6, '0')}"
        return datetime.strptime(base, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def format_instant(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)  # Spark sessions here run with UTC.
    value = value.astimezone(timezone.utc)
    text = value.strftime("%Y-%m-%dT%H:%M:%S")
    if value.microsecond:
        text += "." + f"{value.microsecond:06d}".rstrip("0")
    return text + "Z"


@dataclass
class PublicationAuthorization:
    """Explicit, per-run authorization. Absent authorization, nothing is written."""

    environment: str
    approved_by: str
    allow_create_table: bool = False
    publication_id: str = ""
    publication_sequence: int = 0
    materialization_id: str = ""


@dataclass
class SnapshotRun:
    spark: Any
    artifacts: dict
    as_of: str
    coverage: dict
    unavailable_sources: set[str] = field(default_factory=set)
    source_relations: dict[str, str] | None = None
    population_source: str | None = None
    log: list[dict] = field(default_factory=list)
    source_versions: dict[str, int] = field(default_factory=dict)

    def _run(self, st: dict, params: dict[str, Any] | None = None, sql: str | None = None):
        """Execute one generated statement; view statements register their bound SELECT."""
        df = self._sql(st["name"], sql if sql is not None else st["sql"], params)
        if st.get("view"):
            try:
                df.createOrReplaceTempView(st["view"])
            except Exception as exc:  # noqa: BLE001
                raise map_spark_exception(exc) from exc
        return df

    def _sql(self, name: str, sql: str, params: dict[str, Any] | None = None):
        used = set(_PARAM.findall(sql))
        args = {k: v for k, v in (params or {}).items() if k in used}
        missing = used - set(args)
        if missing:
            raise FeatlasError("UNBOUND_PARAMETER", f"{name} needs parameters {sorted(missing)}")
        self.log.append({"statement": name, "parameters": sorted(args)})
        try:
            return self.spark.sql(sql, args=args) if args else self.spark.sql(sql)
        except Exception as exc:  # noqa: BLE001 - mapped to stable codes
            raise map_spark_exception(exc) from exc

    def _params(self) -> dict[str, Any]:
        return {"as_of": parse_instant(self.as_of)}

    def _readable(self, item: dict) -> bool:
        return not (set(item.get("reads", [])) & self.unavailable_sources)

    def _drop_stale_views(self) -> None:
        """A reused session must never publish a view left over from another as_of."""
        for t in self.spark.catalog.listTables():
            if t.isTemporary and t.name.startswith("fl_"):
                self.spark.catalog.dropTempView(t.name)

    def prepare(self) -> None:
        a = self.artifacts
        if self.population_source and self.population_source in self.unavailable_sources:
            raise FeatlasError("SOURCE_UNAVAILABLE", f"Population source {self.population_source} is unavailable; the eligible population cannot be reconstructed", "SEM-ENT-001")
        self._drop_stale_views()
        for st in a["session"]:
            self._sql(st["name"], st["sql"])
        for st in a["bind_sources"]:
            if not self._readable(st):
                continue
            sql = st["sql"]
            source = st["reads"][0]
            table = a["expected_source_schemas"][source]["table"]
            if self.source_relations:
                sql = sql.replace(f"FROM {table}", f"FROM {self.source_relations[source]}")
            else:
                # Pin one immutable Delta snapshot per source so assertions, the snapshot and the
                # MERGE all read the same rows (lazy views otherwise re-scan live tables).
                version = self._sql(f"version_{source}", f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"]
                self.source_versions[source] = int(version)
                sql = sql.replace(f"FROM {table}", f"FROM {table} VERSION AS OF {int(version)}")
            self._run(st, sql=sql)
        for st in a["stages"]:
            if self._readable(st):
                self._run(st, self._params())

    def compute(self) -> None:
        for f in self.artifacts["features"]:
            cov = self.coverage[f["feature"]]
            if cov["variant"] == "computed":
                self._run(f["computed"], self._params())
            else:
                params = dict(self._params(), coverage_state=cov["coverage_state"])
                self._run(f["degraded"], params)

    def assert_contracts(self) -> list[dict]:
        """Run zero-row assertions; any returned row is a contract violation."""
        violations = []
        degraded = {f for f, cov in self.coverage.items() if cov["variant"] != "computed"}
        for c in self.artifacts["checks"]:
            if not self._readable(c) or c.get("feature") in degraded:
                continue
            rows = self._sql(c["name"], c["sql"], self._params()).limit(1).collect()
            if rows:
                violations.append({"check": c["name"], "code": c["code"], "rule": c["rule"], "example": rows[0].asDict()})
        return violations

    def materialize(self, publication: dict[str, Any]) -> list[dict]:
        """Build the snapshot view and materialize feature views to surface runtime errors."""
        params = dict(self._params(), **publication)
        st = self.artifacts["snapshot"]
        self._run(st, params)
        try:
            return [r.asDict() for r in self.spark.table(st["view"]).collect()]
        except Exception as exc:  # noqa: BLE001
            raise map_spark_exception(exc) from exc

    def run_to_snapshot(self, publication: dict[str, Any]) -> list[dict]:
        self.prepare()
        self.compute()
        violations = self.assert_contracts()
        if violations:
            v = violations[0]
            raise FeatlasError(v["code"], f"Contract assertion {v['check']} returned rows", v["rule"], violations)
        return self.materialize(publication)

    def publish(self, auth: PublicationAuthorization) -> dict:
        """Commit the snapshot in one Delta MERGE and record observed commit evidence.

        Requires explicit authorization; this is the only method that writes.
        """
        if not (auth.environment and auth.approved_by and auth.publication_id and auth.materialization_id):
            raise FeatlasError("PUBLICATION_NOT_AUTHORIZED", "Explicit environment, approver and publication identities are required")
        a = self.artifacts
        pub = {"publication_id": auth.publication_id, "publication_sequence": auth.publication_sequence, "materialization_id": auth.materialization_id}
        self.run_to_snapshot(pub)
        if auth.allow_create_table:
            self._sql("create_feature_table", a["create_table"]["sql"])
        props = {r["key"]: r["value"] for r in self._sql("table_properties", a["table_properties"]["sql"]).collect()}
        for k, v in a["expected_table_properties"].items():
            if props.get(k) != v:
                raise FeatlasError("TARGET_TABLE_MISMATCH", f"{a['output_table']} property {k}={props.get(k)!r}, expected {v!r}; refusing to publish into another feature-set version")
        # An earlier attempt of this publication may already have committed (idempotent retry).
        mc_rows = self._sql("existing_publication", f"SELECT DISTINCT * FROM (SELECT {self._meta('materialization_id')} AS m, {self._meta('published_at')} AS p FROM {a['output_table']} WHERE {self._meta('publication_id')} = :publication_id)", {"publication_id": auth.publication_id}).collect()
        commit_mid = mc_rows[0]["m"] if mc_rows else auth.materialization_id
        if not mc_rows:
            self.spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", auth.materialization_id)
            try:
                self._sql("publish_snapshot", a["publish"]["sql"], dict(self._params(), **pub))
            finally:
                self.spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
        history = self._sql("commit_history", a["evidence"][0]["sql"]).collect()
        commits = [h.asDict() for h in history if h.asDict().get("userMetadata") == commit_mid and h.asDict().get("operation") == "MERGE"]
        if len(commits) != 1:
            raise FeatlasError("PUBLICATION_EVIDENCE_MISSING", f"Expected exactly one MERGE commit with userMetadata {commit_mid}; found {len(commits)}. published_at stays NULL and replay cannot use this publication.")
        commit = commits[0]
        observed = commit["timestamp"]
        self._sql("record_published_at", a["evidence"][1]["sql"], {"observed_commit_timestamp": observed, "publication_id": auth.publication_id})
        micros = self.spark.sql("SELECT unix_micros(:t) AS u", args={"t": observed}).collect()[0]["u"]
        return {
            "table": a["output_table"],
            "table_version": commit.get("version"),
            "commit_timestamp": format_instant(datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(microsecond=micros % 1_000_000)),
            "source_versions": self.source_versions,
            "retry_of_materialization": commit_mid if commit_mid != auth.materialization_id else None,
            "operation": commit.get("operation"),
            "operation_metrics": commit.get("operationMetrics"),
            "publication_id": auth.publication_id,
            "publication_sequence": auth.publication_sequence,
            "materialization_id": auth.materialization_id,
            "approved_by": auth.approved_by,
            "environment": auth.environment,
            "statement_log": self.log,
        }

    def _meta(self, name: str) -> str:
        col = self.artifacts["metadata_columns"][name]
        return "`" + col.replace("`", "``") + "`"

    def preflight_sources(self) -> list[dict]:
        """Compare physical source schemas with the expected mapping (live targets)."""
        problems = []
        for source, spec in self.artifacts["expected_source_schemas"].items():
            if source in self.unavailable_sources:
                continue
            rows = self._sql(f"describe_{source}", f"DESCRIBE TABLE {spec['table']}").collect()
            actual = {r["col_name"]: r["data_type"].upper() for r in rows if r["col_name"] and not r["col_name"].startswith("#")}
            for col in spec["columns"]:
                got = actual.get(col["physical"])
                if got is None:
                    problems.append({"source": source, "column": col["physical"], "problem": "missing"})
                elif got != col["type"]:
                    problems.append({"source": source, "column": col["physical"], "problem": f"type {got} != {col['type']}"})
        return problems
