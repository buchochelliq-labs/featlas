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
    # An explicitly authorized larger budget, e.g. {"max_scan_bytes": ..., "max_output_rows": ...}
    # (SEM-BUDGET-001: exceeding a limit stops the run unless a larger budget is authorized).
    budget_override: dict | None = None


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _plain(row: dict) -> dict:
    from datetime import date, datetime

    return {k: format_instant(v) if isinstance(v, datetime) else v.isoformat() if isinstance(v, date) else v for k, v in row.items()}


def _finding(feature: str, rule: dict, row: dict) -> dict:
    """A quality finding in the core's shape (eval::QualityFinding)."""
    row = _plain(row)
    value, state = row.pop("value"), row.pop("state")
    return {
        "feature": feature,
        "check": rule["check"],
        "check_index": rule["rank"] - 4,
        "severity": "warn" if rule["outcome"] == "warning" else "error",
        "outcome": rule["outcome"],
        "entity_keys": row,
        "value": value,
        "state": state,
    }


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
    # Warning-severity findings of the last execute() (values unchanged, SEM-QUAL-001).
    quality_warnings: list[dict] = field(default_factory=list)

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
        """A reused session must never publish a view left over from another as_of.

        Only this artifact's own views are dropped; other feature sets' views and caller views
        (for example ``fl_request_keys``) are left alone.
        """
        a = self.artifacts
        own = {st["view"] for st in a["bind_sources"] + a["stages"] + [a["snapshot"]] if st.get("view")}
        own |= {f[v]["view"] for f in a["features"] for v in ("computed", "degraded") if f[v].get("view")}
        temporary = {t.name for t in self.spark.catalog.listTables() if t.isTemporary}
        for name in sorted(own & temporary):
            self.spark.catalog.dropTempView(name)

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

    def _check(self, c: dict) -> None:
        """Run one zero-row assertion; a returned row raises its stable code."""
        rows = self._sql(c["name"], c["sql"], self._params()).limit(1).collect()
        if rows:
            raise FeatlasError(c["code"], f"Contract assertion {c['name']} returned rows", c["rule"], {"check": c["name"], "example": rows[0].asDict()})

    def execute(self) -> None:
        """Assertions and feature views in the core's canonical order; raises at the first violation.

        1. Untagged assertions (row validity, reconstruction, population) in generated order.
        2. Per feature in plan order: register its view (computed or degraded variant); for computed
           features run its transform assertions, then materialize the view so runtime errors
           (overflow, division by zero, non-finite, empty-error) surface before later features.
        3. Per feature, its output/quality rules in rank order: the first `error` rule with rows
           raises; `warning` rows are recorded; `quarantine` rows are collected and, once every
           feature has passed its error rules, raise QUALITY_QUARANTINED so nothing is published.
        """
        a = self.artifacts
        self.quality_warnings = []
        quarantined: list[dict] = []
        for c in a["checks"]:
            if c.get("feature") is None and self._readable(c):
                self._check(c)
        for f in a["features"]:
            cov = self.coverage[f["feature"]]
            if cov["variant"] == "computed":
                for c in a["checks"]:
                    if c.get("feature") == f["feature"] and self._readable(c):
                        self._check(c)
                self._run(f["computed"], self._params())
            else:
                self._run(f["degraded"], dict(self._params(), coverage_state=cov["coverage_state"]))
            view = (f["computed"] if cov["variant"] == "computed" else f["degraded"])["view"]
            try:
                self.spark.table(view).count()
            except Exception as exc:  # noqa: BLE001
                raise map_spark_exception(exc) from exc
            for q in f.get("quality", []):
                rows = [r.asDict() for r in self._sql(q["name"], q["sql"]).collect()]
                if not rows:
                    continue
                if q["outcome"] == "error":
                    raise FeatlasError(q["code"], f"{f['feature']}: {q['check']} rule {q['name']} returned rows", q["rule"], {"check": q["name"], "example": _plain(rows[0])})
                findings = quarantined if q["outcome"] == "quarantine" else self.quality_warnings
                for row in rows:
                    findings.append(_finding(f["feature"], q, row))
        if quarantined:
            raise FeatlasError(
                "QUALITY_QUARANTINED",
                f"{len(quarantined)} value(s) violate error-level quality checks under on_violation: quarantine; the snapshot is not published",
                "SEM-QUAL-001",
                {"quarantined": quarantined},
            )

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
        self.execute()
        return self.materialize(publication)

    def publish(self, auth: PublicationAuthorization) -> dict:
        """Commit the snapshot in one Delta MERGE and record observed commit evidence.

        Requires explicit authorization; this is the only method that writes.
        """
        if not (auth.environment and auth.approved_by and auth.publication_id and auth.materialization_id):
            raise FeatlasError("PUBLICATION_NOT_AUTHORIZED", "Explicit environment, approver and publication identities are required")
        a = self.artifacts
        pub = {"publication_id": auth.publication_id, "publication_sequence": auth.publication_sequence, "materialization_id": auth.materialization_id}
        if auth.allow_create_table:
            self._sql("create_feature_table", a["create_table"]["sql"])
        props = {r["key"]: r["value"] for r in self._sql("table_properties", a["table_properties"]["sql"]).collect()}
        for k, v in a["expected_table_properties"].items():
            if props.get(k) != v:
                raise FeatlasError("TARGET_TABLE_MISMATCH", f"{a['output_table']} property {k}={props.get(k)!r}, expected {v!r}; refusing to publish into another feature-set version")
        # Look up an earlier attempt of this publication *before* recomputing, so a retry can
        # always finish recording evidence for rows that are already committed.
        existing = self._sql(
            "existing_publication",
            f"SELECT {self._meta('materialization_id')} AS m, {self._meta('publication_sequence')} AS s, "
            f"unix_micros({self._meta('as_of')}) = unix_micros(:as_of) AS same_as_of, count(*) AS n "
            f"FROM {a['output_table']} WHERE {self._meta('publication_id')} = :publication_id GROUP BY 1, 2, 3",
            dict(self._params(), publication_id=auth.publication_id),
        ).collect()
        retry = bool(existing)
        if retry:
            if len(existing) != 1:
                raise FeatlasError("PUBLICATION_IDENTITY_CONFLICT", f"publication_id {auth.publication_id} already has {len(existing)} distinct (materialization, sequence, as_of) groups; refusing to guess")
            row = existing[0]
            if not row["same_as_of"] or row["s"] != auth.publication_sequence:
                raise FeatlasError("PUBLICATION_IDENTITY_CONFLICT", f"publication_id {auth.publication_id} is already committed with a different as_of or sequence ({row['s']}); use a new publication_id")
            commit_mid, stored_sequence, stored_rows = row["m"], row["s"], row["n"]
        else:
            budget = self.budget_preflight(auth.budget_override)
            rows = self.run_to_snapshot(pub)
            self._check_output_rows(len(rows), auth.budget_override)
            budget["output_rows"] = len(rows)
            after_version = self._latest_version()
            # Best effort: label the commit with the materialization id. Serverless compute may
            # reject this conf; evidence lookup does not depend on it (change data feed below).
            labelled = True
            try:
                self.spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", auth.materialization_id)
            except Exception:  # noqa: BLE001
                labelled = False
            try:
                self._sql("publish_snapshot", a["publish"]["sql"], dict(self._params(), **pub))
            finally:
                if labelled:
                    try:
                        self.spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
                    except Exception:  # noqa: BLE001
                        pass
            commit_mid, stored_sequence, stored_rows = auth.materialization_id, auth.publication_sequence, len(rows)
        commit = self._inserting_commit(auth.publication_id, None if retry else after_version)
        # Rows of a publication are only ever inserted by one MERGE (a retry never writes), so the
        # inserting commit must hold every stored row.
        if commit["rows"] != stored_rows:
            raise FeatlasError("PUBLICATION_EVIDENCE_MISSING", f"The inserting commit holds {commit['rows']} rows but the publication has {stored_rows}; the snapshot was not committed atomically")
        observed = commit["timestamp"]
        self._sql("record_published_at", a["evidence"][1]["sql"], {"observed_commit_timestamp": observed, "publication_id": auth.publication_id})
        micros = self.spark.sql("SELECT unix_micros(:t) AS u", args={"t": observed}).collect()[0]["u"]
        return {
            "table": a["output_table"],
            "table_version": commit.get("version"),
            "commit_timestamp": format_instant(datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(microsecond=micros % 1_000_000)),
            # Identities stored on the committed rows (on a retry these are the original write's).
            "publication_id": auth.publication_id,
            "publication_sequence": stored_sequence,
            "materialization_id": commit_mid,
            "rows": stored_rows,
            "retry": retry,
            "retry_materialization_id": auth.materialization_id if retry else None,
            # Source pins, budget use and quality warnings are only known for the run that computed the rows.
            "source_versions": None if retry else self.source_versions,
            "budget": None if retry else budget,
            "quality_warnings": None if retry else self.quality_warnings,
            "evidence_source": "change data feed (table_changes)",
            "approved_by": auth.approved_by,
            "environment": auth.environment,
            "statement_log": self.log,
        }

    # --- SEM-BUDGET-001 / SEM-DEPLOY-002 -------------------------------------------------------

    def _limit(self, name: str, override: dict | None) -> int:
        b = self.artifacts.get("budget") or {}
        return int((override or {}).get(name) or b.get(name) or 0)

    def budget_preflight(self, override: dict | None = None) -> dict:
        """Upper bound of the bytes this run scans (full size of every read source table) against
        ``max_scan_bytes``; raises BUDGET_EXCEEDED rather than sampling or truncating."""
        limit = self._limit("max_scan_bytes", override)
        scanned, measured = 0, {}
        if not self.source_relations:
            for source, spec in self.artifacts["expected_source_schemas"].items():
                if source in self.unavailable_sources:
                    continue
                detail = self._sql(f"detail_{source}", f"DESCRIBE DETAIL {spec['table']}").collect()[0].asDict()
                measured[source] = int(detail.get("sizeInBytes") or 0)
                scanned += measured[source]
            if limit and scanned > limit:
                raise FeatlasError("BUDGET_EXCEEDED", f"Sources total {scanned} bytes, over max_scan_bytes {limit}; authorize a larger budget explicitly to proceed", "SEM-BUDGET-001", {"measured": measured, "limit": limit})
        return {
            "max_scan_bytes": limit,
            "scan_bytes_upper_bound": scanned if not self.source_relations else None,
            "per_source_bytes": measured,
            "max_output_rows": self._limit("max_output_rows", override),
            "overridden": bool(override),
            "not_enforced_by_host": ["max_daily_cost (no cost signal available to the host)", "max_memory_mib (serverless compute sizes itself)", "max_concurrent_runs (enforced by the job's max_concurrent_runs / the workflow concurrency group)"],
        }

    def _check_output_rows(self, n: int, override: dict | None) -> None:
        limit = self._limit("max_output_rows", override)
        if limit and n > limit:
            raise FeatlasError("BUDGET_EXCEEDED", f"Snapshot has {n} rows, over max_output_rows {limit}; nothing was published", "SEM-BUDGET-001", {"rows": n, "limit": limit})

    def retention_preflight(self) -> list[dict]:
        """Verify retention contracts against the physical tables (read-only).

        * every source must physically retain rows back to its declared
          ``earliest_reproducible_at`` (oldest availability time <= the declared instant), else
          history the contract promises is gone (RETENTION_CONTRACT_VIOLATED);
        * an existing output table must keep its change data feed on, which publication evidence
          needs (CHANGE_DATA_FEED_DISABLED).
        """
        problems = []
        ret = self.artifacts.get("retention") or {}
        for source, chk in (ret.get("sources") or {}).items():
            if source in self.unavailable_sources:
                continue
            oldest = self._sql(f"oldest_{source}", f"SELECT min({_q(chk['available_time'])}) AS t FROM {chk['table']}").collect()[0]["t"]
            declared = parse_instant(chk["earliest_reproducible_at"]).replace(tzinfo=None)
            if oldest is not None and oldest.replace(tzinfo=None) > declared:
                problems.append({"code": "RETENTION_CONTRACT_VIOLATED", "rule": "SEM-DEPLOY-002", "source": source, "table": chk["table"], "declared_earliest_reproducible_at": chk["earliest_reproducible_at"], "oldest_retained": format_instant(oldest)})
        try:
            props = {r["key"]: r["value"] for r in self._sql("table_properties", self.artifacts["table_properties"]["sql"]).collect()}
        except FeatlasError:
            props = None  # the output table does not exist yet; its DDL enables the change data feed
        if props is not None and props.get("delta.enableChangeDataFeed", "false").lower() != "true":
            problems.append({"code": "CHANGE_DATA_FEED_DISABLED", "rule": "SEM-PORT-009", "table": self.artifacts["output_table"], "detail": "publication evidence is read from the change data feed"})
        return problems

    def _evidence(self, name: str) -> dict:
        return next(st for st in self.artifacts["evidence"] if st["name"] == name)

    def _history(self) -> list:
        return [r.asDict() for r in self._sql("commit_candidates", self._evidence("commit_candidates")["sql"]).collect()]

    def _latest_version(self) -> int:
        return max(int(r["version"]) for r in self._history())

    def _inserting_commit(self, publication_id: str, after_version: int | None) -> dict:
        """The commit that inserted the publication's rows, read from the change data feed one
        retained MERGE commit at a time (newest first). After a fresh write only commits newer
        than ``after_version`` (the table version before the MERGE) are considered."""
        candidates = sorted(
            (int(r["version"]) for r in self._history() if r["operation"] == "MERGE" and (after_version is None or int(r["version"]) > after_version)),
            reverse=True,
        )
        unreadable = []
        for v in candidates:
            try:
                found = self._sql("commit_history", self._evidence("commit_history")["sql"], {"publication_id": publication_id, "from_version": v, "to_version": v}).collect()
            except Exception as e:  # noqa: BLE001 - change data for this version may have been cleaned up
                unreadable.append(f"{v}: {type(e).__name__}")
                continue
            if found:
                return found[0].asDict()
        why = f"; change data unreadable for versions {unreadable}" if unreadable else ""
        raise FeatlasError("PUBLICATION_EVIDENCE_MISSING", f"No retained MERGE commit inserts publication {publication_id} (checked {len(candidates)}){why}. published_at stays NULL and replay cannot use this publication.")

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
