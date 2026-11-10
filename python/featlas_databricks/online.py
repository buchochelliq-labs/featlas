"""Online publication to a Databricks Online Feature Store (adapter role ``online_store``).

The generated artifacts of an ``online_store`` target stage the complete feature-set snapshot in
a Unity Catalog feature table that keeps one row per entity (primary key: entity keys plus
``as_of TIMESERIES``; change data feed on), replaced by one Delta MERGE per publication
(``SnapshotRun.publish``). This module makes that snapshot visible online and records *observed*
online visibility (SEM-PORT-009):

1. ``ensure_store``: the Online Feature Store (Lakebase) exists and is AVAILABLE; it is created
   only with ``allow_create``.
2. ``sync``: ``publish_table`` (TRIGGERED) creates the online table on first use and incrementally
   syncs the staged rows.
3. ``observe``: the Feature Serving endpoint (a FeatureSpec over the staging table) is queried for
   the published keys until every key returns this publication's ``publication_id``; the instant
   of the first complete observation is written to the staged rows' ``published_at``. A staging
   commit, a sync-pipeline success or a job end is never used as online visibility.

Every resource-creating call (online store, online table, FeatureSpec, endpoint) needs
``OnlineAuthorization`` with ``allow_create``; ``teardown`` removes what a test created.
Credentials come from the Databricks SDK unified auth; none are accepted here.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .errors import FeatlasError
from .runner import format_instant


@dataclass
class OnlineAuthorization:
    environment: str
    approved_by: str
    allow_create: bool = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


class OnlinePublisher:
    def __init__(self, spark, artifacts: dict, fe=None, workspace=None, sleep: Callable[[float], None] = time.sleep):
        if artifacts.get("surface") != "online_store" or not artifacts.get("online"):
            raise FeatlasError("NOT_AN_ONLINE_TARGET", "These artifacts publish an offline snapshot; OnlinePublisher needs an online_store target")
        self.spark = spark
        self.a = artifacts
        self.o = artifacts["online"]
        self._fe = fe
        self._w = workspace
        self.sleep = sleep
        self.log: list[dict] = []

    # --- clients (lazy: importing this module needs no Databricks packages) -------------------

    @property
    def fe(self):
        if self._fe is None:
            from databricks.feature_engineering import FeatureEngineeringClient  # type: ignore

            self._fe = FeatureEngineeringClient()
        return self._fe

    @property
    def w(self):
        if self._w is None:
            from databricks.sdk import WorkspaceClient  # type: ignore

            self._w = WorkspaceClient()
        return self._w

    def _step(self, step: str, **kw: Any) -> None:
        self.log.append({"step": step, "at": format_instant(_now()), **kw})

    # --- store and sync -------------------------------------------------------------------------

    def ensure_store(self, auth: OnlineAuthorization, timeout_s: float = 1200) -> dict:
        store = None
        try:
            store = self.fe.get_online_store(name=self.o["store"])
        except Exception:  # noqa: BLE001 - not found is reported as None or raised, by version
            store = None
        if store is None:
            if not auth.allow_create:
                raise FeatlasError("ONLINE_STORE_MISSING", f"Online store {self.o['store']} does not exist; creating it needs explicit authorization")
            self._step("create_online_store", name=self.o["store"], capacity=self.o["capacity"])
            store = self.fe.create_online_store(name=self.o["store"], capacity=self.o["capacity"])
        deadline = time.monotonic() + timeout_s
        while str(getattr(store, "state", "")).upper().split(".")[-1] != "AVAILABLE":
            if time.monotonic() > deadline:
                raise FeatlasError("ONLINE_STORE_UNAVAILABLE", f"Online store {self.o['store']} is {getattr(store, 'state', None)} after {timeout_s}s")
            self.sleep(15)
            store = self.fe.get_online_store(name=self.o["store"])
        self._step("online_store_available", name=self.o["store"], state=str(store.state))
        return {"name": store.name, "state": str(store.state), "capacity": str(getattr(store, "capacity", ""))}

    def sync(self, auth: OnlineAuthorization) -> dict:
        """Create the online table on first use and trigger an incremental sync of staged rows."""
        if not auth.allow_create and not self._online_table_exists():
            raise FeatlasError("ONLINE_TABLE_MISSING", f"{self.o['online_table']} does not exist; creating it needs explicit authorization")
        store = self.fe.get_online_store(name=self.o["store"])
        self._step("publish_table", source=self.a["output_table"], online_table=self.o["online_table"])
        res = self.fe.publish_table(online_store=store, source_table_name=self.a["output_table"], online_table_name=self.o["online_table"], publish_mode="TRIGGERED")
        return {"online_table": self.o["online_table"], "result": str(res)[:500]}

    def _online_table_exists(self) -> bool:
        try:
            self.w.tables.get(self.o["online_table"])
            return True
        except Exception:  # noqa: BLE001
            return False

    # --- serving and observation ----------------------------------------------------------------

    def feature_spec_name(self) -> str:
        ns = self.a["output_table"].rsplit(".", 1)[0]
        return f"{ns}.{self.a['feature_set'].replace('.', '_')}_online_spec"

    def ensure_endpoint(self, auth: OnlineAuthorization, timeout_s: float = 1800) -> str:
        name = self.o.get("serving_endpoint")
        if not name:
            raise FeatlasError("ONLINE_ENDPOINT_UNCONFIGURED", "Observing online visibility needs online_store.serving_endpoint in the adapter configuration")
        try:
            self.w.serving_endpoints.get(name)
            return name
        except Exception:  # noqa: BLE001
            pass
        if not auth.allow_create:
            raise FeatlasError("ONLINE_ENDPOINT_MISSING", f"Serving endpoint {name} does not exist; creating it needs explicit authorization")
        from databricks.feature_engineering.entities.feature_lookup import FeatureLookup  # type: ignore
        from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput  # type: ignore

        spec = self.feature_spec_name()
        cols = self.o["feature_columns"] + [self.o["as_of_column"], self.o["publication_id_column"]]
        try:
            self.fe.create_feature_spec(name=spec, features=[FeatureLookup(table_name=self.a["output_table"], lookup_key=self.o["lookup_keys"], feature_names=cols)])
            self._step("create_feature_spec", name=spec)
        except Exception as exc:  # noqa: BLE001 - an existing spec is reused
            if "already exists" not in str(exc).lower():
                raise
        self._step("create_serving_endpoint", name=name, entity=spec)
        self.w.serving_endpoints.create_and_wait(
            name=name,
            config=EndpointCoreConfigInput(served_entities=[ServedEntityInput(entity_name=spec, scale_to_zero_enabled=True, workload_size="Small")]),
            timeout=__import__("datetime").timedelta(seconds=timeout_s),
        )
        return name

    def lookup(self, keys: list[dict]) -> list[dict]:
        """Feature values served online for entity keys (physical key names)."""
        resp = self.w.serving_endpoints.query(name=self.o["serving_endpoint"], dataframe_records=keys)
        out = resp.outputs if hasattr(resp, "outputs") else resp.as_dict().get("outputs")
        return list(out or [])

    def observe(self, publication_id: str, keys: list[dict], timeout_s: float = 1800, poll_s: float = 10) -> dict:
        """Poll the endpoint until every key serves ``publication_id``; return the observation."""
        pid = self.o["publication_id_column"]
        deadline = time.monotonic() + timeout_s
        polls = 0
        while True:
            polls += 1
            at = _now()
            try:
                rows = self.lookup(keys)
                served = [r.get(pid) for r in rows]
            except Exception as exc:  # noqa: BLE001 - endpoint warming up or syncing
                rows, served = [], [f"error: {type(exc).__name__}"]
            if len(rows) == len(keys) and all(s == publication_id for s in served):
                self._step("online_visible", publication_id=publication_id, polls=polls)
                return {"observed_at": format_instant(at), "polls": polls, "rows": rows}
            if time.monotonic() > deadline:
                raise FeatlasError("ONLINE_VISIBILITY_NOT_OBSERVED", f"{publication_id} was not served for all {len(keys)} keys within {timeout_s}s (last served: {served[:5]})")
            self.sleep(poll_s)

    def publish(self, staged: dict, auth: OnlineAuthorization, sample_keys: list[dict], timeout_s: float = 1800) -> dict:
        """After ``SnapshotRun.publish`` staged a publication: sync it and observe it online."""
        if not (auth.environment and auth.approved_by):
            raise FeatlasError("PUBLICATION_NOT_AUTHORIZED", "Explicit environment and approver are required")
        if not sample_keys:
            raise FeatlasError("ONLINE_SAMPLE_EMPTY", "Online visibility is observed on published keys; an empty population publishes nothing to observe")
        store = self.ensure_store(auth)
        sync = self.sync(auth)
        endpoint = self.ensure_endpoint(auth)
        seen = self.observe(staged["publication_id"], sample_keys, timeout_s)
        # Record the observed online instant on the staged rows of this publication only.
        self.spark.sql(
            self.a["evidence"][1]["sql"],
            args={"observed_commit_timestamp": datetime.fromisoformat(seen["observed_at"].replace("Z", "+00:00")).replace(tzinfo=None), "publication_id": staged["publication_id"]},
        )
        return {
            "surface": "online_store",
            "online_store": store,
            "online_table": self.o["online_table"],
            "serving_endpoint": endpoint,
            "publication_id": staged["publication_id"],
            "staged_commit_timestamp": staged.get("commit_timestamp"),
            "published_at": seen["observed_at"],
            "observation": {"keys": len(sample_keys), "polls": seen["polls"]},
            "sync": sync,
            "steps": self.log,
        }

    def teardown(self, delete_store: bool = False) -> list[str]:
        """Remove test resources: endpoint, feature spec, online table and (optionally) store."""
        done = []
        for what, fn in (
            ("serving_endpoint", lambda: self.o.get("serving_endpoint") and self.w.serving_endpoints.delete(self.o["serving_endpoint"])),
            ("feature_spec", lambda: self.fe.delete_feature_spec(name=self.feature_spec_name())),
            ("online_table", lambda: self.w.feature_store.delete_online_table(online_table_name=self.o["online_table"])),
            ("online_store", lambda: delete_store and self.fe.delete_online_store(name=self.o["store"])),
        ):
            try:
                fn()
                done.append(what)
            except Exception as exc:  # noqa: BLE001 - best effort cleanup, reported
                done.append(f"{what}: {type(exc).__name__}: {str(exc)[:200]}")
        return done
