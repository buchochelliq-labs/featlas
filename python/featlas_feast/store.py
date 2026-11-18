"""Feast store host for featlas feature-set targets (see the package docstring)."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

FEAST_TYPES = {"integer": "Int64", "number": "Float64", "string": "String", "boolean": "Bool", "timestamp": "UnixTimestamp", "date": "UnixTimestamp"}


def _utc(v: Any) -> datetime:
    if isinstance(v, str):
        v = datetime.fromisoformat(v.replace("Z", "+00:00"))
    return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)


def _iso(v: datetime) -> str:
    return _utc(v).strftime("%Y-%m-%dT%H:%M:%S.%fZ").replace(".000000Z", "Z")


class FeastStore:
    def __init__(self, gen: dict, feature_set: str, repo_dir: str | Path, consumer: str | None = None):
        """``gen``: a featlas generate result for a deployment whose output target is Feast."""
        self.gen = gen
        plan = gen["plan"]
        self.fs = next(f for f in plan["logical"]["feature_sets"] if f["id"] == feature_set)
        self.target = next(t for t in plan["bindings"]["outputs"] if t["feature_set"] == feature_set)
        adapter = next(a for a in plan["bindings"]["adapters"] if a["id"] == self.target["adapter"])
        if adapter["implementation"] != "dev.featlas:feast":
            raise ValueError(f"{feature_set} is published to {adapter['implementation']}, not Feast")
        self.project = adapter["configuration"]["project"]
        self.role = self.target["role"]
        self.repo = Path(repo_dir)
        self.data = self.repo / "data" / feature_set.replace(".", "_")
        self.data.mkdir(parents=True, exist_ok=True)
        self.meta = self.target["metadata_columns"]
        self.keys = [self.target["entity_key_mapping"][k["name"]] for k in self.fs["population"]["keys"]]
        self.key_types = {self.target["entity_key_mapping"][k["name"]]: k["type"] for k in self.fs["population"]["keys"]}
        types = {f["id"]: f["output"]["type"] for f in self.fs["features"]}
        self.columns = {}
        for m in self.target["feature_mapping"]:
            self.columns[m["column"]] = types[m["feature"]]
            self.columns[m["state_column"]] = "string"
        contracts = {c["id"]: c for c in plan["bindings"].get("consumer_contracts", [])}
        bound = [b["consumer"] for b in plan["bindings"]["consumers"]]
        name = consumer or (bound[0] if bound else None)
        self.consumer = name
        self.ttl = timedelta(seconds=int(contracts[name]["max_staleness_seconds"])) if name in contracts else timedelta(0)
        self.view = feature_set.replace(".", "_")
        self._store = None

    # --- Feast objects ------------------------------------------------------------------------

    @property
    def store(self):
        if self._store is None:
            from feast import FeatureStore
            from feast.repo_config import RepoConfig

            cfg = RepoConfig(project=self.project, registry=str(self.repo / "registry.db"), provider="local", online_store={"type": "sqlite", "path": str(self.repo / "online.db")}, offline_store={"type": "file"}, entity_key_serialization_version=3)
            self._store = FeatureStore(config=cfg)
        return self._store

    def apply(self) -> dict:
        from feast import Entity, FeatureView, Field, FileSource, ValueType
        from feast import types as ft

        vt = {"string": ValueType.STRING, "integer": ValueType.INT64}
        entities = [Entity(name=k, join_keys=[k], value_type=vt.get(self.key_types[k], ValueType.STRING)) for k in self.keys]
        # A directory of per-publication parquet files; the schema file guarantees the directory is
        # never empty (Feast infers nothing from it: every field is declared).
        self._write_schema_seed()
        # No created_timestamp_column: Feast needs it non-null, and published_at is observed only
        # after the file is visible. The host keeps one publication per as_of instead, so
        # Feast never has to break a tie between rows of equal as_of.
        source = FileSource(name=f"{self.view}_source", path=str(self.data), timestamp_field=self.meta["as_of"])
        fields = [Field(name=c, dtype=getattr(ft, FEAST_TYPES[t])) for c, t in self.columns.items()]
        fields += [Field(name=self.meta["publication_id"], dtype=ft.String), Field(name=self.meta["publication_sequence"], dtype=ft.Int64), Field(name=self.meta["materialization_id"], dtype=ft.String)]
        fv = FeatureView(name=self.view, entities=entities, ttl=self.ttl, schema=fields, source=source, online=self.role == "online_store")
        self.store.apply([*entities, fv])
        self._fv = fv
        return {"feature_view": self.view, "entities": self.keys, "ttl_seconds": int(self.ttl.total_seconds()), "fields": [f.name for f in fields], "source": str(self.data)}

    def _frame(self, rows: list[dict]):
        import pandas as pd

        cols = self.keys + list(self.columns) + [self.meta[k] for k in ("as_of", "published_at", "publication_sequence", "publication_id", "materialization_id")]
        df = pd.DataFrame([{c: r.get(c) for c in cols} for r in rows], columns=cols)
        for c in (self.meta["as_of"], self.meta["published_at"]):
            df[c] = pd.to_datetime(df[c], utc=True)
        for c, t in self.columns.items():
            if t == "integer":
                df[c] = df[c].astype("Int64")
            elif t == "number":
                df[c] = df[c].astype("float64")
            elif t == "boolean":
                df[c] = df[c].astype("boolean")
        df[self.meta["publication_sequence"]] = df[self.meta["publication_sequence"]].astype("int64")
        return df

    def _write_schema_seed(self) -> None:
        seed = self.data / "_schema.parquet"
        if not seed.exists():
            self._frame([]).to_parquet(seed, index=False)

    # --- offline publication ------------------------------------------------------------------

    def _observation_rows(self) -> list[dict]:
        p = self.data.parent / f"{self.view}_publications.jsonl"
        if not p.exists():
            return []
        return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]

    def _observations(self) -> dict[str, str]:
        return {o["publication_id"]: o["published_at"] for o in self._observation_rows()}

    def publish_offline(self, rows: list[dict], publication: dict) -> dict:
        """Write one publication atomically and record its observed visibility."""
        if not rows:
            raise ValueError("an empty snapshot publishes nothing to observe")
        pid = publication["publication_id"]
        if pid in self._observations():
            return {"publication_id": pid, "retry": True, "published_at": self._observations()[pid]}
        as_of = _iso(rows[0][self.meta["as_of"]])
        stored = {o.get("as_of") for o in self._observation_rows()}
        if as_of in stored:
            raise ValueError(f"SAME_AS_OF_REPUBLICATION: a publication for as_of {as_of} is already stored; Feast would choose between equal timestamps arbitrarily")
        final = self.data / f"pub-{publication['publication_sequence']:020d}-{hashlib.sha256(pid.encode()).hexdigest()[:12]}.parquet"
        fd, tmp = tempfile.mkstemp(dir=self.data.parent, suffix=".parquet.tmp")
        os.close(fd)
        self._frame([dict(r, **{self.meta["published_at"]: None}) for r in rows]).to_parquet(tmp, index=False)
        os.replace(tmp, final)  # atomic within one filesystem
        seen = self._observe_offline(pid, rows)
        with open(self.data.parent / f"{self.view}_publications.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"publication_id": pid, "as_of": as_of, "published_at": seen, "file": final.name}) + "\n")
        return {"surface": "offline_snapshot", "store": "dev.featlas:feast", "publication_id": pid, "file": final.name, "rows": len(rows), "published_at": seen, "retry": False, "evidence_source": "first complete read through Feast get_historical_features"}

    def _observe_offline(self, pid: str, rows: list[dict]) -> str:
        at = datetime.now(timezone.utc)
        as_of = rows[0][self.meta["as_of"]]
        got = self.historical([{**{k: r[k] for k in self.keys}, "as_of": as_of} for r in rows])
        if len(got) != len(rows) or any(g.get(self.meta["publication_id"]) != pid for g in got):
            raise RuntimeError(f"publication {pid} is not readable through Feast for every key")
        return _iso(at)

    def historical(self, requests: list[dict]) -> list[dict]:
        """Feast point-in-time join: per request (keys + as_of), the latest stored row with
        as_of <= request as_of within the TTL."""
        import pandas as pd

        if not requests:
            return []
        edf = pd.DataFrame([{**{k: r[k] for k in self.keys}, "event_timestamp": _utc(r["as_of"])} for r in requests])
        feats = [f"{self.view}:{c}" for c in [*self.columns, self.meta["publication_id"], self.meta["publication_sequence"]]]
        df = self.store.get_historical_features(entity_df=edf, features=feats, full_feature_names=False).to_df()
        out = []
        for r in df.to_dict(orient="records"):
            out.append({k: (None if _isna(v) else _plain(v)) for k, v in r.items()})
        return out

    def records(self) -> list[dict]:
        """Every stored row with its observed published_at, for the featlas consumer API."""
        import pandas as pd

        obs = self._observations()
        files = sorted(p for p in self.data.glob("pub-*.parquet"))
        if not files:
            return []
        df = pd.concat([pd.read_parquet(f) for f in files])
        out = []
        for r in df.to_dict(orient="records"):
            r = {k: (None if _isna(v) else _plain(v)) for k, v in r.items()}
            r[self.meta["published_at"]] = obs.get(r[self.meta["publication_id"]])
            r[self.meta["as_of"]] = _iso(r[self.meta["as_of"]])
            out.append(r)
        return out

    # --- online publication -------------------------------------------------------------------

    def publish_online(self, publication_id: str, as_of: Any, keys: list[dict]) -> dict:
        """Materialize up to as_of into the online store; observe the publication online."""
        start = _utc(as_of) - max(self.ttl, timedelta(days=1))
        self.store.materialize(start_date=start, end_date=_utc(as_of) + timedelta(seconds=1), feature_views=[self.view])
        at = datetime.now(timezone.utc)
        got = self.online(keys)
        served = [g.get(self.meta["publication_id"]) for g in got]
        if any(s != publication_id for s in served):
            raise RuntimeError(f"online store serves {served[:5]} instead of {publication_id}")
        return {"surface": "online_store", "store": "dev.featlas:feast", "publication_id": publication_id, "published_at": _iso(at), "keys": len(keys), "evidence_source": "get_online_features after materialize"}

    def online(self, keys: list[dict]) -> list[dict]:
        feats = [f"{self.view}:{c}" for c in [*self.columns, self.meta["publication_id"]]]
        d = self.store.get_online_features(features=feats, entity_rows=[{k: r[k] for k in self.keys} for r in keys]).to_dict()
        n = len(keys)
        return [{c: _plain(d[c][i]) if c in d else None for c in [*self.keys, *self.columns, self.meta["publication_id"]]} for i in range(n)]


def _isna(v: Any) -> bool:
    try:
        import pandas as pd

        return v is None or (not isinstance(v, (list, dict, str)) and bool(pd.isna(v)))
    except (TypeError, ValueError):
        return False


def _plain(v: Any) -> Any:
    if hasattr(v, "to_pydatetime"):
        return _iso(v.to_pydatetime())
    if isinstance(v, datetime):
        return _iso(v)
    if hasattr(v, "item"):
        return v.item()
    return v
