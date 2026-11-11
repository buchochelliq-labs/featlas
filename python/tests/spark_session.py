"""Shared local Spark session for tests (Delta Lake enabled when delta-spark is installed)."""

from __future__ import annotations

import os
import tempfile

from featlas_databricks.differential import utc_process

try:
    from delta import configure_spark_with_delta_pip  # type: ignore

    HAVE_DELTA = True
except ImportError:  # pragma: no cover
    HAVE_DELTA = False


LIVE = os.environ.get("FEATLAS_SPARK") == "databricks"


def scopes() -> list[str]:
    return [s for s in os.environ.get("DATABRICKS_SCOPES", "").replace(",", " ").split() if s]


def apply_default_scopes() -> None:
    """Make every Databricks SDK config built without explicit scopes use DATABRICKS_SCOPES.

    Clients such as the Feature Engineering client build their own SDK config, which would
    request ``all-apis`` and be refused by a scoped service-principal secret. Live tests only.
    """
    sc = scopes()
    if not sc:
        return
    from databricks.sdk.core import Config  # type: ignore

    if getattr(Config, "_featlas_scoped", False):
        return
    orig = Config.__init__

    def init(self, *a, **kw):
        if not kw.get("scopes"):  # clients pass scopes=None explicitly
            kw["scopes"] = list(sc)
        orig(self, *a, **kw)
        if self.scopes:
            self.as_dict()["scopes"] = tuple(self.scopes)

    Config.__init__ = init
    Config._featlas_scoped = True


def apply_mlflow_scoped_creds() -> list[str]:
    """Let the Feature Engineering client authenticate with the scoped OAuth M2M credentials.

    Its REST helper resolves credentials through MLflow's ``get_databricks_host_creds``, which in
    platform run 42984205307 sent none (HTTP 401 "Credential was not sent or was of an
    unsupported type for this API") while the SDK's scoped token was accepted by the same API.
    This hands MLflow a bearer token minted from the scoped SDK config on each call (in memory
    only; never logged). Returns the patched module names. Live tests only.
    """
    import sys

    try:
        import importlib

        importlib.import_module("databricks.feature_engineering")  # load its modules before patching
        from mlflow.utils import databricks_utils as du
        from mlflow.utils.rest_utils import MlflowHostCreds
    except ImportError:
        return []
    from databricks.sdk.core import Config  # type: ignore

    cfg = Config()

    def creds(*_a, **_k):
        auth = cfg.authenticate().get("Authorization", "")
        return MlflowHostCreds(host=cfg.host, token=auth.split(" ", 1)[1] if " " in auth else None)

    patched = []
    for name, mod in list(sys.modules.items()):
        if (name.startswith(("databricks.ml_features", "databricks.feature_engineering", "mlflow")) or mod is du) and callable(getattr(mod, "get_databricks_host_creds", None)):
            mod.get_databricks_host_creds = creds
            patched.append(name)
    return sorted(patched)


def get_spark():
    """Local Spark (default) or, with FEATLAS_SPARK=databricks, Databricks serverless via Databricks Connect.

    Live mode authenticates with the Databricks SDK's unified auth environment variables
    (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET); no credential is read here.
    The SDK requests the OAuth scope ``all-apis`` unless scopes are set explicitly and has no
    environment variable for them, so DATABRICKS_SCOPES (space/comma separated, non-secret) is
    passed through for service-principal secrets bound to narrower scopes.
    """
    utc_process()
    if LIVE:
        from databricks.connect import DatabricksSession  # type: ignore
        from databricks.sdk.core import Config  # type: ignore

        sc = scopes()
        cfg = Config(serverless_compute_id="auto", **({"scopes": sc} if sc else {}))
        if cfg.scopes:
            # Databricks Connect caches sessions keyed by a hash of the SDK config's values, and
            # the SDK stores scopes as a list (unhashable). The SDK only joins/iterates them.
            cfg.as_dict()["scopes"] = tuple(cfg.scopes)
        s = DatabricksSession.builder.sdkConfig(cfg).getOrCreate()
        s.sql("SET TIME ZONE 'UTC'")
        return s
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master("local[1]")
        .appName("featlas-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="featlas-wh-"))
    )
    if HAVE_DELTA:
        b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        return configure_spark_with_delta_pip(b).getOrCreate()
    return b.getOrCreate()
