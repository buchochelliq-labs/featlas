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

        scopes = [s for s in os.environ.get("DATABRICKS_SCOPES", "").replace(",", " ").split() if s]
        cfg = Config(serverless_compute_id="auto", **({"scopes": scopes} if scopes else {}))
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
