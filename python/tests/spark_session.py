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
    (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET); nothing is read here.
    """
    utc_process()
    if LIVE:
        from databricks.connect import DatabricksSession  # type: ignore

        s = DatabricksSession.builder.serverless(True).getOrCreate()
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
