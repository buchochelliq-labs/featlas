"""Shared local Spark session for tests (Delta Lake enabled when delta-spark is installed)."""

from __future__ import annotations

import tempfile

from featlas_databricks.differential import utc_process

try:
    from delta import configure_spark_with_delta_pip  # type: ignore

    HAVE_DELTA = True
except ImportError:  # pragma: no cover
    HAVE_DELTA = False


def get_spark():
    utc_process()
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
