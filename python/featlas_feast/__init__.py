"""Feast (open-source feature store) as a featlas offline/online store: ``dev.featlas:feast@0.1.0``.

featlas computes complete feature-set snapshots (here with the Spark SQL lowering, see
``featlas_databricks.runner.SnapshotRun``); this host publishes them to Feast and reads them back:

* ``FeastStore.apply()`` registers one Feast entity per entity key and one feature view per
  feature set (fields: every feature value and state column plus the publication metadata;
  ``timestamp_field`` = as_of, one publication per as_of (Feast needs no tie-break), TTL = the bound
  consumer's ``max_staleness_seconds``) over the target parquet directory.
* ``publish_offline(rows, publication)`` writes one parquet file per publication and renames it
  into the directory (all of a publication or none of it is visible), then reads it back through
  Feast; the instant of that first complete read is the observed ``published_at``, recorded in
  ``_publications.jsonl`` beside the data and returned as evidence. Rows in the parquet file carry
  ``published_at`` = NULL; ``records()`` joins the observed instants for the core consumer API.
* ``historical(requests)`` is Feast's point-in-time join; ``consume(requests)`` is the featlas
  consumer API over the same stored records. They agree on source-as-known reads.
* ``publish_online(publication, keys)`` materializes into the online store (SQLite locally) and
  observes visibility with ``get_online_features``.

Nothing here computes a feature value, and no credential is involved (local stores only).
"""

from .store import FeastStore

__all__ = ["FeastStore"]
