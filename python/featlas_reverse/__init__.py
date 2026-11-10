"""featlas_reverse: from an existing feature table and its code to a featlas specification.

* ``analyze``      — static analysis of the feature SQL (sqlglot, Databricks dialect);
* ``describe``     — table descriptions (from JSON or read-only from a Spark/Unity Catalog table);
* ``draft``        — draft a spec with an explicit assumptions/questions report;
* ``characterize`` — legacy outputs as characterization goldens;
* ``split``        — split a wide feature set into smaller feature tables (metrics + apply);
* ``parity``       — SQL proving the regenerated tables reproduce the legacy table;
* ``mcp_server``   — the same operations as a read-only MCP stdio server.

The featlas core (``featlas`` bindings) validates, tests and generates; nothing here re-implements
specification semantics.
"""

__all__ = ["analyze", "describe", "draft", "characterize", "split", "parity"]
