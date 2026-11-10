# Contributing

**In plain terms:** semantics belong in the Rust core and nowhere else; everything around it is
a thin translation layer. Every behaviour change needs a test whose expected answer was worked
out independently, and anything that changes what runs on Databricks invalidates the live
evidence until the live workflow is run again.

## Where a change goes

| Change | Place | Also update |
| --- | --- | --- |
| Meaning of the language (validation, evaluation, selection, labels, quality) | `crates/featlas-core` | Goldens/negative tests; the Spark lowering if it is lowered; [DECISIONS.md](DECISIONS.md) for a new interpretation |
| Spark SQL lowering | `crates/featlas-adapters/src/databricks.rs` | Differential tests; this changes the lock digest (see below) |
| Capability assessment | `crates/featlas-adapters/src/databricks_caps.rs` | Kept separate so it does not change the lowering pin |
| Databricks execution order / publication | `python/featlas_databricks/runner.py` | Also pinned by the lock |
| A new API operation | `crates/featlas-api`, then CLI (`commands.rs`), MCP (`mcp.rs`) and bindings (`crates/featlas-py`, `python/featlas`) | Transport parity test |
| Text output | `crates/featlas-cli/src/render.rs` only | JSON and MCP output must stay byte-stable |
| Reverse engineering | `python/featlas_reverse` | `tests/test_reverse*.py`, `examples/reverse/regenerate.sh` outputs, the skills if the workflow changes |
| Example-derived projects | never by hand: `projects/derive.py`, `projects/quality/derive.py` | — |
| Documentation site | `docs/` (new pages), `mkdocs.yml` | Run `zensical build --strict` |

Rules that tests enforce:

* The core must not depend on vendor code
  (`rebinding.rs::core_crate_has_no_vendor_dependencies`).
* Model structs deny unknown fields and have no serde defaults (`model.rs`).
* Every diagnostic carries a code, stage, SEM rule and JSON-pointer path (`diag.rs`).
* Core and generated Spark SQL report the same first diagnostic (DECISIONS D8; differential
  harness).
* Goldens are never regenerated from the implementation (SEM-TEST-002).

## Build and test locally

```sh
cargo build --release
cargo test --release
cd python
pip install "maturin>=1.16,<2" pytest PyYAML==6.0.3 sqlglot==30.30.0
maturin develop --release
pytest -q tests/test_native.py tests/test_reverse_mcp.py
# with Java 17+ and: pip install pyspark==4.0.1 delta-spark==4.0.0
PYSPARK_PYTHON=$(which python) pytest -q tests/test_local_spark_differential.py \
  tests/test_local_delta_publication.py tests/test_reverse.py
```

## Changing `databricks.rs` or `runner.py`

Both files are embedded in the binary and hashed. Any byte change makes every Databricks check
fall back to `unverified` until a live run on the new bytes is pinned in `databricks_lock.json`
([Re-pinning](guides/databricks.md#re-pinning-the-adapter-lock)). Pull requests touching them (or
the core, the host, `python/tests/**`, `projects/live/**`) trigger the live workflow, which needs
the `databricks-free` environment's configuration.

## Records

The files under `docs/` that are not part of this site's own pages — `DECISIONS.md`,
`EVIDENCE.md`, `TRACEABILITY.md`, `SPEC_FINDINGS.md`, `DATABRICKS_RUNBOOK.md`, `ODCS.md` and
`docs/evidence/` — are records. They are linked from this site but edited only as part of the
work they record. Feature status for the site lives in [status.md](status.md).

## Documentation site

```sh
pip install zensical==0.1.0
zensical build --strict      # output in site/ (ignored by git)
```

The CI job `docs` runs the same command.
