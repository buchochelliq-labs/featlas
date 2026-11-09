#!/bin/sh
# Regenerate docs/evidence/ from the current checkout. Requires: cargo, python3.12 with the kit
# requirements (KIT_PYTHON), and optionally a PySpark venv (SPARK_PYTHON) for the differential suite.
set -eu
cd "$(dirname "$0")/.."
OUT=docs/evidence
mkdir -p "$OUT"
cargo build --release --quiet
B=target/release/fe-spec
E=spec/feature-engineering-spec-v0.2.0/examples
TS=2026-11-09T00:00:00Z

{ echo "# commands run by tools/make_evidence.sh"; echo "candidate_commit: $(git rev-parse HEAD)"; echo "dirty: $(git status --porcelain | wc -l) files"; echo "rustc: $(rustc --version)"; echo "fe-spec: $($B --version)"; echo "fe-spec sha256: $(sha256sum $B | cut -d' ' -f1)"; echo "Cargo.lock sha256: $(sha256sum Cargo.lock | cut -d' ' -f1)"; } > "$OUT/candidate.txt"

cargo test --release --quiet 2>&1 | grep -E "^test result|running" > "$OUT/cargo-test.txt" || true

for f in $E/0*.yaml; do $B test "$f" --format json > "$OUT/fixtures-$(basename "$f" .yaml).json" || true; done
for f in $E/0*.yaml projects/0*.yaml; do $B validate "$f" | tail -1; done > "$OUT/validate.txt"
$B hash projects/06_store_migration.yaml > "$OUT/hash-06.json"
$B hash $E/01_fault_count.yaml > "$OUT/hash-example-01.json"

$B capabilities projects/01_fault_count.yaml --deployment fault_dev --evidence-dir "$OUT/local-acceptance" --assessed-at $TS > "$OUT/capability-report-positive-local.json"
$B capabilities projects/01_fault_count.yaml --deployment fault_prod --assessed-at $TS > "$OUT/capability-report-databricks-unverified.json"
$B capabilities projects/04_fitted_and_custom_contracts.yaml --deployment contracts_prod --assessed-at $TS > "$OUT/capability-report-rejecting-custom.json"
$B check-report $E/reports/databricks_capabilities_unverified.json --project $E/06_store_migration.yaml > "$OUT/kit-report-check.json" || true
$B plan $E/01_fault_count.yaml --deployment fault_prod > "$OUT/plan-synthetic-pins-rejected.json" || true
$B migration-plan projects/06_store_migration.yaml --from fault_prod --to fault_alternative_store > "$OUT/migration-plan-06.json"
rm -rf "$OUT/generated-fault_prod"
$B generate projects/01_fault_count.yaml --deployment fault_prod --out "$OUT/generated-fault_prod" > /dev/null
# Generation timestamps make the capability report vary; the manifest records file digests.

if [ -n "${KIT_PYTHON:-}" ]; then
  (cd spec/feature-engineering-spec-v0.2.0 && "$KIT_PYTHON" tools/verify_bundle.py && "$KIT_PYTHON" tools/verify_manifest.py) > "$OUT/kit-verification.txt" 2>&1
  find spec -name __pycache__ -prune -exec rm -rf {} +
fi
if [ -n "${SPARK_PYTHON:-}" ]; then
  (cd python && PYSPARK_PYTHON="$SPARK_PYTHON" PYSPARK_DRIVER_PYTHON="$SPARK_PYTHON" FEATLAS_FE_SPEC="../$B" FEATLAS_DIFFERENTIAL_REPORT="../$OUT/local-spark-differential.json" "$SPARK_PYTHON" -m pytest -q tests -p no:cacheprovider) > "$OUT/local-spark-pytest.txt" 2>&1 || true
  tail -1 "$OUT/local-spark-pytest.txt"
fi
(cd "$OUT" && find . -type f ! -name SHA256SUMS | sort | xargs sha256sum > SHA256SUMS)
echo "evidence written to $OUT"
