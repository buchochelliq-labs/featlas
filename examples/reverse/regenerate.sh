#!/usr/bin/env bash
# Regenerate the checked-in reverse-engineering example outputs (read-only inputs, no warehouse).
# Needs the featlas wheel and the 'reverse' extra: pip install 'featlas[reverse]' (or maturin develop).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p generated
featlas-reverse draft --table customer_wide.table.json --sql customer_wide_features.sql \
  --source orders.table.json --source customers.table.json \
  --out generated/customer_wide.spec.yaml --report generated/drafting-report.json
featlas-reverse split-analyze generated/customer_wide.spec.yaml --feature-set customer_wide \
  --table customer_wide.table.json > generated/split-analysis.json
featlas-reverse split-apply generated/customer_wide.spec.yaml --feature-set customer_wide \
  --assignment generated/split-analysis.json --strategy tag:domain \
  --out generated/customer_wide.split.yaml --mapping generated/split-mapping.json
featlas-reverse parity generated/customer_wide.split.yaml --legacy-table main.features.customer_wide \
  --legacy-as-of-column feature_ts --key tenant_id --key customer_id --as-of 2026-06-01T00:00:00Z > generated/parity.sql
