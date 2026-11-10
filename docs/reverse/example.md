# Worked example: `examples/reverse/`

**In plain terms:** a synthetic "customer_wide" table with 15 feature columns, built nightly by
one SQL query over an orders table, is turned into a spec, split into six tables by business
domain, and given a parity query. Every input and output is checked into the repository, and
`regenerate.sh` reproduces the outputs byte for byte.

## Inputs

| File | Content |
| --- | --- |
| `customer_wide_features.sql` | The legacy SQL (below) |
| `customer_wide.table.json` | Legacy table `main.features.customer_wide`: 18 columns (keys, `feature_ts`, 15 features), primary key `(tenant_id, customer_id, feature_ts)`, per-column `domain` and `business_unit` tags |
| `orders.table.json` | Fact table `main.sales.orders`, primary key `order_id`, 12 columns including `ordered_at` and `ingested_at` |
| `customers.table.json` | Registry `main.crm.customers`, one row per tenant and customer, `registered_at`, `ingested_at` |

```sql title="examples/reverse/customer_wide_features.sql"
-- Legacy wide feature table: one row per tenant/customer, rebuilt nightly for :as_of.
SELECT
  o.tenant_id,
  o.customer_id,
  :as_of AS feature_ts,
  COUNT(*) AS orders_30d,
  COUNT(DISTINCT o.order_id) AS distinct_orders_30d,
  COALESCE(SUM(CASE WHEN o.status = 'settled' THEN o.amount_cents END), 0) AS settled_spend_30d,
  AVG(o.amount_cents) AS avg_order_value_30d,
  MAX(o.amount_cents) AS max_order_value_30d,
  COUNT(*) FILTER (WHERE o.channel = 'web') AS web_orders_30d,
  COUNT(*) FILTER (WHERE o.channel = 'store') AS store_orders_30d,
  COUNT(CASE WHEN o.status = 'returned' THEN 1 END) AS returns_30d,
  COALESCE(SUM(o.discount_cents), 0) AS discount_30d,
  COUNT(CASE WHEN o.payment_method = 'card' THEN 1 END) AS card_payments_30d,
  COUNT(CASE WHEN o.payment_method = 'wallet' THEN 1 END) AS wallet_payments_30d,
  MAX(o.shipping_days) AS max_shipping_days_30d,
  AVG(o.shipping_days) AS avg_shipping_days_30d,
  returns_30d / NULLIF(orders_30d, 0) AS return_rate_30d,
  web_orders_30d / NULLIF(orders_30d, 0) AS web_share_30d
FROM main.sales.orders o
WHERE o.ordered_at >= :as_of - INTERVAL 30 DAYS
  AND o.ordered_at < :as_of
  AND o.is_test = false
GROUP BY o.tenant_id, o.customer_id
```

Column tags in `customer_wide.table.json`:

| Column | `domain` | `business_unit` |
| --- | --- | --- |
| orders_30d, distinct_orders_30d | purchasing | commerce |
| settled_spend_30d, avg_order_value_30d, max_order_value_30d, discount_30d | revenue | finance |
| web_orders_30d, store_orders_30d, web_share_30d | channel | marketing |
| returns_30d, return_rate_30d | returns | operations |
| card_payments_30d, wallet_payments_30d | payments | finance |
| max_shipping_days_30d, avg_shipping_days_30d | fulfilment | operations |

## Running it

```sh
pip install 'featlas[reverse]'   # or: maturin develop, plus sqlglot==30.30.0 and PyYAML==6.0.3
examples/reverse/regenerate.sh
```

Output on this commit (run in a copy of the directory; the result was identical to the checked-in
`generated/`):

```text
{"spec": "generated/customer_wide.spec.yaml", "report": "generated/drafting-report.json", "valid": true, "assumptions": 14, "questions": 1, "unsupported": 0}
{"spec": "generated/customer_wide.split.yaml", "groups": ["channel", "fulfilment", "payments", "purchasing", "returns", "revenue"], "valid": true}
```

(featlas is not on PyPI yet; install the locally built wheel with the `reverse` extra.)

## What the analyzer understood

`featlas-reverse analyze customer_wide_features.sql` (summarized): fact table `o` =
`main.sales.orders`; group by `tenant_id`, `customer_id`; window on `ordered_at`, lookback
2 592 000 s (30 days), lower inclusive, upper exclusive, anchor `:as_of`; shared filter
`is_test = false`; no unsupported constructs. Per column: 13 aggregates (`count`,
`count_distinct`, `sum`, `mean`, `max`) with their own filters (`o.status = 'settled'`,
`o.channel = 'web'`, …; `COALESCE(…, 0)` noted as `coalesce_zero`), and two expressions,
`return_rate_30d` (depends on `orders_30d`, `returns_30d`) and `web_share_30d` (depends on
`orders_30d`, `web_orders_30d`).

## The drafting report

`generated/drafting-report.json` — all 14 assumptions:

| Decision | Value | Why (as recorded) |
| --- | --- | --- |
| `project.owner` | ml-platform | Owner of the legacy table (or a placeholder) |
| `source_ids.main.sales.orders` | orders | Logical source id from the table name |
| `entity.id` | customer | Entity named after its most specific key |
| `entity.tenant_key` | tenant_id | Tenant key detected by name (null = single-tenant) |
| `population.table` | main.crm.customers | A provided table whose primary key equals the entity keys |
| `source_ids.main.crm.customers` | customers | Logical source id from the table name |
| `sources.main.sales.orders.available_time` | ingested_at | Column name suggests ingestion/availability time |
| `sources.main.crm.customers.available_time` | ingested_at | Column name suggests ingestion/availability time |
| `earliest_as_of` | 2026-01-01T00:00:00Z | Earliest reproducible instant; set to the start of retained source history |
| `feature_set.id` | customer_wide | Feature set named after the legacy table |
| `project.id` | customer_wide_reverse | Project id |
| `target.table` | main.features.customer_wide_featlas | Regenerated table written next to the legacy one (the legacy table is kept for parity checks) |
| `deployment.id` | customer_wide_databricks | Deployment id |
| `deployment.environment` | development | Environment label |

One non-blocking question remains — `samples`: "No characterization samples: provide, for a few
entities and one as_of, the source rows and the legacy table's output row (see the skill).
Without them only structural cases exist." `unsupported` is empty, and `mapping` lists all 15
legacy columns, for example
`"settled_spend_30d": {"feature": "customer.settled_spend_30d", "version": "1.0.0", "legacy_sql":
"COALESCE(SUM(CASE WHEN o.status = 'settled' THEN o.amount_cents END), 0) AS settled_spend_30d"}`.

## The drafted spec

`generated/customer_wide.spec.yaml` is `status: draft`. The `CASE WHEN` filter and the shared
`WHERE` filter become one `and` predicate (excerpt, descriptions omitted):

```yaml title="generated/customer_wide.spec.yaml (excerpt)"
- id: customer.settled_spend_30d
  version: '1.0.0'
  entity: customer
  inputs:
  - alias: orders
    source: orders
    columns: [amount_cents, customer_id, ingested_at, is_test, order_id, ordered_at, status, tenant_id]
  time:
    timezone: UTC
    knowledge_mode: source_as_known
    window: {lookback_seconds: 2592000, lower: inclusive, upper: exclusive}
    availability: {cutoff: as_of, upper: inclusive}
  transform:
    kind: aggregate
    filter:
      kind: call
      operator: and
      args:
      - {kind: call, operator: equal, args: [{kind: column, input: orders, column: is_test}, {kind: literal, type: boolean, value: false}]}
      - {kind: call, operator: equal, args: [{kind: column, input: orders, column: status}, {kind: literal, type: string, value: settled}]}
    operation: sum
    value: {kind: column, input: orders, column: amount_cents}
    nulls: ignore
```

(The file uses block style; this excerpt is re-flowed for space.) `returns_30d / NULLIF(orders_30d, 0)`
becomes an expression feature with pinned dependencies and `safe_divide`:

```yaml
- id: customer.return_rate_30d
  dependencies:
  - {feature: customer.orders_30d, version: '1.0.0', alignment: same_entity_as_of}
  - {feature: customer.returns_30d, version: '1.0.0', alignment: same_entity_as_of}
  transform:
    kind: expression
    expression:
      kind: call
      operator: safe_divide
      args:
      - {kind: feature, feature: customer.returns_30d, version: '1.0.0'}
      - {kind: feature, feature: customer.orders_30d, version: '1.0.0'}
```

Validating the split spec on this commit:

```text
warning EMPTY_IDENTITY_REVIEW [references:SEM-TRANS-004] /features/2/missingness/empty (line 681, column 13): A known-empty identity for sum/mean/min/max is a semantic choice; confirm it is intended
warning EMPTY_IDENTITY_REVIEW [references:SEM-TRANS-004] /features/8/missingness/empty (line 1588, column 13): A known-empty identity for sum/mean/min/max is a semantic choice; confirm it is intended
PASS: examples/reverse/generated/customer_wide.split.yaml (parse=passed, schema=passed, references=passed); planning, data and runtime levels not evaluated by validate
```

The two warnings are the `COALESCE(SUM(…), 0)` columns (`settled_spend_30d`, `discount_30d`):
"no orders" was mapped to 0, which a human must confirm. `fe-spec test` on the split spec:
`executed=15 passed=15 failed=0 not_executed=0` (structural cases only, since no samples were
provided).

## The split

`split-analysis.json` recommends `tag:domain` (scores and metrics in
[Splitting](splitting.md#domain-vs-business-unit-in-the-worked-example)); `regenerate.sh` applies
it. `split-mapping.json` maps each legacy column to its new table, for example
`web_orders_30d → main.features.customer_wide_featlas_channel` (column `web_orders_30d`, state
column `web_orders_30d_state`). Its compatibility view begins:

```sql
-- Compatibility view: the wide customer_wide shape over the split tables.
CREATE OR REPLACE VIEW `main`.`features`.`customer_wide_featlas_compat` AS
SELECT t0.`tenant_id`, t0.`customer_id`, t0.`feature_as_of`, t0.`web_orders_30d`, t0.`store_orders_30d`, t0.`web_share_30d`, t1.`max_shipping_days_30d`, ...
FROM `main`.`features`.`customer_wide_featlas_channel` t0
FULL OUTER JOIN `main`.`features`.`customer_wide_featlas_fulfilment` t1 ON t1.`tenant_id` = t0.`tenant_id` AND t1.`customer_id` = t0.`customer_id` AND t1.`feature_as_of` = t0.`feature_as_of`
...
```

Two derived features cross tables: `return_rate_30d` (in `returns`) and `web_share_30d` (in
`channel`) both depend on `orders_30d` (in `purchasing`), which is recomputed in their jobs.

## The parity query

`generated/parity.sql` (abridged):

```sql
-- Parity of main.features.customer_wide against main.features.customer_wide_featlas_channel, ... at 2026-06-01T00:00:00Z (read-only).
WITH l AS (SELECT `tenant_id`, `customer_id`, `web_orders_30d`, ... FROM `main`.`features`.`customer_wide` WHERE `feature_ts` = TIMESTAMP '2026-06-01T00:00:00Z'),
n0 AS (SELECT `tenant_id`, `customer_id`, `web_orders_30d`, `store_orders_30d`, `web_share_30d` FROM `main`.`features`.`customer_wide_featlas_channel` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
...
k AS (SELECT `tenant_id`, `customer_id` FROM n0 UNION ... FROM n5),
all_keys AS (SELECT `tenant_id`, `customer_id` FROM l UNION SELECT `tenant_id`, `customer_id` FROM k)
SELECT
  count(*) AS rows_compared,
  sum(CASE WHEN l.`tenant_id` IS NULL THEN 1 ELSE 0 END) AS missing_in_legacy,
  sum(CASE WHEN k.`tenant_id` IS NULL THEN 1 ELSE 0 END) AS missing_in_new,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`web_orders_30d` <=> n0.`web_orders_30d`) THEN 1 ELSE 0 END) AS `mismatched_web_orders_30d`,
  ...
FROM all_keys a
LEFT JOIN l ON ...
```

On Spark with synthetic data, `test_split_by_domain_regenerates_legacy_values` runs this exact
query (table names rebound to the legacy rows and the regenerated snapshots) and gets 3 rows
compared, nothing missing, no mismatches; [Regeneration and parity](regenerate.md) explains how
to read non-zero results.
