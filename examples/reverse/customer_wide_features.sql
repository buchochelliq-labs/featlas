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
