-- Parity of main.features.customer_wide against main.features.customer_wide_featlas_channel, main.features.customer_wide_featlas_fulfilment, main.features.customer_wide_featlas_payments, main.features.customer_wide_featlas_purchasing, main.features.customer_wide_featlas_returns, main.features.customer_wide_featlas_revenue at 2026-06-01T00:00:00Z (read-only).
WITH l AS (SELECT `tenant_id`, `customer_id`, `web_orders_30d`, `store_orders_30d`, `web_share_30d`, `max_shipping_days_30d`, `avg_shipping_days_30d`, `card_payments_30d`, `wallet_payments_30d`, `orders_30d`, `distinct_orders_30d`, `returns_30d`, `return_rate_30d`, `settled_spend_30d`, `avg_order_value_30d`, `max_order_value_30d`, `discount_30d` FROM `main`.`features`.`customer_wide` WHERE `feature_ts` = TIMESTAMP '2026-06-01T00:00:00Z'),
n0 AS (SELECT `tenant_id`, `customer_id`, `web_orders_30d`, `store_orders_30d`, `web_share_30d` FROM `main`.`features`.`customer_wide_featlas_channel` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
n1 AS (SELECT `tenant_id`, `customer_id`, `max_shipping_days_30d`, `avg_shipping_days_30d` FROM `main`.`features`.`customer_wide_featlas_fulfilment` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
n2 AS (SELECT `tenant_id`, `customer_id`, `card_payments_30d`, `wallet_payments_30d` FROM `main`.`features`.`customer_wide_featlas_payments` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
n3 AS (SELECT `tenant_id`, `customer_id`, `orders_30d`, `distinct_orders_30d` FROM `main`.`features`.`customer_wide_featlas_purchasing` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
n4 AS (SELECT `tenant_id`, `customer_id`, `returns_30d`, `return_rate_30d` FROM `main`.`features`.`customer_wide_featlas_returns` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
n5 AS (SELECT `tenant_id`, `customer_id`, `settled_spend_30d`, `avg_order_value_30d`, `max_order_value_30d`, `discount_30d` FROM `main`.`features`.`customer_wide_featlas_revenue` WHERE `feature_as_of` = TIMESTAMP '2026-06-01T00:00:00Z'),
k AS (SELECT `tenant_id`, `customer_id` FROM n0 UNION SELECT `tenant_id`, `customer_id` FROM n1 UNION SELECT `tenant_id`, `customer_id` FROM n2 UNION SELECT `tenant_id`, `customer_id` FROM n3 UNION SELECT `tenant_id`, `customer_id` FROM n4 UNION SELECT `tenant_id`, `customer_id` FROM n5),
all_keys AS (SELECT `tenant_id`, `customer_id` FROM l UNION SELECT `tenant_id`, `customer_id` FROM k)
SELECT
  count(*) AS rows_compared,
  sum(CASE WHEN l.`tenant_id` IS NULL THEN 1 ELSE 0 END) AS missing_in_legacy,
  sum(CASE WHEN k.`tenant_id` IS NULL THEN 1 ELSE 0 END) AS missing_in_new,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`web_orders_30d` <=> n0.`web_orders_30d`) THEN 1 ELSE 0 END) AS `mismatched_web_orders_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`store_orders_30d` <=> n0.`store_orders_30d`) THEN 1 ELSE 0 END) AS `mismatched_store_orders_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`web_share_30d` <=> n0.`web_share_30d`) THEN 1 ELSE 0 END) AS `mismatched_web_share_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`max_shipping_days_30d` <=> n1.`max_shipping_days_30d`) THEN 1 ELSE 0 END) AS `mismatched_max_shipping_days_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`avg_shipping_days_30d` <=> n1.`avg_shipping_days_30d`) THEN 1 ELSE 0 END) AS `mismatched_avg_shipping_days_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`card_payments_30d` <=> n2.`card_payments_30d`) THEN 1 ELSE 0 END) AS `mismatched_card_payments_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`wallet_payments_30d` <=> n2.`wallet_payments_30d`) THEN 1 ELSE 0 END) AS `mismatched_wallet_payments_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`orders_30d` <=> n3.`orders_30d`) THEN 1 ELSE 0 END) AS `mismatched_orders_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`distinct_orders_30d` <=> n3.`distinct_orders_30d`) THEN 1 ELSE 0 END) AS `mismatched_distinct_orders_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`returns_30d` <=> n4.`returns_30d`) THEN 1 ELSE 0 END) AS `mismatched_returns_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`return_rate_30d` <=> n4.`return_rate_30d`) THEN 1 ELSE 0 END) AS `mismatched_return_rate_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`settled_spend_30d` <=> n5.`settled_spend_30d`) THEN 1 ELSE 0 END) AS `mismatched_settled_spend_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`avg_order_value_30d` <=> n5.`avg_order_value_30d`) THEN 1 ELSE 0 END) AS `mismatched_avg_order_value_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`max_order_value_30d` <=> n5.`max_order_value_30d`) THEN 1 ELSE 0 END) AS `mismatched_max_order_value_30d`,
  sum(CASE WHEN l.`tenant_id` IS NOT NULL AND k.`tenant_id` IS NOT NULL AND NOT (l.`discount_30d` <=> n5.`discount_30d`) THEN 1 ELSE 0 END) AS `mismatched_discount_30d`
FROM all_keys a
LEFT JOIN l ON l.`tenant_id` = a.`tenant_id` AND l.`customer_id` = a.`customer_id`
LEFT JOIN k ON k.`tenant_id` = a.`tenant_id` AND k.`customer_id` = a.`customer_id`
LEFT JOIN n0 ON n0.`tenant_id` = a.`tenant_id` AND n0.`customer_id` = a.`customer_id`
LEFT JOIN n1 ON n1.`tenant_id` = a.`tenant_id` AND n1.`customer_id` = a.`customer_id`
LEFT JOIN n2 ON n2.`tenant_id` = a.`tenant_id` AND n2.`customer_id` = a.`customer_id`
LEFT JOIN n3 ON n3.`tenant_id` = a.`tenant_id` AND n3.`customer_id` = a.`customer_id`
LEFT JOIN n4 ON n4.`tenant_id` = a.`tenant_id` AND n4.`customer_id` = a.`customer_id`
LEFT JOIN n5 ON n5.`tenant_id` = a.`tenant_id` AND n5.`customer_id` = a.`customer_id`
