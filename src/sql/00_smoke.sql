-- Reads the marts the smoke job builds. `make sql FILE=src/sql/00_smoke.sql`
-- runs this on the serverless warehouse and prints the results in the terminal,
-- which is the fastest way to confirm a deploy actually landed data.

-- Row counts across the medallion layers. Bronze should exceed silver by
-- roughly the duplicate + defect rate the generator injects (~3%); if they are
-- equal, the silver transform is not running.
SELECT 'bronze_events'      AS table_name, COUNT(*) AS rows FROM workspace.sa_coding.bronze_events
UNION ALL
SELECT 'silver_events',     COUNT(*) FROM workspace.sa_coding.silver_events
UNION ALL
SELECT 'gold_daily_revenue', COUNT(*) FROM workspace.sa_coding.gold_daily_revenue
ORDER BY table_name;

-- The last fortnight of the mart, with late arrivals visible. A day where
-- late_orders is a large share of orders is a day whose revenue is still moving.
SELECT
  event_date,
  orders,
  customers,
  revenue,
  avg_order_value,
  late_orders,
  ROUND(100.0 * late_orders / NULLIF(orders, 0), 1) AS late_pct
FROM workspace.sa_coding.gold_daily_revenue
ORDER BY event_date DESC
LIMIT 14;
