-- Session 3: Session 2 rebuilt as a Lakeflow Declarative Pipeline (triggered mode).
-- Default catalog/schema of the pipeline: shop / silver. Gold is published with a
-- fully-qualified name.

-- Bronze-in-pipeline: incremental file ingestion (Auto Loader under the hood).
CREATE OR REFRESH STREAMING TABLE orders_stream (
  CONSTRAINT valid_amount   EXPECT (amount IS NOT NULL AND amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL)          ON VIOLATION DROP ROW
)
COMMENT 'Typed, validated orders, one row per file record'
AS SELECT
  CAST(order_id AS BIGINT)        AS order_id,
  CAST(customer_id AS BIGINT)     AS customer_id,
  CAST(product_id AS BIGINT)      AS product_id,
  CAST(quantity AS INT)           AS quantity,
  CAST(amount AS DOUBLE)          AS amount,
  status,
  CAST(order_ts AS TIMESTAMP)     AS order_ts,
  coupon_code,
  _metadata.file_name             AS source_file
FROM STREAM read_files(
  '${landing}/orders',
  format => 'json',
  schemaHints => 'order_id BIGINT, customer_id BIGINT, amount DOUBLE, coupon_code STRING'
);

-- Silver: deduplicated orders (the producer re-sends one order per file).
CREATE OR REFRESH MATERIALIZED VIEW orders
COMMENT 'One row per order_id'
AS SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_file DESC) AS rn
  FROM orders_stream
) WHERE rn = 1;

-- Gold: revenue per hour.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.revenue_hourly
COMMENT 'Revenue per hour'
AS SELECT
  date_trunc('HOUR', order_ts) AS order_hour,
  COUNT(*)                     AS orders,
  ROUND(SUM(amount), 2)        AS revenue
FROM orders
GROUP BY ALL;
