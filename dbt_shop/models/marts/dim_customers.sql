{{ config(materialized='table', table_format='iceberg') }}

select
  customer_id,
  count(*)              as orders,
  round(sum(amount), 2) as revenue,
  min(order_ts)         as first_order_ts,
  max(order_ts)         as last_order_ts
from {{ ref('fct_orders') }}
group by customer_id
