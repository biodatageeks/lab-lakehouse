{{ config(materialized='iceberg_table') }}

-- Iceberg tables store timestamps in microseconds: cast TIMESTAMP_NTZ(9) down to (6).
select
  customer_id,
  count(*)                                as orders,
  round(sum(amount), 2)                   as revenue,
  min(order_ts)::timestamp_ntz(6)         as first_order_ts,
  max(order_ts)::timestamp_ntz(6)         as last_order_ts
from {{ ref('fct_orders') }}
group by customer_id
