{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_id') }}

select
  o.order_id,
  o.customer_id,
  o.product_id,
  p.category,
  o.quantity,
  o.amount,
  o.status,
  o.order_ts,
  o.coupon_code
from {{ ref('stg_orders') }} o
left join {{ source('silver', 'DIM_PRODUCTS') }} p using (product_id)
{% if is_incremental() %}
where o.order_ts >= (select coalesce(max(order_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
