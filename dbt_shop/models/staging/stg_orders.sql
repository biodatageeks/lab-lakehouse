select
  order_id,
  customer_id,
  product_id,
  quantity,
  amount,
  status,
  order_ts,
  coupon_code
from {{ source('silver', 'ORDERS') }}
