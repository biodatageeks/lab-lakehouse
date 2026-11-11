-- Custom (singular) test: every customer has positive revenue. Returns failing rows.
select * from {{ ref('dim_customers') }} where revenue <= 0
