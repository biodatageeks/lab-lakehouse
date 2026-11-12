select * from {{ ref('dim_customers') }} where revenue <= 0
