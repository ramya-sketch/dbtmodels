select
    id as order_id,
    user_id as customer_id,
    order_date,
    status

from `bionic-genre-363105`.dbt_core.stg_orders