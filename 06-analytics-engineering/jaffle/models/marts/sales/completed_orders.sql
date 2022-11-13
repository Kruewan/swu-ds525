with

int_orders_customers_joined as (
    select * from {{ ref('int_orders_customers_joined') }} 
)

, final as (
    select 
        *
    from int_orders_customers_joined
    where order_status = 'completed'
)

select * from final