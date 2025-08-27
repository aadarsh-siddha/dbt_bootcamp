with 
orders as(
    select * from {{ ref("stg_jaffle_shop__orders") }}
),
customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),
payments as (
    select * from {{ ref('stg_stripe__payments') }}
),
order_level_payments as (
    select 
        order_id, 
        max(created_at) as payment_finalized_date, 
        sum(amount) as total_amount_paid
    from payments
    where status != 'fail'
    group by 1
),
paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        customers.first_name  as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join order_level_payments p on orders.order_id = p.order_id
    left join customers on orders.customer_id = customers.customer_id 
),
customer_orders as 
(
    select 
        customers.customer_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date, 
        count(orders.order_id) as number_of_orders
    from customers 
    left join orders
    on orders.customer_id = customers.customer_id
    group by 1
),
final as
(
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        case 
            when customer_orders.first_order_date = paid_orders.order_placed_at then 'new'
            else 'return' 
        end as nvsr,
        sum(total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as customer_lifetime_value,
        customer_orders.first_order_date as fdos,
    from paid_orders
    left join customer_orders on customer_orders.customer_id = paid_orders.customer_id
order by order_id 
)
select * from final