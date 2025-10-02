WITH paid_orders as 
(
    select 
        Orders.ID as order_id,
        Orders.customer	as customer_id,
        Orders.ordered_at AS order_placed_at,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.FIRST_NAME as customer_first_name,
        C.LAST_NAME as customer_last_name
    FROM raw.jaffle_shop.orders as Orders
    left join (select ORDERID as order_id, max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid
            from raw.stripe.payment
            where STATUS <> 'fail'
            group by 1) p ON orders.ID = p.order_id
    left join raw.jaffle_shop.customers C on orders.customer = C.ID 
),
customer_orders as 
(
    select 
        C.ID as customer_id, 
        min(ordered_at) as first_order_date,
        max(ordered_at) as most_recent_order_date, 
        count(ORDERS.ID) AS number_of_orders
    from raw.jaffle_shop.customers C 
    left join raw.jaffle_shop.orders as Orders
    on orders.customer = C.ID 
    group by 1
)
select
    p.customer_id,
    p.order_placed_at,
    p.total_amount_paid,
    p.payment_finalized_date,
    p.customer_first_name,
    p.customer_last_name,
    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY p.customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE WHEN c.first_order_date = p.order_placed_at THEN 'new' ELSE 'return' END as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
FROM paid_orders p
left join customer_orders c on p.customer_id = c.customer_id
LEFT OUTER JOIN 
(
        select
            p.order_id,
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
) x on x.order_id = p.order_id
ORDER BY p.order_id
