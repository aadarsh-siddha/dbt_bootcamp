with orders as(
    select * from {{ ref('stg_jaffle_shop__orders') }}
),
{% set status_types = ['completed', 'placed','return_pending','returned','shipped'] %}
daily as (
    select 
        order_date,
        count(*) as num_orders,
        {% for status_value in status_types %}
            sum(case when status='{{status_value}}' then 1 else 0 end) AS n_{{status_value}}
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from orders
    group by 1
)
select * from daily
