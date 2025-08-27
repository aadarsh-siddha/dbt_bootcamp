
{% set old_query %}
  select * from {{ ref('customer_orders') }}
{% endset %}

{% set new_query %}
  select * from {{ ref('refactor_customer_orders') }}
{% endset %}

{{ 
  audit_helper.compare_and_classify_query_results(
    old_query, 
    new_query, 
    primary_key_columns=['order_id'], 
    columns=['order_id', 'total_amount_paid', 'customer_id']
  )
}}
