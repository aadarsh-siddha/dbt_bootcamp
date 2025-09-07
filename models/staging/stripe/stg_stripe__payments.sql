SELECT
    id AS payment_id,
    orderid AS order_id,
    paymentmethod AS payment_method,
    status,
    {{ cents_to_dollars("amount", 2) }} AS amount,
    created AS created_at
FROM 
    {{ source('stripe', 'payment') }}