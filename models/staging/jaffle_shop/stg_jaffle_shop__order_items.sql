with

source as (

    select * from {{ source('jaffle_shop', 'items') }}

),

renamed as (

    select
        CAST(id AS VARCHAR)        as order_item_id,
        CAST(order_id AS VARCHAR)  as order_id,
        CAST(sku AS VARCHAR)       as product_id

    from source

)

select * from renamed
