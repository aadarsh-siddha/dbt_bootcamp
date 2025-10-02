with

source as (

    select * from {{ source('jaffle_shop', 'supplies') }}

),

renamed as (

    select

        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,
        CAST(id AS VARCHAR) as supply_id,
        CAST(sku AS VARCHAR) as product_id,

        ---------- text
        name as supply_name,

        ---------- numerics
        (cost / 100.0) as supply_cost,

        ---------- booleans
        perishable as is_perishable_supply

    from source

)

select * from renamed