with source as (
    select * from {{ ref('raw_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        city
    from source
)

select * from renamed
