with orders as (
    select * from {{ ref('stg_orders') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

final as (
    select o.order_id,
        o.order_date,
        o.city,
        o.amount,
        w.temp_celsius,
        w.condition
    from orders o
    left join weather w
        on o.city = w.city
        and o.order_date = w.weather_date
)

select * from final
