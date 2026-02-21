with orders as (
    select * from {{ ref('stg_orders') }}
),
weather as (
    select * from {{ ref('stg_weather') }}
),
joined as (
    select
        o.order_id,
        o.customer_name,
        o.product,
        o.amount_usd,
        o.city,
        o.order_date,
        o.order_month,
        o.order_year,
        w.temp_celsius,
        w.humidity_pct,
        w.weather_condition,
        w.temp_bucket,
        case when w.temp_bucket = 'hot' then true else false end as sold_on_hot_day
    from orders o
    left join weather w
        on  o.city       = w.city
        and o.order_date = w.weather_date
)
select * from joined
