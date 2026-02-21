with source as (
    select * from {{ ref('raw_weather') }}
),

renamed as (
    select city,
        date as weather_date,
        temp_celsius,
        humidity,
        condition
    from source
)

select * from renamed
