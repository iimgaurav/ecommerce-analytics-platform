with source as (
    select * from {{ ref('raw_weather') }}
),
cleaned as (
    select
        trim(city)                     as city,
        cast(date as date)             as weather_date,
        cast(temp_celsius as double)   as temp_celsius,
        cast(humidity as integer)      as humidity_pct,
        trim(condition)                as weather_condition,
        case
            when cast(temp_celsius as double) >= 30 then 'hot'
            when cast(temp_celsius as double) >= 22 then 'warm'
            else 'cool'
        end                            as temp_bucket,
        current_timestamp              as dbt_loaded_at
    from source
)
select * from cleaned
