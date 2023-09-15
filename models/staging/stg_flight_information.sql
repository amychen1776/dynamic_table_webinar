{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse = 'pc_dbt_wh',
        target_lag = target_lag_environment(),
        on_configuration_change = 'apply'
    )
}}

with source as (

    select * from {{ source('flight_location', 'url_streaming_table') }}

),

renamed as (

    select distinct

        record_content:iss_position:latitude::string as latitude,
        record_content:iss_position:longitude::string as longitude,
        record_content:message::string as status,
        record_content:timestamp::timestamp as update_at

    from source
),

add_primary_key as (

    select 

        *,
                {{ dbt_utils.generate_surrogate_key([
            'update_at', 'latitude', 'longitude'])}} 
            as primary_key

    from renamed 

)

select * from add_primary_key