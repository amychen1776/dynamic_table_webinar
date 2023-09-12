{{
    config(
        materialized='dynamic_table',
    )
}}

with source as (

    select * from {{ source('flight_location', 'url_streaming_table') }}


),

renamed as (

    select 

        record_content:iss_position:latitude::string as latitude,
        record_content:iss_position:longitude::string as longitude

    from source

)

select * from renamed