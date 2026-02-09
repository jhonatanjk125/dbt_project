with source as (

    select * from {{ source('raw', 'sla_definitions') }}

),

renamed as (

    select
        lower(trim(priority))                           as priority,
        cast(first_response_sla_minutes as int)          as first_response_sla_minutes,
        cast(resolution_sla_minutes as int)              as resolution_sla_minutes,
        description                                      as priority_description

    from source

)

select * from renamed