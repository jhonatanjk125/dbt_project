with source as (

    select * from {{ source('raw', 'raw_tickets') }}

),

renamed as (

    select
        ticket_id,
        customer_id,
        agent_id,
        lower(trim(channel))                        as channel,
        lower(trim(priority))                        as priority,
        lower(trim(category))                        as category,
        lower(trim(status))                          as status,
        cast(created_at as timestamp)                as created_at,
        cast(first_response_at as timestamp)         as first_response_at,
        cast(resolved_at as timestamp)               as resolved_at,
        cast(satisfaction_score as int)               as satisfaction_score

    from source

)

select * from renamed