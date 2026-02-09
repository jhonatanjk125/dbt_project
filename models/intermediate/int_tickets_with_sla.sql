/*
    Enriches each ticket with:
    - Actual response/resolution times in minutes
    - SLA targets from the definitions table
    - Breach flags (did we miss the SLA?)
*/

with tickets as (

    select * from {{ ref('stg_tickets') }}

),

sla as (

    select * from {{ ref('stg_sla_definitions') }}

),

enriched as (

    select
        t.ticket_id,
        t.customer_id,
        t.agent_id,
        t.channel,
        t.priority,
        t.category,
        t.status,
        t.created_at,
        t.first_response_at,
        t.resolved_at,
        t.satisfaction_score,

        -- Actual durations (in minutes)
        date_diff('minute', t.created_at, t.first_response_at)
            as first_response_minutes,

        case
            when t.resolved_at is not null
            then date_diff('minute', t.created_at, t.resolved_at)
        end as resolution_minutes,

        -- SLA targets
        s.first_response_sla_minutes,
        s.resolution_sla_minutes,

        -- SLA breach flags
        case
            when t.first_response_at is null then null  -- still open, can't determine yet
            when date_diff('minute', t.created_at, t.first_response_at)
                 > s.first_response_sla_minutes
            then true
            else false
        end as is_first_response_breached,

        case
            when t.resolved_at is not null
                 and date_diff('minute', t.created_at, t.resolved_at)
                     > s.resolution_sla_minutes
            then true
            when t.resolved_at is null then null  -- still open, can't determine yet
            else false
        end as is_resolution_breached

    from tickets t
    left join sla s
        on t.priority = s.priority

)

select * from enriched