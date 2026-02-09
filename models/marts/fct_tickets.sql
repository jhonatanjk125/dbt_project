/*
    Fact table: one row per support ticket, fully enriched with SLA performance,
    customer details, and agent information. This is the primary analytical table
    for ticket-level reporting.
*/

with tickets as (

    select * from {{ ref('int_tickets_with_sla') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

agents as (

    select * from {{ ref('stg_agents') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['t.ticket_id']) }} as ticket_key,

        t.ticket_id,
        t.customer_id,
        t.agent_id,

        -- Customer context
        c.customer_name,
        c.plan_tier,
        c.monthly_revenue       as customer_monthly_revenue,

        -- Agent context
        a.agent_name,
        a.team                  as agent_team,
        a.shift                 as agent_shift,

        -- Ticket attributes
        t.channel,
        t.priority,
        t.category,
        t.status,

        -- Timestamps
        t.created_at,
        t.first_response_at,
        t.resolved_at,

        -- Performance metrics
        t.first_response_minutes,
        t.resolution_minutes,
        t.satisfaction_score,

        -- SLA targets & breach flags
        t.first_response_sla_minutes,
        t.resolution_sla_minutes,
        t.is_first_response_breached,
        t.is_resolution_breached

    from tickets t
    left join customers c
        on t.customer_id = c.customer_id
    left join agents a
        on t.agent_id = a.agent_id

)

select * from final