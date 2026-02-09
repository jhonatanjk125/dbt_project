/*
    Dimension table: customers enriched with their support health profile.
    Includes a computed support_health_score (0-100) based on resolution rate,
    satisfaction, and SLA adherence. Used for account risk assessment.
*/

with customers as (

    select * from {{ ref('stg_customers') }}

),

support_profile as (

    select * from {{ ref('int_customer_support_profile') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,

        c.customer_id,
        c.customer_name,
        c.plan_tier,
        c.signup_date,
        c.monthly_revenue,
        c.is_active,

        -- Support metrics
        coalesce(sp.total_tickets, 0)               as total_tickets,
        coalesce(sp.resolved_tickets, 0)             as resolved_tickets,
        coalesce(sp.open_tickets, 0)                 as open_tickets,
        sp.resolution_rate,
        sp.avg_satisfaction_score,
        coalesce(sp.first_response_breaches, 0)      as first_response_breaches,
        coalesce(sp.resolution_breaches, 0)           as resolution_breaches,
        coalesce(sp.is_repeat_contact, false)         as is_repeat_contact,
        sp.first_ticket_at,
        sp.last_ticket_at,

        -- Support health score (0-100)
        -- Weighted: 40% resolution rate, 30% CSAT, 30% SLA adherence
        case
            when sp.total_tickets is null or sp.total_tickets = 0
            then null  -- no tickets = no score
            else round(
                (coalesce(sp.resolution_rate, 0) * 40)
                + (coalesce(sp.avg_satisfaction_score, 0) / 5.0 * 30)
                + (
                    (1.0 - {{ safe_divide(
                        'coalesce(sp.first_response_breaches, 0) + coalesce(sp.resolution_breaches, 0)',
                        'sp.total_tickets * 2'
                    ) }}) * 30
                  )
            , 1)
        end as support_health_score

    from customers c
    left join support_profile sp
        on c.customer_id = sp.customer_id

)

select * from final