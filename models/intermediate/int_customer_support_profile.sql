/*
    Builds a support profile for each customer:
    - Total tickets, resolution rate, average CSAT
    - Repeat contact flag (high-touch customers)
*/

with tickets as (

    select * from {{ ref('int_tickets_with_sla') }}

),

customer_agg as (

    select
        customer_id,

        count(*)                                            as total_tickets,
        count(case when status = 'resolved' then 1 end)    as resolved_tickets,
        count(case when status = 'open' then 1 end)        as open_tickets,

        {{ safe_divide(
            'count(case when status = \'resolved\' then 1 end)',
            'count(*)'
        ) }}                                                as resolution_rate,

        avg(satisfaction_score)                              as avg_satisfaction_score,

        count(case when is_first_response_breached then 1 end)
            as first_response_breaches,

        count(case when is_resolution_breached then 1 end)
            as resolution_breaches,

        min(created_at)                                     as first_ticket_at,
        max(created_at)                                     as last_ticket_at

    from tickets
    group by customer_id

),

flagged as (

    select
        *,
        case when total_tickets >= 3 then true else false end as is_repeat_contact

    from customer_agg

)

select * from flagged