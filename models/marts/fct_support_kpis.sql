/*
    Executive KPI summary: aggregated operational metrics.
    One row = one reporting period (overall summary for the dataset window).
*/

with tickets as (
  select * from {{ ref('fct_tickets') }}
),

final as (
  select
    -- Volume
    count(*) as total_tickets,
    count(*) filter (where status = 'resolved') as resolved_tickets,
    count(*) filter (where status = 'open') as open_tickets,

    -- Resolution rate
    {{ safe_divide("count(*) filter (where status = 'resolved')", "count(*)") }} as resolution_rate,

    -- Response time
    round(avg(first_response_minutes), 1) as avg_first_response_minutes,
    round(median(first_response_minutes), 1) as median_first_response_minutes,

    -- Resolution time (resolved tickets only)
    round(avg(resolution_minutes) filter (where status = 'resolved'), 1) as avg_resolution_minutes,

    -- Satisfaction
    round(avg(satisfaction_score), 2) as avg_csat_score,

    -- SLA performance (met rate)
    {{ safe_divide(
      "count(*) filter (where is_first_response_breached = false)",
      "count(*) filter (where first_response_minutes is not null)"
    ) }} as first_response_sla_met_rate,

    {{ safe_divide(
      "count(*) filter (where is_resolution_breached = false)",
      "count(*) filter (where is_resolution_breached is not null)"
    ) }} as resolution_sla_met_rate,

    -- Channel mix
    count(*) filter (where channel = 'email') as email_tickets,
    count(*) filter (where channel = 'chat') as chat_tickets,
    count(*) filter (where channel = 'phone') as phone_tickets,

    -- Priority mix
    count(*) filter (where priority = 'critical') as critical_tickets,
    count(*) filter (where priority = 'high') as high_tickets,

    min(created_at) as period_start,
    max(created_at) as period_end

  from tickets
)

select * from final