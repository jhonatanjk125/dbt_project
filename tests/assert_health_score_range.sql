-- Validates that the computed support_health_score stays within
-- the expected 0-100 range. A score outside this range indicates
-- a bug in the scoring formula.

select
    customer_id,
    support_health_score

from {{ ref('dim_customers') }}

where
    support_health_score is not null
    and (support_health_score < 0 or support_health_score > 100)