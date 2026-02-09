-- Validates that all satisfaction scores are within the expected 1-5 range.
-- This catches data quality issues from upstream systems that might send
-- invalid ratings (e.g., 0, 6, or negative values).

select
    ticket_id,
    satisfaction_score

from {{ ref('fct_tickets') }}

where
    satisfaction_score is not null
    and (satisfaction_score < 1 or satisfaction_score > 5)