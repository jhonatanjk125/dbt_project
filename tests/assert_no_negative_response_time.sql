-- Validates that first_response_at is never before created_at.
-- A negative response time indicates a timestamp data quality issue
-- in the source system.

select
    ticket_id,
    created_at,
    first_response_at,
    first_response_minutes

from {{ ref('int_tickets_with_sla') }}

where
    first_response_minutes is not null
    and first_response_minutes < 0