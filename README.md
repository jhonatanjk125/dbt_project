# Support Analytics

A dbt project for analyzing customer support ticket data. Built on DuckDB for fast local development and MotherDuck for cloud deployment.

## Project Structure

```
models/
├── staging/       # Clean raw data (views)
├── intermediate/  # Business logic transforms
└── marts/         # Final tables: fct_tickets, dim_customers, fct_support_kpis
```

## Quick Start

```bash
# Activate virtual environment
.\.venv\Scripts\Activate.ps1  # Windows
source .venv/bin/activate      # macOS/Linux

# Install dbt packages
dbt deps --profiles-dir .

# Load seed data
dbt seed --profiles-dir .

# Build all models and run tests
dbt build --profiles-dir .
```

## Targets

| Target | Database | Usage |
|--------|----------|-------|
| `local` (default) | DuckDB file | Local development |
| `motherduck` | MotherDuck cloud | CI/CD, shared environments |

To use MotherDuck:
```bash
export MOTHERDUCK_TOKEN=your_token
dbt build --profiles-dir . --target motherduck
```

## Key Models

- **fct_tickets** – Ticket-level fact table with SLA performance and customer/agent context
- **dim_customers** – Customer dimension with computed `support_health_score` (0-100)
- **fct_support_kpis** – Aggregated metrics for executive dashboards

## Tests

The project includes schema tests (uniqueness, not null, relationships) plus custom singular tests:
- `assert_health_score_range` – Health scores stay within 0-100
- `assert_no_negative_response_time` – Response times are non-negative
- `assert_satisfaction_score_range` – CSAT scores are 1-5
