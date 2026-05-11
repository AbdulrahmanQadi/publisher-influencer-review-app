# Publisher Influencer Review App — React + FastAPI

This is a Databricks Apps version of the Influencer / Content Creator feedback UI.

It uses:

- React frontend served as static files
- FastAPI backend
- Databricks SQL Connector
- Databricks Delta / Unity Catalog tables
- Databricks App resource-based SQL warehouse configuration

## Tables

Queue source:

```sql
ml_prod.sandbox.publisher_influencer_review_handoff_v1_1
```

Decision write target:

```sql
ml_prod.sandbox.publisher_influencer_review_decisions_v1
```

## Required Databricks App resources

| Resource | Permission | Resource key |
|---|---|---|
| SQL warehouse | Can use | `sql-warehouse` |
| Queue table | Can select | `review-queue-table` |
| Decisions table | Can modify | `review-decisions-table` |

The `app.yaml` expects the SQL warehouse resource key to be exactly:

```yaml
valueFrom: 'sql-warehouse'
```

## Setup

1. Run `sql/setup_review_tables.sql` once to create/confirm the decisions table and results view.
2. Commit these files to the root of your GitHub repo.
3. Configure the Databricks App Git source.
4. Add the SQL warehouse and table resources.
5. Deploy.

## Local development

For local testing, set:

```bash
export DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export REVIEW_QUEUE_TABLE="ml_prod.sandbox.publisher_influencer_review_handoff_v1_1"
export REVIEW_DECISIONS_TABLE="ml_prod.sandbox.publisher_influencer_review_decisions_v1"
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

You also need valid local Databricks authentication through the Databricks SDK.

## Reviewer identity

The app attempts to read reviewer identity from Databricks Apps / proxy headers. If headers are unavailable, it falls back to a local development identity. For production reviewer attribution, enable the appropriate Databricks Apps user authentication headers / on-behalf-of configuration if required in your workspace.
