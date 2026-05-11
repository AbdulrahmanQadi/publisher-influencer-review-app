# Influencer / Content Creator Flashcard Review App

A Databricks Apps + Streamlit review interface for the Publisher Influencer / Content Creator benchmark feedback loop.

The app presents one publisher at a time as a modern flashcard and captures structured reviewer decisions directly into a Databricks Delta table.

## Tables

### Review queue source

```sql
ml_prod.sandbox.publisher_influencer_review_handoff_v1_1
```

This should contain the 500 Benchmark v1 sample rows enriched with the v2 EDA feature/context layer.

### Review decisions output

```sql
ml_prod.sandbox.publisher_influencer_review_decisions_v1
```

Run `sql/setup_review_tables.sql` once before deployment to create the decisions table and results view.

## Files

```text
app.py
requirements.txt
app.yaml
.streamlit/config.toml
sql/setup_review_tables.sql
README.md
```

## Required app environment variables

The app needs access to a Databricks SQL warehouse. Configure one of these in Databricks Apps:

```text
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
```

or:

```text
DATABRICKS_WAREHOUSE_ID=<warehouse-id>
```

The app.yaml already sets:

```text
REVIEW_QUEUE_TABLE=ml_prod.sandbox.publisher_influencer_review_handoff_v1_1
REVIEW_DECISIONS_TABLE=ml_prod.sandbox.publisher_influencer_review_decisions_v1
DECISION_SOURCE=databricks_app_flashcard_ui
```

## Permissions required

The app identity / service principal needs:

- `SELECT` on `ml_prod.sandbox.publisher_influencer_review_handoff_v1_1`
- `SELECT` and `MODIFY` on `ml_prod.sandbox.publisher_influencer_review_decisions_v1`
- `SELECT` on the result view if used
- `CAN USE` on the SQL warehouse

Reviewers need access to the Databricks App itself.

## Reviewer identity

The app is designed for Databricks Apps authentication. It reads the reviewer identity from request headers where available.

When running locally, it falls back to:

```text
LOCAL_REVIEWER_EMAIL=local_reviewer@local.dev
LOCAL_REVIEWER_NAME=Local Reviewer
```

## Decision flow

Each card asks the reviewer to choose:

- `Creator`
- `Not creator`
- `Unsure`

The app then captures:

- reviewed cluster label
- review outcome
- reason category
- reason detail
- optional comment
- reviewer name/email
- timestamp
- decision source
- benchmark/enrichment context at time of review

## Local development

Install dependencies:

```bash
pip install -r requirements.txt
```

Set environment variables:

```bash
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export REVIEW_QUEUE_TABLE="ml_prod.sandbox.publisher_influencer_review_handoff_v1_1"
export REVIEW_DECISIONS_TABLE="ml_prod.sandbox.publisher_influencer_review_decisions_v1"
```

Run:

```bash
streamlit run app.py
```

## Deployment to Databricks Apps

1. Commit this folder to a repo or upload it into the Databricks App source location.
2. Run `sql/setup_review_tables.sql` once.
3. Configure the SQL warehouse resource / environment variable.
4. Deploy the app using `app.yaml`.
5. Share the app with the reviewer group.
