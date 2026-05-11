CREATE TABLE IF NOT EXISTS ml_prod.sandbox.publisher_influencer_review_decisions_v1 (
    review_batch_id STRING,
    PublisherKey INT,

    reviewed_cluster_label STRING,
    review_outcome STRING,
    review_reason_category STRING,
    review_reason_detail STRING,
    review_comment STRING,

    reviewer_name STRING,
    reviewer_email STRING,
    reviewed_at TIMESTAMP,
    decision_source STRING,

    review_sequence INT,
    priority_bucket STRING,
    review_confidence_hint STRING,
    creator_evidence_score INT,
    non_creator_risk_score INT,

    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ml_prod.sandbox.publisher_influencer_review_decisions_v1
IS 'Structured reviewer decisions captured from the Influencer / Content Creator review UI.';

CREATE OR REPLACE VIEW ml_prod.sandbox.publisher_influencer_review_results_v1_1 AS
SELECT
    q.*,
    CASE WHEN d.PublisherKey IS NULL THEN 'unreviewed' ELSE 'reviewed' END AS final_review_status,
    d.reviewed_cluster_label,
    d.review_outcome,
    d.review_reason_category,
    d.review_reason_detail,
    d.review_comment,
    d.reviewer_name,
    d.reviewer_email,
    d.reviewed_at,
    d.decision_source,
    d.created_at AS decision_created_at,
    d.updated_at AS decision_updated_at
FROM ml_prod.sandbox.publisher_influencer_review_handoff_v1_1 q
LEFT JOIN ml_prod.sandbox.publisher_influencer_review_decisions_v1 d
    ON q.review_batch_id = d.review_batch_id
   AND q.PublisherKey = d.PublisherKey;
