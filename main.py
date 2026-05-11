import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


QUEUE_TABLE = os.getenv(
    "REVIEW_QUEUE_TABLE",
    "ml_prod.sandbox.publisher_influencer_review_handoff_v1_1",
)
DECISIONS_TABLE = os.getenv(
    "REVIEW_DECISIONS_TABLE",
    "ml_prod.sandbox.publisher_influencer_review_decisions_v1",
)
DECISION_SOURCE = os.getenv(
    "DECISION_SOURCE",
    "databricks_app_react_flashcard_ui",
)


def validate_table_name(table_name: str) -> str:
    name = str(table_name or "").strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$", name):
        raise RuntimeError(f"Invalid table name configured: {name}")
    return name


QUEUE_TABLE = validate_table_name(QUEUE_TABLE)
DECISIONS_TABLE = validate_table_name(DECISIONS_TABLE)

app = FastAPI(title="Publisher Influencer Review App", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")


class DecisionPayload(BaseModel):
    review_batch_id: str
    PublisherKey: int
    decision_type: str = Field(pattern="^(creator|not_creator|unsure)$")
    review_reason_category: str
    review_reason_detail: str
    review_comment: Optional[str] = ""


def get_http_path() -> str:
    explicit_http_path = os.getenv("DATABRICKS_HTTP_PATH", "").strip()
    if explicit_http_path:
        return explicit_http_path

    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
    if warehouse_id:
        if warehouse_id.startswith("/sql/"):
            return warehouse_id
        return f"/sql/1.0/warehouses/{warehouse_id}"

    return ""


def get_server_hostname(cfg: Config) -> str:
    host = os.getenv("DATABRICKS_HOST", "").strip() or str(cfg.host or "").strip()
    return host.replace("https://", "").replace("http://", "").strip("/")


def get_connection():
    http_path = get_http_path()
    if not http_path:
        raise RuntimeError(
            "Databricks SQL warehouse is not configured. Set DATABRICKS_WAREHOUSE_ID "
            "from the sql-warehouse app resource, or set DATABRICKS_HTTP_PATH."
        )

    cfg = Config()
    server_hostname = get_server_hostname(cfg)
    if not server_hostname:
        raise RuntimeError("Databricks host is not configured in the app runtime.")

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def query_df(query: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
    conn = get_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query, parameters=params or [])
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] if cursor.description else []
        return pd.DataFrame(rows, columns=columns)
    finally:
        try:
            if cursor:
                cursor.close()
        finally:
            conn.close()


def execute_sql(query: str, params: Optional[List[Any]] = None) -> None:
    conn = get_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query, parameters=params or [])
    finally:
        try:
            if cursor:
                cursor.close()
        finally:
            conn.close()


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()

    # databricks connector can return decimal/numpy-like values in some workspaces.
    try:
        import decimal
        if isinstance(value, decimal.Decimal):
            if value == value.to_integral_value():
                return int(value)
            return float(value)
    except Exception:
        pass

    return value


def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    records = []
    for record in df.to_dict(orient="records"):
        records.append({k: clean_value(v) for k, v in record.items()})
    return records


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def get_reviewer_from_request(request: Request) -> Dict[str, str]:
    headers = {k.lower(): v for k, v in request.headers.items()}

    email = (
        headers.get("x-forwarded-email")
        or headers.get("x-databricks-user-email")
        or headers.get("x-forwarded-user")
        or headers.get("x-databricks-user")
        or headers.get("x-ms-client-principal-name")
        or os.getenv("USER_EMAIL", "")
        or ""
    ).strip()

    name = (
        headers.get("x-forwarded-preferred-username")
        or headers.get("x-databricks-user-name")
        or headers.get("x-ms-client-principal-name")
        or ""
    ).strip()

    if not name and email:
        name = email.split("@")[0].replace(".", " ").replace("_", " ").title()

    if not email:
        # Local/dev fallback. In Databricks Apps, enable user auth headers later for proper reviewer identity.
        email = "local_reviewer@local.dev"
        name = "Local Reviewer"

    return {"reviewer_name": name or email, "reviewer_email": email}


def infer_review_outcome(decision_type: str, row: Dict[str, Any]) -> Tuple[str, str]:
    current_cluster = parse_bool(row.get("signal_current_cluster", False))

    if decision_type == "creator":
        return "belongs", "accepted_current_cluster" if current_cluster else "add_to_cluster"

    if decision_type == "not_creator":
        return "does_not_belong", "remove_from_cluster" if current_cluster else "unclear"

    return "unsure", "unclear"


@app.get("/")
def root():
    return FileResponse("static/index.html")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/me")
def me(request: Request):
    return get_reviewer_from_request(request)


@app.get("/api/queue")
def queue(request: Request):
    reviewer = get_reviewer_from_request(request)

    try:
        progress_sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN d.PublisherKey IS NOT NULL THEN 1 ELSE 0 END) AS reviewed_rows,
                COUNT(*) - SUM(CASE WHEN d.PublisherKey IS NOT NULL THEN 1 ELSE 0 END) AS remaining_rows
            FROM {QUEUE_TABLE} q
            LEFT JOIN {DECISIONS_TABLE} d
                ON q.review_batch_id = d.review_batch_id
               AND q.PublisherKey = d.PublisherKey
        """
        progress_df = query_df(progress_sql)
        progress = progress_df.iloc[0].to_dict() if not progress_df.empty else {}

        queue_sql = f"""
            SELECT
                q.*
            FROM {QUEUE_TABLE} q
            LEFT JOIN {DECISIONS_TABLE} d
                ON q.review_batch_id = d.review_batch_id
               AND q.PublisherKey = d.PublisherKey
            WHERE d.PublisherKey IS NULL
            ORDER BY q.review_sequence
        """
        queue_df = query_df(queue_sql)

        return {
            "reviewer": reviewer,
            "queue": df_to_records(queue_df),
            "progress": {
                "total_rows": safe_int(progress.get("total_rows", 0)),
                "reviewed_rows": safe_int(progress.get("reviewed_rows", 0)),
                "remaining_rows": safe_int(progress.get("remaining_rows", len(queue_df))),
            },
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/progress")
def progress():
    try:
        summary_sql = f"""
            SELECT
                q.priority_bucket,
                d.reviewed_cluster_label,
                d.review_reason_category,
                COUNT(*) AS rows
            FROM {QUEUE_TABLE} q
            LEFT JOIN {DECISIONS_TABLE} d
                ON q.review_batch_id = d.review_batch_id
               AND q.PublisherKey = d.PublisherKey
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """
        return {"summary": df_to_records(query_df(summary_sql))}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/decision")
def save_decision(payload: DecisionPayload, request: Request):
    reviewer = get_reviewer_from_request(request)

    try:
        row_sql = f"""
            SELECT *
            FROM {QUEUE_TABLE}
            WHERE review_batch_id = ?
              AND PublisherKey = CAST(? AS INT)
            LIMIT 1
        """
        row_df = query_df(row_sql, [payload.review_batch_id, payload.PublisherKey])
        if row_df.empty:
            raise HTTPException(status_code=404, detail="Publisher not found in review queue.")

        row = {k: clean_value(v) for k, v in row_df.iloc[0].to_dict().items()}

        existing_sql = f"""
            SELECT reviewer_email, reviewer_name
            FROM {DECISIONS_TABLE}
            WHERE review_batch_id = ?
              AND PublisherKey = CAST(? AS INT)
            LIMIT 1
        """
        existing_df = query_df(existing_sql, [payload.review_batch_id, payload.PublisherKey])

        if not existing_df.empty:
            existing_email = str(existing_df.iloc[0].get("reviewer_email") or "").lower().strip()
            current_email = str(reviewer["reviewer_email"] or "").lower().strip()
            if existing_email and existing_email != current_email:
                existing_name = str(existing_df.iloc[0].get("reviewer_name") or existing_email)
                raise HTTPException(
                    status_code=409,
                    detail=f"Already reviewed by {existing_name}. This row is read-only.",
                )

        reviewed_cluster_label, review_outcome = infer_review_outcome(payload.decision_type, row)
        reviewed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        merge_sql = f"""
            MERGE INTO {DECISIONS_TABLE} AS target
            USING (
                SELECT
                    ? AS review_batch_id,
                    CAST(? AS INT) AS PublisherKey,
                    ? AS reviewed_cluster_label,
                    ? AS review_outcome,
                    ? AS review_reason_category,
                    ? AS review_reason_detail,
                    ? AS review_comment,
                    ? AS reviewer_name,
                    ? AS reviewer_email,
                    CAST(? AS TIMESTAMP) AS reviewed_at,
                    ? AS decision_source,
                    CAST(? AS INT) AS review_sequence,
                    ? AS priority_bucket,
                    ? AS review_confidence_hint,
                    CAST(? AS INT) AS creator_evidence_score,
                    CAST(? AS INT) AS non_creator_risk_score
            ) AS source
            ON target.review_batch_id = source.review_batch_id
           AND target.PublisherKey = source.PublisherKey

            WHEN MATCHED THEN UPDATE SET
                target.reviewed_cluster_label = source.reviewed_cluster_label,
                target.review_outcome = source.review_outcome,
                target.review_reason_category = source.review_reason_category,
                target.review_reason_detail = source.review_reason_detail,
                target.review_comment = source.review_comment,
                target.reviewer_name = source.reviewer_name,
                target.reviewer_email = source.reviewer_email,
                target.reviewed_at = source.reviewed_at,
                target.decision_source = source.decision_source,
                target.review_sequence = source.review_sequence,
                target.priority_bucket = source.priority_bucket,
                target.review_confidence_hint = source.review_confidence_hint,
                target.creator_evidence_score = source.creator_evidence_score,
                target.non_creator_risk_score = source.non_creator_risk_score,
                target.updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                review_batch_id,
                PublisherKey,
                reviewed_cluster_label,
                review_outcome,
                review_reason_category,
                review_reason_detail,
                review_comment,
                reviewer_name,
                reviewer_email,
                reviewed_at,
                decision_source,
                review_sequence,
                priority_bucket,
                review_confidence_hint,
                creator_evidence_score,
                non_creator_risk_score,
                created_at,
                updated_at
            )
            VALUES (
                source.review_batch_id,
                source.PublisherKey,
                source.reviewed_cluster_label,
                source.review_outcome,
                source.review_reason_category,
                source.review_reason_detail,
                source.review_comment,
                source.reviewer_name,
                source.reviewer_email,
                source.reviewed_at,
                source.decision_source,
                source.review_sequence,
                source.priority_bucket,
                source.review_confidence_hint,
                source.creator_evidence_score,
                source.non_creator_risk_score,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """

        params = [
            payload.review_batch_id,
            payload.PublisherKey,
            reviewed_cluster_label,
            review_outcome,
            payload.review_reason_category,
            payload.review_reason_detail,
            payload.review_comment or "",
            reviewer["reviewer_name"],
            reviewer["reviewer_email"],
            reviewed_at,
            DECISION_SOURCE,
            safe_int(row.get("review_sequence", 0)),
            str(row.get("priority_bucket") or ""),
            str(row.get("review_confidence_hint") or ""),
            safe_int(row.get("creator_evidence_score", 0)),
            safe_int(row.get("non_creator_risk_score", 0)),
        ]

        execute_sql(merge_sql, params)

        return {
            "ok": True,
            "message": "Decision saved.",
            "reviewer": reviewer,
            "reviewed_cluster_label": reviewed_cluster_label,
            "review_outcome": review_outcome,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
