import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field


# ============================================================
# Configuration
# ============================================================

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


# ============================================================
# Safety helpers
# ============================================================

def validate_table_name(table_name: str) -> str:
    name = str(table_name or "").strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$", name):
        raise RuntimeError(f"Invalid table name configured: {name}")
    return name


QUEUE_TABLE = validate_table_name(QUEUE_TABLE)
DECISIONS_TABLE = validate_table_name(DECISIONS_TABLE)


# ============================================================
# FastAPI app
# ============================================================

app = FastAPI(title="Publisher Influencer Review App", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class DecisionPayload(BaseModel):
    review_batch_id: str
    PublisherKey: int
    decision_type: str = Field(pattern="^(creator|not_creator|unsure)$")
    review_reason_category: str
    review_reason_detail: str
    review_comment: Optional[str] = ""


# ============================================================
# Databricks SQL helpers
# ============================================================

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


# ============================================================
# Data serialization helpers
# ============================================================

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
    records: List[Dict[str, Any]] = []
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


# ============================================================
# Reviewer identity
# ============================================================

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
        # Local/dev fallback. In Databricks Apps, user headers should be available after auth setup.
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


# ============================================================
# API endpoints
# ============================================================

@app.get("/health")
def health():
    return {"status": "ok", "version": "3.0.0"}


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
            "progress": {k: clean_value(v) for k, v in progress.items()},
            "queue": df_to_records(queue_df),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        row_df = query_df(row_sql, [payload.review_batch_id, int(payload.PublisherKey)])
        if row_df.empty:
            raise HTTPException(status_code=404, detail="Publisher not found in review queue.")
        row = row_df.iloc[0].to_dict()

        existing_sql = f"""
            SELECT reviewer_email, reviewer_name
            FROM {DECISIONS_TABLE}
            WHERE review_batch_id = ?
              AND PublisherKey = CAST(? AS INT)
            LIMIT 1
        """
        existing_df = query_df(existing_sql, [payload.review_batch_id, int(payload.PublisherKey)])
        if not existing_df.empty:
            existing_email = str(existing_df.iloc[0].get("reviewer_email") or "").lower().strip()
            current_email = reviewer["reviewer_email"].lower().strip()
            if existing_email and existing_email != current_email:
                existing_name = existing_df.iloc[0].get("reviewer_name") or existing_email
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
            ) VALUES (
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

        execute_sql(
            merge_sql,
            [
                payload.review_batch_id,
                int(payload.PublisherKey),
                reviewed_cluster_label,
                review_outcome,
                payload.review_reason_category,
                payload.review_reason_detail,
                payload.review_comment or "",
                reviewer["reviewer_name"],
                reviewer["reviewer_email"],
                reviewed_at,
                DECISION_SOURCE,
                safe_int(row.get("review_sequence")),
                row.get("priority_bucket") or "",
                row.get("review_confidence_hint") or "",
                safe_int(row.get("creator_evidence_score")),
                safe_int(row.get("non_creator_risk_score")),
            ],
        )

        return {"status": "saved", "reviewer": reviewer}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ============================================================
# Premium inline React app
# ============================================================

INDEX_HTML = r'''
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Influencer Review</title>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <style>
    :root {
      --bg0: #050B15;
      --bg1: #07111F;
      --bg2: #0B1729;
      --panel: rgba(8, 18, 32, 0.72);
      --panel2: rgba(10, 23, 41, 0.84);
      --text: #F8FAFC;
      --muted: #A9BAD4;
      --soft: #C7D2FE;
      --blue: #60A5FA;
      --cyan: #22D3EE;
      --teal: #2DD4BF;
      --green: #34D399;
      --amber: #FBBF24;
      --red: #FB7185;
      --purple: #A78BFA;
      --border: rgba(148, 163, 184, 0.16);
      --border2: rgba(126, 167, 255, 0.22);
      --shadow: 0 24px 72px rgba(0,0,0,0.42);
      --radius: 30px;
    }
    * { box-sizing: border-box; }
    html, body, #root {
      margin: 0;
      min-height: 100vh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at 12% 5%, rgba(96, 165, 250, 0.24), transparent 27%),
        radial-gradient(circle at 88% 9%, rgba(45, 212, 191, 0.17), transparent 29%),
        radial-gradient(circle at 52% 97%, rgba(124, 58, 237, 0.15), transparent 25%),
        linear-gradient(135deg, #050B15 0%, #07111F 45%, #10152B 100%);
      overflow-x: hidden;
    }
    body:before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(255,255,255,0.026) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.026) 1px, transparent 1px);
      background-size: 56px 56px;
      mask-image: radial-gradient(circle at center, black, transparent 78%);
    }
    button, textarea { font-family: inherit; }
    .app-shell {
      min-height: 100vh;
      padding: 16px 24px 34px;
      position: relative;
    }
    .ambient-glow {
      position: fixed;
      width: 420px;
      height: 420px;
      border-radius: 999px;
      pointer-events: none;
      filter: blur(22px);
      opacity: .12;
      background: linear-gradient(135deg, var(--blue), var(--teal));
      right: -140px;
      top: 110px;
    }
    .topbar {
      max-width: 1180px;
      margin: 0 auto 18px;
      display: grid;
      grid-template-columns: minmax(330px, 1fr) minmax(300px, 0.9fr) minmax(230px, 0.7fr);
      align-items: center;
      gap: 20px;
      border: 1px solid var(--border);
      background:
        radial-gradient(circle at 92% 20%, rgba(45,212,191,0.12), transparent 30%),
        linear-gradient(135deg, rgba(10, 22, 39, 0.86), rgba(7, 15, 28, 0.91));
      border-radius: 26px;
      box-shadow: 0 20px 56px rgba(0,0,0,0.30), inset 0 1px 0 rgba(255,255,255,0.06);
      backdrop-filter: blur(18px);
      padding: 14px 18px;
    }
    .brand-row { display: flex; align-items: center; gap: 14px; }
    .logo-orb {
      width: 46px;
      height: 46px;
      border-radius: 17px;
      background:
        radial-gradient(circle at 30% 25%, #E0F2FE, transparent 24%),
        linear-gradient(135deg, #2563EB, #14B8A6 78%);
      display: grid;
      place-items: center;
      box-shadow: 0 16px 36px rgba(34, 211, 238, 0.25);
      font-weight: 950;
      letter-spacing: -0.08em;
    }
    .brand-title { font-size: 1.04rem; font-weight: 950; letter-spacing: -0.03em; line-height: 1.1; }
    .brand-subtitle { margin-top: 4px; color: #BFD0EA; font-size: 0.80rem; font-weight: 650; }
    .progress-zone { min-width: 270px; }
    .progress-label { display: flex; justify-content: space-between; font-size: 0.78rem; color: #D6E4FF; margin-bottom: 8px; font-weight: 850; }
    .progress-track { width: 100%; height: 8px; background: rgba(148, 163, 184, 0.13); border-radius: 999px; overflow: hidden; box-shadow: inset 0 1px 2px rgba(0,0,0,0.28); }
    .progress-fill { height: 8px; border-radius: 999px; background: linear-gradient(90deg, var(--blue), var(--cyan), var(--teal)); box-shadow: 0 0 20px rgba(34, 211, 238, 0.34); transition: width 450ms ease; }
    .reviewer-badge { text-align: right; min-width: 205px; }
    .reviewer-label { color: #90A7C8; text-transform: uppercase; letter-spacing: 0.08em; font-size: 0.64rem; font-weight: 950; }
    .reviewer-name { color: #F8FAFC; font-size: 0.92rem; font-weight: 900; margin-top: 4px; }
    .reviewer-email { color: #93C5FD; font-size: 0.76rem; font-weight: 650; margin-top: 2px; }
    .main-view { max-width: 1180px; margin: 0 auto; }
    .stage { display: grid; grid-template-columns: 56px minmax(0, 1fr) 56px; gap: 14px; align-items: center; }
    .nav-button {
      width: 50px; height: 50px; border-radius: 999px; border: 1px solid rgba(147, 197, 253, 0.25); color: #E0F2FE;
      background: radial-gradient(circle at 30% 20%, rgba(96,165,250,0.26), transparent 45%), rgba(13, 24, 42, 0.86);
      box-shadow: 0 18px 44px rgba(0,0,0,0.30); font-size: 1.95rem; font-weight: 950; display: grid; place-items: center; cursor: pointer;
      transition: transform 180ms ease, border-color 180ms ease, opacity 180ms ease, box-shadow 180ms ease;
    }
    .nav-button:hover:not(:disabled) { transform: translateY(-2px) scale(1.04); border-color: rgba(147, 197, 253, 0.75); box-shadow: 0 20px 48px rgba(96,165,250,0.18); }
    .nav-button:disabled { opacity: 0.18; cursor: not-allowed; }
    .card-wrap { perspective: 1200px; position: relative; }
    .card-stack { position: relative; max-width: 840px; margin: 0 auto; }
    .card-stack:before, .card-stack:after {
      content: ""; position: absolute; left: 26px; right: 26px; height: 100%; border-radius: var(--radius);
      background: rgba(20, 38, 64, 0.36); border: 1px solid rgba(126, 167, 255, 0.10); pointer-events: none;
    }
    .card-stack:before { top: 12px; transform: scale(0.975); opacity: 0.38; }
    .card-stack:after { top: 24px; transform: scale(0.945); opacity: 0.20; }
    .flashcard {
      max-width: 840px; min-height: 440px; margin: 0 auto; border: 1px solid rgba(126, 167, 255, 0.24); border-radius: var(--radius);
      background:
        radial-gradient(circle at 88% 8%, rgba(20,184,166,0.22), transparent 28%),
        radial-gradient(circle at 8% 96%, rgba(96,165,250,0.17), transparent 28%),
        linear-gradient(145deg, rgba(18,31,52,0.96), rgba(8,17,31,0.98));
      box-shadow: var(--shadow), inset 0 1px 0 rgba(255,255,255,0.06);
      padding: 24px 30px 22px; overflow: hidden; position: relative; z-index: 2; transition: border-color 260ms ease, box-shadow 260ms ease;
    }
    .flashcard:after { content: ""; position: absolute; inset: auto -120px -140px auto; width: 260px; height: 260px; background: radial-gradient(circle, rgba(34,211,238,.13), transparent 70%); pointer-events: none; }
    .flashcard.accept-active { border-color: rgba(45,212,191,0.52); box-shadow: 0 24px 72px rgba(20,184,166,0.20), var(--shadow); }
    .flashcard.reject-active { border-color: rgba(248,113,113,0.52); box-shadow: 0 24px 72px rgba(220,38,38,0.20), var(--shadow); }
    .flashcard.unsure-active { border-color: rgba(251,191,36,0.52); box-shadow: 0 24px 72px rgba(245,158,11,0.18), var(--shadow); }
    .slide-next { animation: slideNext 300ms cubic-bezier(.2,.8,.2,1); }
    .slide-prev { animation: slidePrev 300ms cubic-bezier(.2,.8,.2,1); }
    @keyframes slideNext { from { opacity: 0; transform: translateX(24px) rotateY(-3deg) scale(.992); filter: blur(2px); } to { opacity: 1; transform: translateX(0) rotateY(0) scale(1); filter: blur(0); } }
    @keyframes slidePrev { from { opacity: 0; transform: translateX(-24px) rotateY(3deg) scale(.992); filter: blur(2px); } to { opacity: 1; transform: translateX(0) rotateY(0) scale(1); filter: blur(0); } }
    .card-topline { display: flex; justify-content: space-between; align-items: flex-start; gap: 14px; margin-bottom: 12px; }
    .card-kicker { color: #A8BEDD; font-size: 0.72rem; font-weight: 950; letter-spacing: 0.10em; text-transform: uppercase; }
    .impact-pill { display: inline-flex; align-items: center; gap: 8px; padding: 8px 11px; border-radius: 999px; background: rgba(45,212,191,.09); border: 1px solid rgba(45,212,191,.16); color: #CFFAFE; font-size: .75rem; font-weight: 900; max-width: 270px; line-height: 1.25; }
    .impact-dot { width: 8px; height: 8px; border-radius: 999px; background: linear-gradient(135deg, var(--cyan), var(--teal)); box-shadow: 0 0 14px rgba(34,211,238,.45); flex: none; }
    .chip-row { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 15px; }
    .chip { border-radius: 999px; padding: 6px 10px; font-size: 0.72rem; font-weight: 900; border: 1px solid transparent; white-space: nowrap; }
    .chip.green { background: rgba(16,185,129,.17); color: #BBF7D0; border-color: rgba(74,222,128,.24); }
    .chip.blue { background: rgba(59,130,246,.16); color: #BFD7FF; border-color: rgba(96,165,250,.26); }
    .chip.amber { background: rgba(245,158,11,.17); color: #FCD9A6; border-color: rgba(251,191,36,.26); }
    .chip.red { background: rgba(239,68,68,.16); color: #FECACA; border-color: rgba(248,113,113,.24); }
    .chip.slate { background: rgba(148,163,184,.13); color: #D7E0EC; border-color: rgba(203,213,225,.13); }
    .publisher-title { font-size: clamp(2rem, 4.1vw, 2.52rem); font-weight: 950; letter-spacing: -0.055em; line-height: 1.02; margin: 0 0 10px; }
    .publisher-site a { color: #9EC5FF; font-weight: 850; text-decoration: none; }
    .publisher-site a:hover { text-decoration: underline; }
    .description { margin-top: 17px; border: 1px solid rgba(255,255,255,0.06); background: rgba(5, 13, 25, 0.58); border-radius: 22px; padding: 15px 17px; line-height: 1.48; color: #F1F6FF; font-size: 1.02rem; max-height: 105px; overflow-y: auto; }
    .stats-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-top: 16px; }
    .stat { background: rgba(6, 14, 26, 0.62); border: 1px solid rgba(255,255,255,0.06); border-radius: 18px; padding: 11px 12px; min-width: 0; }
    .stat-label { color: #9EC5FF; font-size: 0.64rem; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 950; margin-bottom: 6px; }
    .stat-value { color: #FFFFFF; font-weight: 900; font-size: .88rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .card-intel { margin-top: 16px; display: grid; grid-template-columns: 1.15fr .85fr; gap: 10px; }
    .intel-box { background: rgba(59,130,246,0.075); border: 1px solid rgba(96,165,250,0.11); border-radius: 18px; padding: 11px 12px; min-width: 0; }
    .intel-label { color: #93C5FD; text-transform: uppercase; letter-spacing: .08em; font-size: .65rem; font-weight: 950; margin-bottom: 6px; }
    .intel-value { color: #DDEAFE; font-size: .82rem; line-height: 1.35; }
    .mini-chipline { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
    .mini-chip { border-radius: 999px; padding: 5px 8px; font-size: .68rem; font-weight: 850; color: #DDEAFE; background: rgba(148,163,184,.10); border: 1px solid rgba(148,163,184,.13); }
    .mini-chip.good { color: #BBF7D0; background: rgba(16,185,129,.12); border-color: rgba(74,222,128,.20); }
    .mini-chip.warn { color: #FCD9A6; background: rgba(245,158,11,.13); border-color: rgba(251,191,36,.22); }
    .mini-chip.risk { color: #FECACA; background: rgba(239,68,68,.13); border-color: rgba(248,113,113,.22); }
    .context-card { max-width: 900px; margin: 14px auto 0; border: 1px solid rgba(148, 163, 184, 0.15); background: rgba(6, 14, 26, 0.62); border-radius: 22px; overflow: hidden; }
    .context-toggle { width: 100%; background: transparent; border: 0; color: #EAF2FF; padding: 16px 18px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; font-weight: 950; font-size: 0.94rem; }
    .context-toggle span { color: #93C5FD; }
    .context-body { border-top: 1px solid rgba(148, 163, 184, 0.11); padding: 18px; animation: drawerOpen 240ms ease-out; }
    @keyframes drawerOpen { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
    .context-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
    .context-section { border: 1px solid rgba(148,163,184,.12); background: rgba(10, 23, 41, .48); border-radius: 18px; padding: 14px; }
    .context-label { color: #93C5FD; font-size: .68rem; text-transform: uppercase; letter-spacing: .08em; font-weight: 950; margin-bottom: 8px; }
    .context-value { color: #F8FAFC; line-height: 1.45; font-size: .91rem; }
    .kv-grid { display: grid; grid-template-columns: 120px 1fr; gap: 6px 10px; color: #EAF2FF; font-size: .86rem; }
    .kv-key { color: #91A9C9; font-weight: 800; }
    .kv-val { color: #F8FAFC; font-weight: 760; overflow-wrap: anywhere; }
    .flag-cloud { display: flex; flex-wrap: wrap; gap: 7px; }
    .warning-box { margin-top: 10px; background: rgba(245,158,11,.14); border: 1px solid rgba(251,191,36,.25); color: #FCD9A6; border-radius: 14px; padding: 10px 12px; font-weight: 850; }
    .context-note { color: #BFD0EA; font-size: .82rem; margin-top: 12px; }
    .action-help { text-align: center; color: #CAD7EA; font-size: .84rem; font-weight: 800; margin: 16px 0 10px; }
    .action-buttons { max-width: 980px; margin: 0 auto; display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }
    .action-btn { min-height: 68px; border-radius: 24px; border: 1px solid transparent; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 14px; color: white; font-weight: 950; box-shadow: 0 18px 44px rgba(0,0,0,.28), inset 0 1px 0 rgba(255,255,255,.08); transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease, filter 180ms ease; }
    .action-btn strong { display: block; font-size: 1.02rem; }
    .action-btn small { display: block; margin-top: 2px; opacity: .88; font-weight: 800; }
    .action-btn .icon { font-size: 1.45rem; }
    .action-btn.reject { background: radial-gradient(circle at 20% 20%, rgba(248,113,113,.30), transparent 44%), linear-gradient(135deg, rgba(127,29,29,.96), rgba(69,10,10,.98)); border-color: rgba(248,113,113,.42); color: #FEE2E2; }
    .action-btn.unsure { background: radial-gradient(circle at 20% 20%, rgba(251,191,36,.34), transparent 44%), linear-gradient(135deg, rgba(120,53,15,.96), rgba(69,26,3,.98)); border-color: rgba(251,191,36,.44); color: #FEF3C7; }
    .action-btn.accept { background: radial-gradient(circle at 20% 20%, rgba(45,212,191,.32), transparent 44%), linear-gradient(135deg, rgba(6,95,70,.96), rgba(20,83,45,.98)); border-color: rgba(45,212,191,.44); color: #CCFBF1; }
    .action-btn:hover { transform: translateY(-2px) scale(1.01); filter: saturate(1.08); }
    .action-btn.active { outline: 2px solid rgba(255,255,255,.58); outline-offset: 3px; }
    .shortcuts { margin: 10px auto 0; text-align: center; color: #93A8C7; font-size: .76rem; font-weight: 750; }
    .reason-panel { max-width: 860px; margin: 18px auto 0; border: 1px solid rgba(126,167,255,.22); background: linear-gradient(145deg, rgba(11,23,40,.92), rgba(7,15,28,.96)); border-radius: 24px; padding: 18px; box-shadow: 0 24px 72px rgba(0,0,0,.34); animation: reasonRise 260ms ease-out; }
    @keyframes reasonRise { from { opacity: 0; transform: translateY(18px) scale(.99); } to { opacity: 1; transform: translateY(0) scale(1); } }
    .reason-title { font-size: 1.05rem; font-weight: 950; margin-bottom: 4px; }
    .reason-subtitle { color: #BFD0EA; font-size: .86rem; margin-bottom: 14px; }
    .reason-chips { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }
    .reason-chip { border: 1px solid rgba(148,163,184,.18); background: rgba(148,163,184,.10); color: #EAF2FF; padding: 9px 11px; border-radius: 999px; cursor: pointer; font-weight: 850; }
    .reason-chip.selected { border-color: rgba(45,212,191,.52); background: rgba(20,184,166,.16); color: #CCFBF1; }
    textarea { width: 100%; min-height: 86px; resize: vertical; color: #F8FAFC; background: rgba(6,14,26,.72); border: 1px solid rgba(148,163,184,.16); border-radius: 18px; padding: 12px; font-size: .94rem; outline: none; }
    textarea:focus { border-color: rgba(96,165,250,.58); }
    .save-row { display: flex; gap: 10px; margin-top: 12px; }
    .save-btn, .cancel-btn { border: 0; cursor: pointer; border-radius: 16px; padding: 12px 16px; font-weight: 950; }
    .save-btn { flex: 1; background: linear-gradient(135deg, #2563EB, #14B8A6); color: #FFFFFF; box-shadow: 0 16px 36px rgba(37,99,235,.22); }
    .cancel-btn { background: rgba(148,163,184,.12); color: #EAF2FF; border: 1px solid rgba(148,163,184,.16); }
    .toast { position: fixed; right: 24px; bottom: 24px; border-radius: 16px; padding: 12px 14px; background: rgba(6,95,70,.94); color: #CCFBF1; border: 1px solid rgba(45,212,191,.32); box-shadow: 0 20px 48px rgba(0,0,0,.35); font-weight: 900; }
    .empty-state, .error-card { max-width: 760px; margin: 80px auto; border: 1px solid var(--border); background: var(--panel); border-radius: 24px; padding: 28px; text-align: center; color: #EAF2FF; }
    .error-card { color: #FECACA; border-color: rgba(248,113,113,.30); }
    @media (max-width: 960px) {
      .topbar { grid-template-columns: 1fr; text-align: left; }
      .reviewer-badge { text-align: left; }
      .stage { grid-template-columns: 42px 1fr 42px; gap: 8px; }
      .nav-button { width: 42px; height: 42px; font-size: 1.55rem; }
      .stats-grid, .card-intel, .context-grid, .action-buttons { grid-template-columns: 1fr; }
      .publisher-title { font-size: 2rem; }
      .flashcard { min-height: 0; padding: 22px; }
      .card-topline { flex-direction: column; align-items: flex-start; }
      .impact-pill { max-width: none; }
    }
  </style>
</head>
<body>
  <div id="root"></div>
  <script>
    const e = React.createElement;
    const { useEffect, useMemo, useRef, useState } = React;

    const REASON_OPTIONS = {
      creator: [
        ['Creator / personal brand', 'creator_individual_or_creator_brand'],
        ['Social profile evidence', 'creator_individual_or_creator_brand'],
        ['UGC / product review creator', 'creator_individual_or_creator_brand'],
        ['Creator-commerce / affiliate', 'creator_individual_or_creator_brand'],
        ['Other creator evidence', 'other'],
      ],
      not_creator: [
        ['Business / company', 'business_or_company'],
        ['Agency / network', 'agency_or_network'],
        ['Media / editorial publisher', 'publisher_or_media'],
        ['Utility or non-creator site', 'utility_or_non_creator'],
        ['Not enough creator evidence', 'insufficient_information'],
        ['Other', 'other'],
      ],
      unsure: [
        ['Missing or weak website', 'insufficient_information'],
        ['Missing or weak description', 'insufficient_information'],
        ['Conflicting signals', 'insufficient_information'],
        ['Needs manual escalation', 'insufficient_information'],
        ['Other', 'other'],
      ],
    };

    const DECISION_META = {
      creator: { title: 'Creator', subtitle: 'Confirm this publisher genuinely belongs in the Influencer / Content Creator cluster.' },
      not_creator: { title: 'Not creator', subtitle: 'Confirm this publisher should be excluded or removed from the cluster.' },
      unsure: { title: 'Unsure', subtitle: 'Use this when evidence is incomplete, conflicting, or requires escalation.' },
    };

    function safe(v, fallback = '') {
      if (v === null || v === undefined || Number.isNaN(v)) return fallback;
      const s = String(v).trim();
      return s || fallback;
    }
    function bool(v) {
      if (v === true) return true;
      return ['true', '1', 'yes', 'y'].includes(String(v || '').toLowerCase());
    }
    function formatScore(v) {
      if (v === null || v === undefined || v === '') return '—';
      const n = Number(v);
      return Number.isFinite(n) ? String(Math.round(n)) : String(v);
    }
    function pct(progress) {
      const total = Number(progress?.total_rows || 0);
      const reviewed = Number(progress?.reviewed_rows || 0);
      if (!total) return 0;
      return Math.round((reviewed / total) * 1000) / 10;
    }
    function domainFor(url) {
      const raw = safe(url);
      if (!raw) return '';
      try {
        const u = new URL(raw.startsWith('http') ? raw : `https://${raw}`);
        return u.hostname.replace(/^www\./, '');
      } catch { return raw.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0]; }
    }
    function bucketChip(bucket) {
      if (['p1_current_cluster_strong', 'p3_hidden_positive_strong'].includes(bucket)) return 'green';
      if (['p2_current_cluster', 'p4_hidden_positive'].includes(bucket)) return 'blue';
      if (['p5_adjacent_supported', 'p6_social_and_keyword'].includes(bucket)) return 'amber';
      return 'slate';
    }
    function confidenceChip(conf) {
      if (conf === 'likely_creator') return 'green';
      if (conf === 'possible_creator') return 'blue';
      if (conf === 'creator_commercial_business_like') return 'amber';
      if (conf === 'likely_not_creator') return 'red';
      return 'slate';
    }
    function labelFromSnake(s) {
      return safe(s).replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
    function truncate(text, len = 120) {
      const s = safe(text);
      return s.length > len ? `${s.slice(0, len - 1)}…` : s;
    }
    function actionImplication(row) {
      if (bool(row.signal_current_cluster)) {
        return {
          label: 'Current cluster publisher',
          text: 'Reviewer decision helps confirm whether this publisher should stay in the cluster or be removed.',
          creator: 'Keep in cluster',
          reject: 'Remove from cluster',
        };
      }
      if (bool(row.signal_hidden_positive)) {
        return {
          label: 'Hidden-positive candidate',
          text: 'Reviewer decision helps decide whether this publisher should be added to the cluster.',
          creator: 'Candidate to add',
          reject: 'Leave out',
        };
      }
      return {
        label: 'Boundary review case',
        text: 'Reviewer decision helps clarify how similar future publishers should be treated.',
        creator: 'Include as creator',
        reject: 'Exclude from creator cluster',
      };
    }
    function evidenceItems(row) {
      const items = [];
      if (bool(row.is_social_profile_url)) items.push('Social profile URL');
      if (bool(row.is_link_in_bio_url)) items.push('Link-in-bio');
      if (bool(row.is_creator_storefront_url)) items.push('Creator storefront');
      if (bool(row.signal_description_creator_terms)) items.push('Creator terms');
      if (bool(row.signal_description_creator_commercial_terms)) items.push('Commercial creator language');
      if (bool(row.signal_name_creator_terms)) items.push('Creator name signal');
      if (bool(row.strong_creator_profile_flag)) items.push('Strong creator profile');
      return items;
    }
    function riskItems(row) {
      const items = [];
      if (bool(row.creator_commercial_business_like_flag)) items.push('Commercial/business-like creator');
      if (bool(row.signal_possible_business_entity)) items.push('Possible business/entity');
      if (bool(row.signal_network_or_agency)) items.push('Possible agency/network');
      if (bool(row.signal_description_business_terms)) items.push('Business terms');
      if (bool(row.signal_description_agency_terms)) items.push('Agency terms');
      if (bool(row.signal_description_media_terms)) items.push('Media/editorial terms');
      if (bool(row.is_restricted_flag) || bool(row.IsRestricted)) items.push('Restricted');
      if (bool(row.is_test_flag) || bool(row.IsTest)) items.push('Test publisher');
      if (safe(row.BlacklistStatus)) items.push(`Blacklist: ${safe(row.BlacklistStatus)}`);
      return items;
    }
    function qualityItems(row) {
      const items = [];
      items.push(bool(row.has_website) ? 'Website available' : 'Missing website');
      items.push(bool(row.has_description) ? 'Description available' : 'Missing description');
      if (safe(row.publisher_recency_band)) items.push(labelFromSnake(row.publisher_recency_band));
      if (safe(row.publisher_status_norm || row.PublisherStatus)) items.push(`Status: ${safe(row.publisher_status_norm || row.PublisherStatus)}`);
      if (bool(row.is_registered_flag)) items.push('Registered');
      return items;
    }
    async function apiGet(path) {
      const res = await fetch(path, { credentials: 'include' });
      if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
      return res.json();
    }
    async function apiPost(path, payload) {
      const res = await fetch(path, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
      return res.json();
    }

    function Chip({ label, color = 'slate' }) {
      return e('span', { className: `chip ${color}` }, label);
    }
    function MiniChip({ label, tone = '' }) {
      return e('span', { className: `mini-chip ${tone}` }, label);
    }
    function Topbar({ progress, reviewer }) {
      const percentage = pct(progress);
      const total = Number(progress?.total_rows || 0);
      const reviewed = Number(progress?.reviewed_rows || 0);
      const remaining = Number(progress?.remaining_rows || Math.max(total - reviewed, 0));
      return e('header', { className: 'topbar' },
        e('div', { className: 'brand-row' },
          e('div', { className: 'logo-orb' }, 'R'),
          e('div', null,
            e('div', { className: 'brand-title' }, 'Influencer / Content Creator Review'),
            e('div', { className: 'brand-subtitle' }, 'Premium flashcard feedback loop for Publisher Success')
          )
        ),
        e('div', { className: 'progress-zone' },
          e('div', { className: 'progress-label' }, e('span', null, `${reviewed} / ${total} reviewed`), e('span', null, `${remaining} left`)),
          e('div', { className: 'progress-track' }, e('div', { className: 'progress-fill', style: { width: `${percentage}%` } }))
        ),
        e('div', { className: 'reviewer-badge' },
          e('div', { className: 'reviewer-label' }, 'Signed in as'),
          e('div', { className: 'reviewer-name' }, safe(reviewer?.reviewer_name, 'Reviewer')),
          e('div', { className: 'reviewer-email' }, safe(reviewer?.reviewer_email, ''))
        )
      );
    }

    function Flashcard({ row, index, total, remaining, direction, decision }) {
      const bucket = safe(row.priority_bucket);
      const bucketLabel = safe(row.priority_bucket_label, labelFromSnake(bucket) || 'Priority bucket');
      const conf = safe(row.review_confidence_hint);
      const confLabel = safe(row.review_confidence_hint_label, labelFromSnake(conf) || 'Evidence hint');
      const website = safe(row.PublisherWebSite);
      const domain = domainFor(website);
      const description = safe(row.PublisherDescription, 'No description provided.');
      const impact = actionImplication(row);
      const ev = evidenceItems(row);
      const risks = riskItems(row);
      const quality = qualityItems(row);
      const compactEvidence = ev.length ? ev.slice(0, 4).join(' · ') : safe(row.review_evidence_summary, 'No strong creator evidence detected.');
      const activeClass = decision === 'creator' ? 'accept-active' : decision === 'not_creator' ? 'reject-active' : decision === 'unsure' ? 'unsure-active' : '';
      return e('div', { className: 'card-wrap' },
        e('div', { className: 'card-stack' },
          e('article', { key: `${safe(row.PublisherKey)}-${index}`, className: `flashcard ${direction === 'prev' ? 'slide-prev' : 'slide-next'} ${activeClass}` },
            e('div', { className: 'card-topline' },
              e('div', { className: 'card-kicker' }, `Card ${index + 1} of ${total} unreviewed · ${remaining} remaining`),
              e('div', { className: 'impact-pill' }, e('span', { className: 'impact-dot' }), e('span', null, impact.label))
            ),
            e('div', { className: 'chip-row' },
              e(Chip, { label: bucketLabel, color: bucketChip(bucket) }),
              e(Chip, { label: confLabel, color: confidenceChip(conf) }),
              e(Chip, { label: `Website: ${safe(row.website_type, 'unknown')}`, color: 'slate' })
            ),
            e('div', { className: 'publisher-title' }, safe(row.Publisher, 'Unknown publisher')),
            e('div', { className: 'publisher-site' }, website ? e('a', { href: website.startsWith('http') ? website : `https://${website}`, target: '_blank', rel: 'noopener noreferrer' }, domain || website) : 'No website provided'),
            e('div', { className: 'description' }, description),
            e('div', { className: 'stats-grid' },
              e('div', { className: 'stat' }, e('div', { className: 'stat-label' }, 'Vertical'), e('div', { className: 'stat-value', title: safe(row.current_publisher_vertical, '—') }, safe(row.current_publisher_vertical, '—'))),
              e('div', { className: 'stat' }, e('div', { className: 'stat-label' }, 'Current type'), e('div', { className: 'stat-value', title: safe(row.current_publisher_type_group, '—') }, safe(row.current_publisher_type_group, '—'))),
              e('div', { className: 'stat' }, e('div', { className: 'stat-label' }, 'Subvertical'), e('div', { className: 'stat-value', title: safe(row.current_publisher_subvertical, '—') }, safe(row.current_publisher_subvertical, '—'))),
              e('div', { className: 'stat' }, e('div', { className: 'stat-label' }, 'Evidence / risk'), e('div', { className: 'stat-value' }, `${formatScore(row.creator_evidence_score)} / ${formatScore(row.non_creator_risk_score)}`))
            ),
            e('div', { className: 'card-intel' },
              e('div', { className: 'intel-box' },
                e('div', { className: 'intel-label' }, 'Evidence detected'),
                e('div', { className: 'intel-value' }, compactEvidence),
                e('div', { className: 'mini-chipline' }, ev.slice(0, 5).map(item => e(MiniChip, { key: item, label: item, tone: 'good' })))
              ),
              e('div', { className: 'intel-box' },
                e('div', { className: 'intel-label' }, 'Decision impact'),
                e('div', { className: 'intel-value' }, impact.text),
                e('div', { className: 'mini-chipline' },
                  e(MiniChip, { label: `Creator: ${impact.creator}`, tone: 'good' }),
                  e(MiniChip, { label: `Not creator: ${impact.reject}`, tone: 'risk' })
                )
              )
            ),
            risks.length > 0 && e('div', { className: 'mini-chipline', style: { marginTop: 10 } }, risks.slice(0, 4).map(item => e(MiniChip, { key: item, label: item, tone: item.toLowerCase().includes('commercial') ? 'warn' : 'risk' })))
          )
        )
      );
    }

    function ContextPanel({ row }) {
      const [open, setOpen] = useState(false);
      if (!row) return null;
      const impact = actionImplication(row);
      const ev = evidenceItems(row);
      const risks = riskItems(row);
      const quality = qualityItems(row);
      return e('div', { className: 'context-card' },
        e('button', { className: 'context-toggle', onClick: () => setOpen(!open) },
          e('span', null, open ? 'Hide review context' : 'Review context · why this card was selected'),
          e('span', null, open ? '⌃' : '⌄')
        ),
        open && e('div', { className: 'context-body' },
          e('div', { className: 'context-grid' },
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Business implication'),
              e('div', { className: 'context-value' }, impact.text),
              e('div', { className: 'mini-chipline' }, e(MiniChip, { label: `Creator → ${impact.creator}`, tone: 'good' }), e(MiniChip, { label: `Not creator → ${impact.reject}`, tone: 'risk' }), e(MiniChip, { label: 'Unsure → Needs review', tone: 'warn' }))
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Current classification'),
              e('div', { className: 'kv-grid' },
                e('div', { className: 'kv-key' }, 'Vertical'), e('div', { className: 'kv-val' }, safe(row.current_publisher_vertical, '—')),
                e('div', { className: 'kv-key' }, 'Subvertical'), e('div', { className: 'kv-val' }, safe(row.current_publisher_subvertical, '—')),
                e('div', { className: 'kv-key' }, 'Type group'), e('div', { className: 'kv-val' }, safe(row.current_publisher_type_group, '—')),
                e('div', { className: 'kv-key' }, 'Group'), e('div', { className: 'kv-val' }, safe(row.current_publisher_group, '—'))
              )
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Positive creator evidence'),
              e('div', { className: 'flag-cloud' }, ev.length ? ev.map(item => e(MiniChip, { key: item, label: item, tone: 'good' })) : e(MiniChip, { label: 'No strong creator evidence detected' }))
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Risk / warning lens'),
              e('div', { className: 'flag-cloud' }, risks.length ? risks.map(item => e(MiniChip, { key: item, label: item, tone: item.toLowerCase().includes('commercial') ? 'warn' : 'risk' })) : e(MiniChip, { label: 'No major risk flags', tone: 'good' })),
              safe(row.reviewer_warning_message) && e('div', { className: 'warning-box' }, safe(row.reviewer_warning_message))
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Data quality and activity'),
              e('div', { className: 'flag-cloud' }, quality.map((item, i) => e(MiniChip, { key: `${item}-${i}`, label: item, tone: item.toLowerCase().includes('missing') ? 'warn' : '' })))
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Publisher profile'),
              e('div', { className: 'kv-grid' },
                e('div', { className: 'kv-key' }, 'Network'), e('div', { className: 'kv-val' }, safe(row.PublisherNetwork, '—')),
                e('div', { className: 'kv-key' }, 'Location'), e('div', { className: 'kv-val' }, safe(row.PublisherSignUpLocation, '—')),
                e('div', { className: 'kv-key' }, 'Business type'), e('div', { className: 'kv-val' }, safe(row.BusinessTypeName, '—')),
                e('div', { className: 'kv-key' }, 'Status'), e('div', { className: 'kv-val' }, safe(row.PublisherStatus || row.publisher_status_norm, '—'))
              )
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Traffic / promotion context'),
              e('div', { className: 'kv-grid' },
                e('div', { className: 'kv-key' }, 'Promotion'), e('div', { className: 'kv-val' }, safe(row.PromotionType, '—')),
                e('div', { className: 'kv-key' }, 'Parent promo'), e('div', { className: 'kv-val' }, safe(row.ParentPromoType, '—')),
                e('div', { className: 'kv-key' }, 'Traffic type'), e('div', { className: 'kv-val' }, safe(row.TrafficSourceType2, '—')),
                e('div', { className: 'kv-key' }, 'Channel'), e('div', { className: 'kv-val' }, safe(row.TrafficSourceChannel, '—'))
              )
            ),
            e('section', { className: 'context-section' },
              e('div', { className: 'context-label' }, 'Why this publisher is in the queue'),
              e('div', { className: 'context-value' }, safe(row.priority_bucket_description, 'No bucket explanation available.')),
              e('div', { className: 'context-note' }, safe(row.reviewer_guidance_hint, 'Reviewer should inspect manually.'))
            )
          ),
          e('div', { className: 'context-note' }, 'These are supporting signals only. The final decision should still be based on reviewer judgement.')
        )
      );
    }

    function ActionButtons({ selected, onDecision }) {
      return e(React.Fragment, null,
        e('div', { className: 'action-help' }, 'Browse freely with the side arrows. Choose a decision only when you are ready.'),
        e('div', { className: 'action-buttons' },
          e('button', { className: `action-btn reject ${selected === 'not_creator' ? 'active' : ''}`, onClick: () => onDecision('not_creator') }, e('span', { className: 'icon' }, '←'), e('span', null, e('strong', null, 'Not creator'), e('small', null, 'Remove / exclude'))),
          e('button', { className: `action-btn unsure ${selected === 'unsure' ? 'active' : ''}`, onClick: () => onDecision('unsure') }, e('span', { className: 'icon' }, '?'), e('span', null, e('strong', null, 'Unsure'), e('small', null, 'Needs judgement'))),
          e('button', { className: `action-btn accept ${selected === 'creator' ? 'active' : ''}`, onClick: () => onDecision('creator') }, e('span', null, e('strong', null, 'Creator'), e('small', null, 'Keep / add')), e('span', { className: 'icon' }, '→'))
        )
      );
    }

    function ReasonPanel({ decision, row, onCancel, onSaved }) {
      const panelRef = useRef(null);
      const options = REASON_OPTIONS[decision] || [];
      const [reason, setReason] = useState(options[0] || ['', '']);
      const [comment, setComment] = useState('');
      const [saving, setSaving] = useState(false);
      const meta = DECISION_META[decision];
      useEffect(() => {
        setReason(options[0] || ['', '']);
        setComment('');
        setTimeout(() => panelRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' }), 80);
      }, [decision, row?.PublisherKey]);
      async function save() {
        setSaving(true);
        try {
          await apiPost('/api/decision', {
            review_batch_id: row.review_batch_id,
            PublisherKey: Number(row.PublisherKey),
            decision_type: decision,
            review_reason_category: reason[1],
            review_reason_detail: reason[0],
            review_comment: comment,
          });
          onSaved();
        } catch (err) {
          alert(err.message || String(err));
        } finally { setSaving(false); }
      }
      return e('div', { className: 'reason-panel', ref: panelRef },
        e('div', { className: 'reason-title' }, meta.title),
        e('div', { className: 'reason-subtitle' }, meta.subtitle),
        e('div', { className: 'reason-chips' }, options.map(opt => e('button', { key: opt[0], className: `reason-chip ${reason[0] === opt[0] ? 'selected' : ''}`, onClick: () => setReason(opt) }, opt[0]))),
        e('textarea', { value: comment, onChange: ev => setComment(ev.target.value), placeholder: 'Optional note for ambiguous, misleading, or useful cases…' }),
        e('div', { className: 'save-row' },
          e('button', { className: 'save-btn', onClick: save, disabled: saving }, saving ? 'Saving…' : 'Save decision & next'),
          e('button', { className: 'cancel-btn', onClick: onCancel, disabled: saving }, 'Cancel')
        )
      );
    }

    function App() {
      const [loading, setLoading] = useState(true);
      const [error, setError] = useState('');
      const [reviewer, setReviewer] = useState(null);
      const [progress, setProgress] = useState(null);
      const [queue, setQueue] = useState([]);
      const [idx, setIdx] = useState(0);
      const [direction, setDirection] = useState('next');
      const [decision, setDecision] = useState(null);
      const [toast, setToast] = useState('');

      async function load() {
        setError('');
        try {
          const data = await apiGet('/api/queue');
          setReviewer(data.reviewer);
          setProgress(data.progress);
          setQueue(data.queue || []);
          setIdx(prev => Math.min(prev, Math.max((data.queue || []).length - 1, 0)));
        } catch (err) {
          setError(err.message || String(err));
        } finally { setLoading(false); }
      }
      useEffect(() => { load(); }, []);
      const row = queue[idx];
      const totalCards = queue.length;
      function next() { if (idx < totalCards - 1) { setDirection('next'); setDecision(null); setIdx(idx + 1); } }
      function prev() { if (idx > 0) { setDirection('prev'); setDecision(null); setIdx(idx - 1); } }
      function chooseDecision(d) { setDecision(d); }
      async function saved() {
        setDecision(null);
        setToast('Decision saved');
        await load();
        setTimeout(() => setToast(''), 2200);
      }
      useEffect(() => {
        const handler = ev => {
          if (['TEXTAREA', 'INPUT'].includes(document.activeElement?.tagName)) return;
          if (ev.key === 'ArrowRight') next();
          if (ev.key === 'ArrowLeft') prev();
          if (ev.key.toLowerCase() === 'a') chooseDecision('creator');
          if (ev.key.toLowerCase() === 'd') chooseDecision('not_creator');
          if (ev.key.toLowerCase() === 's') chooseDecision('unsure');
          if (ev.key === 'Escape') setDecision(null);
        };
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
      }, [idx, totalCards, row]);

      if (loading) return e('div', { className: 'app-shell' }, e('div', { className: 'ambient-glow' }), e('div', { className: 'empty-state' }, 'Loading review queue…'));
      if (error) return e('div', { className: 'app-shell' }, e('div', { className: 'ambient-glow' }), e('div', { className: 'error-card' }, error));
      if (!row) return e('div', { className: 'app-shell' }, e('div', { className: 'ambient-glow' }), e(Topbar, { progress, reviewer }), e('div', { className: 'empty-state' }, 'All publishers in this review batch have been reviewed.'));

      return e('div', { className: 'app-shell' },
        e('div', { className: 'ambient-glow' }),
        e(Topbar, { progress, reviewer }),
        e('main', { className: 'main-view' },
          e('div', { className: 'stage' },
            e('button', { className: 'nav-button', onClick: prev, disabled: idx <= 0, title: 'Previous card' }, '‹'),
            e(Flashcard, { row, index: idx, total: totalCards, remaining: totalCards, direction, decision }),
            e('button', { className: 'nav-button', onClick: next, disabled: idx >= totalCards - 1, title: 'Next card' }, '›')
          ),
          e(ContextPanel, { row }),
          e(ActionButtons, { selected: decision, onDecision: chooseDecision }),
          e('div', { className: 'shortcuts' }, 'Shortcuts: ← previous · → next · A creator · D not creator · S unsure · Esc cancel'),
          decision && e(ReasonPanel, { decision, row, onCancel: () => setDecision(null), onSaved: saved })
        ),
        toast && e('div', { className: 'toast' }, toast)
      );
    }
    ReactDOM.createRoot(document.getElementById('root')).render(e(App));
  </script>
</body>
</html>
'''


@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse(INDEX_HTML)
