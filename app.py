"""
Influencer / Content Creator Flashcard Review App

Production-oriented Streamlit app for Databricks Apps.
- Reads review queue from a Databricks Delta table.
- Writes reviewer decisions to a Databricks Delta table using MERGE.
- Uses Databricks App authentication headers to identify the reviewer.

Expected source table:
    ml_prod.sandbox.publisher_influencer_review_handoff_v1_1

Expected decisions table:
    ml_prod.sandbox.publisher_influencer_review_decisions_v1
"""

from __future__ import annotations

import html
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# =========================================================
# App configuration
# =========================================================
st.set_page_config(
    page_title="Influencer Review",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

QUEUE_TABLE = os.getenv(
    "REVIEW_QUEUE_TABLE",
    "ml_prod.sandbox.publisher_influencer_review_handoff_v1_1",
)
DECISIONS_TABLE = os.getenv(
    "REVIEW_DECISIONS_TABLE",
    "ml_prod.sandbox.publisher_influencer_review_decisions_v1",
)
DECISION_SOURCE = os.getenv("DECISION_SOURCE", "databricks_app_flashcard_ui")

# Local fallback identity only applies outside Databricks Apps.
LOCAL_REVIEWER_EMAIL = os.getenv("LOCAL_REVIEWER_EMAIL", "local_reviewer@local.dev")
LOCAL_REVIEWER_NAME = os.getenv("LOCAL_REVIEWER_NAME", "Local Reviewer")

# =========================================================
# CSS / visual system
# =========================================================
st.markdown(
    """
    <style>
    .block-container {
        max-width: 1440px;
        padding-top: 0.6rem;
        padding-bottom: 1.2rem;
    }

    .stApp {
        background:
            radial-gradient(circle at 12% 5%, rgba(91,140,255,0.16), transparent 25%),
            radial-gradient(circle at 88% 10%, rgba(168,85,247,0.12), transparent 23%),
            radial-gradient(circle at 50% 100%, rgba(20,184,166,0.08), transparent 28%),
            linear-gradient(180deg, #07111F 0%, #091525 100%);
        color: #F8FBFF;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0A1322 0%, #0C182A 100%);
        border-right: 1px solid rgba(255,255,255,0.06);
    }

    .topbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        background: rgba(10, 20, 35, 0.78);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 22px;
        padding: 14px 18px;
        margin-bottom: 16px;
        box-shadow: 0 12px 32px rgba(0,0,0,0.22);
        backdrop-filter: blur(12px);
    }

    .topbar-title {
        font-size: 1.2rem;
        font-weight: 900;
        color: #F8FBFF;
        letter-spacing: -0.02em;
    }

    .topbar-subtitle {
        font-size: 0.88rem;
        color: #AFC0D8;
        margin-top: 2px;
    }

    .reviewer-badge {
        text-align: right;
        min-width: 250px;
    }

    .reviewer-name {
        font-size: 0.95rem;
        font-weight: 800;
        color: #F8FBFF;
    }

    .reviewer-email {
        font-size: 0.8rem;
        color: #9FB2CC;
        margin-top: 2px;
    }

    .progress-shell {
        width: 100%;
        height: 8px;
        background: rgba(255,255,255,0.07);
        border-radius: 999px;
        overflow: hidden;
        margin: 8px 0 10px 0;
    }

    .progress-fill {
        height: 8px;
        border-radius: 999px;
        background: linear-gradient(90deg, #5B8CFF 0%, #14B8A6 100%);
        box-shadow: 0 0 20px rgba(91,140,255,0.30);
    }

    .flashcard-wrap {
        max-width: 860px;
        margin: 0 auto;
    }

    .flashcard {
        position: relative;
        overflow: hidden;
        background:
            radial-gradient(circle at 90% 5%, rgba(91,140,255,0.16), transparent 26%),
            linear-gradient(145deg, rgba(18,31,52,0.94) 0%, rgba(11,22,39,0.96) 100%);
        border: 1px solid rgba(126,167,255,0.20);
        border-radius: 34px;
        padding: 30px 34px 26px 34px;
        box-shadow:
            0 24px 70px rgba(0,0,0,0.38),
            inset 0 1px 0 rgba(255,255,255,0.05),
            0 0 48px rgba(91,140,255,0.10);
        min-height: 520px;
    }

    .flashcard:before {
        content: "";
        position: absolute;
        width: 260px;
        height: 260px;
        top: -90px;
        right: -80px;
        background: radial-gradient(circle, rgba(20,184,166,0.20) 0%, transparent 70%);
        pointer-events: none;
    }

    .flashcard-kicker {
        font-size: 0.78rem;
        color: #8FA6C6;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.09em;
        margin-bottom: 12px;
    }

    .publisher-title {
        font-size: 2.25rem;
        line-height: 1.04;
        font-weight: 950;
        color: #F8FBFF;
        letter-spacing: -0.035em;
        margin: 4px 0 12px 0;
        max-width: 780px;
    }

    .publisher-site {
        margin: 0 0 18px 0;
        color: #C9D6EA;
        font-size: 0.96rem;
    }

    .publisher-site a {
        color: #9CC0FF !important;
        font-weight: 750;
        text-decoration: none;
    }

    .publisher-site a:hover {
        text-decoration: underline;
    }

    .description-card {
        background: rgba(7, 17, 31, 0.60);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 22px;
        padding: 18px 18px;
        color: #F1F6FF;
        font-size: 1.05rem;
        line-height: 1.62;
        margin: 16px 0 18px 0;
    }

    .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin: 10px 0 8px 0;
    }

    .chip {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 7px 11px;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 850;
        border: 1px solid transparent;
        white-space: nowrap;
    }

    .chip-green { background: rgba(16,185,129,0.17); color: #BBF7D0; border-color: rgba(74,222,128,0.24); }
    .chip-blue { background: rgba(59,130,246,0.16); color: #BFD7FF; border-color: rgba(96,165,250,0.26); }
    .chip-amber { background: rgba(245,158,11,0.17); color: #FCD9A6; border-color: rgba(251,191,36,0.26); }
    .chip-red { background: rgba(239,68,68,0.16); color: #FECACA; border-color: rgba(248,113,113,0.24); }
    .chip-slate { background: rgba(148,163,184,0.13); color: #D7E0EC; border-color: rgba(203,213,225,0.13); }
    .chip-purple { background: rgba(168,85,247,0.16); color: #E9D5FF; border-color: rgba(196,181,253,0.22); }

    .evidence-box {
        margin-top: 18px;
        background: linear-gradient(135deg, rgba(15,28,48,0.74) 0%, rgba(10,22,39,0.76) 100%);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 22px;
        padding: 15px 16px;
    }

    .section-label {
        color: #A6B8D3;
        font-size: 0.74rem;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 6px;
    }

    .section-value {
        color: #EAF1FF;
        font-size: 0.95rem;
        line-height: 1.55;
    }

    .warning-box {
        margin-top: 12px;
        background: rgba(245,158,11,0.14);
        border: 1px solid rgba(251,191,36,0.25);
        color: #FDECC8;
        border-radius: 16px;
        padding: 10px 12px;
        font-weight: 700;
        font-size: 0.90rem;
    }

    .card-footer-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
        margin-top: 16px;
    }

    .mini-stat {
        background: rgba(7, 17, 31, 0.50);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 10px 12px;
    }

    .mini-stat-label {
        color: #8FA6C6;
        font-size: 0.70rem;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .mini-stat-value {
        color: #F8FBFF;
        font-size: 0.98rem;
        font-weight: 900;
        margin-top: 2px;
    }

    .decision-zone {
        max-width: 860px;
        margin: 18px auto 0 auto;
    }

    .decision-help {
        text-align: center;
        color: #9FB2CC;
        font-size: 0.88rem;
        margin-top: 10px;
        margin-bottom: 4px;
    }

    .pending-box {
        max-width: 860px;
        margin: 16px auto 0 auto;
        background: rgba(10, 20, 35, 0.78);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 24px;
        padding: 18px 18px 14px 18px;
        box-shadow: 0 16px 40px rgba(0,0,0,0.24);
    }

    .pending-title {
        font-size: 1.05rem;
        font-weight: 900;
        color: #F8FBFF;
        margin-bottom: 8px;
    }

    .empty-state {
        max-width: 720px;
        margin: 80px auto;
        text-align: center;
        background: rgba(10, 20, 35, 0.78);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 30px;
        padding: 34px;
        box-shadow: 0 24px 70px rgba(0,0,0,0.28);
    }

    .empty-state h1 {
        color: #F8FBFF;
        font-size: 2rem;
        margin-bottom: 8px;
    }

    .empty-state p {
        color: #B4C3D9;
        font-size: 1rem;
        line-height: 1.5;
    }

    div.stButton > button {
        border-radius: 18px !important;
        min-height: 54px !important;
        border: 1px solid rgba(255,255,255,0.09) !important;
        background: linear-gradient(135deg, rgba(18,31,52,0.96) 0%, rgba(14,24,40,0.96) 100%) !important;
        color: #F8FBFF !important;
        font-weight: 900 !important;
        transition: all 0.14s ease-in-out !important;
    }

    div.stButton > button:hover {
        border-color: rgba(91,140,255,0.34) !important;
        box-shadow: 0 0 0 1px rgba(91,140,255,0.10), 0 0 28px rgba(91,140,255,0.12);
        transform: translateY(-1px);
    }

    [data-testid="stMetric"] {
        background: rgba(12,22,38,0.72);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 18px;
        padding: 10px 14px;
    }

    [data-testid="stMetric"] label { color: #A6B8D3 !important; }
    [data-testid="stMetricValue"] { color: #F8FBFF !important; }

    div[data-baseweb="input"] > div,
    div[data-baseweb="select"] > div,
    textarea {
        border-radius: 14px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# Small helpers
# =========================================================
def esc(value: Any) -> str:
    """HTML-escape any user/data value before rendering with unsafe_allow_html."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return html.escape(str(value), quote=True)


def safe_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return safe_str(value).strip().lower() in {"true", "1", "yes", "y", "t"}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "" or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def safe_float_str(value: Any, default: str = "—") -> str:
    try:
        if value is None or value == "" or pd.isna(value):
            return default
        return str(int(float(value)))
    except Exception:
        return safe_str(value) or default


def normalise_url(url: Any) -> str:
    url = safe_str(url).strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def extract_domain(url: Any) -> str:
    url = normalise_url(url)
    if not url:
        return ""
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return safe_str(url)


def format_pct(part: int, total: int) -> float:
    if not total:
        return 0.0
    return round(part / total * 100, 1)

# =========================================================
# Databricks connection helpers
# =========================================================
def _server_hostname_from_host(host: str) -> str:
    host = host or ""
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def _get_http_path() -> str:
    direct = os.getenv("DATABRICKS_HTTP_PATH") or os.getenv("DATABRICKS_SQL_HTTP_PATH")
    if direct:
        return direct

    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
    if warehouse_id:
        return f"/sql/1.0/warehouses/{warehouse_id}"

    return ""


@st.cache_resource(show_spinner=False)
def get_databricks_config() -> Config:
    return Config()


def get_connection():
    cfg = get_databricks_config()
    http_path = _get_http_path()

    if not http_path:
        st.error(
            "Databricks SQL warehouse is not configured. Set DATABRICKS_HTTP_PATH "
            "or DATABRICKS_WAREHOUSE_ID in the Databricks App environment."
        )
        st.stop()

    return sql.connect(
        server_hostname=_server_hostname_from_host(cfg.host),
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )


def query_df(query: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, parameters=params or [])
            return cursor.fetchall_arrow().to_pandas()


def execute_sql(query: str, params: Optional[List[Any]] = None) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, parameters=params or [])

# =========================================================
# Auth helpers
# =========================================================
def get_authenticated_reviewer() -> Dict[str, str]:
    """
    In Databricks Apps, reviewer identity is expected to come from auth/proxy headers.
    Locally, falls back to LOCAL_REVIEWER_EMAIL / LOCAL_REVIEWER_NAME.
    """
    headers_raw = {}
    try:
        headers_raw = dict(getattr(st.context, "headers", {}) or {})
    except Exception:
        headers_raw = {}

    headers = {str(k).lower(): str(v) for k, v in headers_raw.items()}

    email = (
        headers.get("x-forwarded-email")
        or headers.get("x-databricks-user-email")
        or headers.get("x-forwarded-user")
        or os.getenv("REVIEWER_EMAIL")
        or LOCAL_REVIEWER_EMAIL
    )

    name = (
        headers.get("x-forwarded-preferred-username")
        or headers.get("x-databricks-user-name")
        or os.getenv("REVIEWER_NAME")
        or ""
    )

    if not name and email:
        name = email.split("@")[0].replace(".", " ").replace("_", " ").title()

    return {
        "reviewer_email": email.strip().lower(),
        "reviewer_name": name.strip() or "Authenticated Reviewer",
    }

# =========================================================
# Data access
# =========================================================
@st.cache_data(ttl=20, show_spinner="Loading review queue…")
def load_review_rows() -> pd.DataFrame:
    query = f"""
        SELECT
            q.*,
            CASE WHEN d.PublisherKey IS NULL THEN false ELSE true END AS is_reviewed,
            d.reviewed_cluster_label AS saved_reviewed_cluster_label,
            d.review_outcome AS saved_review_outcome,
            d.review_reason_category AS saved_review_reason_category,
            d.review_reason_detail AS saved_review_reason_detail,
            d.review_comment AS saved_review_comment,
            d.reviewer_name AS saved_reviewer_name,
            d.reviewer_email AS saved_reviewer_email,
            d.reviewed_at AS saved_reviewed_at
        FROM {QUEUE_TABLE} q
        LEFT JOIN {DECISIONS_TABLE} d
            ON q.review_batch_id = d.review_batch_id
           AND q.PublisherKey = d.PublisherKey
        ORDER BY q.review_sequence
    """
    df = query_df(query)

    # Keep app resilient if optional columns are not present.
    required_cols = [
        "review_sequence", "review_batch_id", "PublisherKey", "Publisher", "PublisherWebSite",
        "PublisherDescription", "priority_bucket", "priority_bucket_label",
        "priority_bucket_description", "candidate_reason", "website_type", "website_domain",
        "review_confidence_hint", "review_confidence_hint_label", "creator_evidence_score",
        "non_creator_risk_score", "review_evidence_summary", "reviewer_guidance_hint",
        "reviewer_warning_message", "current_publisher_type_group", "current_publisher_subvertical",
        "signal_current_cluster", "is_reviewed", "saved_reviewer_email", "saved_reviewer_name",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    if not df.empty:
        df["PublisherKey"] = df["PublisherKey"].astype(str)
        df["review_sequence_sort"] = pd.to_numeric(df["review_sequence"], errors="coerce")
        df = df.sort_values(["review_sequence_sort", "PublisherKey"], na_position="last").reset_index(drop=True)

    return df


def save_decision_to_delta(decision: Dict[str, Any]) -> Tuple[bool, str]:
    existing_query = f"""
        SELECT
            reviewer_email,
            reviewer_name
        FROM {DECISIONS_TABLE}
        WHERE review_batch_id = ?
          AND PublisherKey = ?
        LIMIT 1
    """

    existing = query_df(
        existing_query,
        [decision["review_batch_id"], safe_int(decision["PublisherKey"])],
    )

    if not existing.empty:
        existing_email = safe_str(existing.iloc[0].get("reviewer_email", "")).strip().lower()
        current_email = safe_str(decision.get("reviewer_email", "")).strip().lower()

        if existing_email and existing_email != current_email:
            existing_name = safe_str(existing.iloc[0].get("reviewer_name", existing_email))
            return False, f"Already reviewed by {existing_name}. This card is read-only."

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

    reviewed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    execute_sql(
        merge_sql,
        [
            decision["review_batch_id"],
            safe_int(decision["PublisherKey"]),
            decision["reviewed_cluster_label"],
            decision["review_outcome"],
            decision["review_reason_category"],
            decision.get("review_reason_detail", ""),
            decision.get("review_comment", ""),
            decision["reviewer_name"],
            decision["reviewer_email"],
            reviewed_at,
            decision.get("decision_source", DECISION_SOURCE),
            safe_int(decision.get("review_sequence", 0)),
            decision.get("priority_bucket", ""),
            decision.get("review_confidence_hint", ""),
            safe_int(decision.get("creator_evidence_score", 0)),
            safe_int(decision.get("non_creator_risk_score", 0)),
        ],
    )

    st.cache_data.clear()
    return True, "Saved."

# =========================================================
# Review logic helpers
# =========================================================
def is_current_cluster(row: pd.Series) -> bool:
    bucket = safe_str(row.get("priority_bucket", ""))
    return boolish(row.get("signal_current_cluster")) or bucket in {
        "p1_current_cluster_strong",
        "p2_current_cluster",
    }


def get_review_outcome(label: str, row: pd.Series) -> str:
    current = is_current_cluster(row)
    if label == "belongs":
        return "accepted_current_cluster" if current else "add_to_cluster"
    if label == "does_not_belong":
        return "remove_from_cluster" if current else "unclear"
    return "unclear"


REASON_OPTIONS = {
    "belongs": [
        ("Creator / personal brand", "creator_individual_or_creator_brand"),
        ("Social profile", "creator_individual_or_creator_brand"),
        ("UGC / product review creator", "creator_individual_or_creator_brand"),
        ("Creator-commerce / affiliate", "creator_individual_or_creator_brand"),
        ("Looks like influencer/content creator", "creator_individual_or_creator_brand"),
        ("Other", "other"),
    ],
    "does_not_belong": [
        ("Business/company", "business_or_company"),
        ("Agency/network", "agency_or_network"),
        ("Media/editorial publisher", "publisher_or_media"),
        ("Utility or non-creator site", "utility_or_non_creator"),
        ("Not enough creator evidence", "insufficient_information"),
        ("Other", "other"),
    ],
    "unsure": [
        ("Missing or weak website", "insufficient_information"),
        ("Missing or weak description", "insufficient_information"),
        ("Conflicting signals", "insufficient_information"),
        ("Needs manual escalation", "other"),
        ("Other", "other"),
    ],
}

DECISION_LABELS = {
    "belongs": "Creator",
    "does_not_belong": "Not creator",
    "unsure": "Unsure",
}


def confidence_chip_class(raw_hint: str) -> str:
    raw_hint = safe_str(raw_hint)
    if raw_hint == "likely_creator":
        return "chip-green"
    if raw_hint == "possible_creator":
        return "chip-blue"
    if raw_hint == "creator_commercial_business_like":
        return "chip-amber"
    if raw_hint == "likely_not_creator":
        return "chip-red"
    return "chip-slate"


def bucket_chip_class(bucket: str) -> str:
    if bucket in {"p1_current_cluster_strong", "p3_hidden_positive_strong"}:
        return "chip-green"
    if bucket in {"p2_current_cluster", "p6_social_and_keyword"}:
        return "chip-blue"
    if bucket in {"p4_hidden_positive", "p5_adjacent_supported"}:
        return "chip-amber"
    return "chip-slate"


def review_status_for_row(row: pd.Series, reviewer_email: str) -> Tuple[bool, bool]:
    """Returns (is_reviewed, is_locked_by_other)."""
    is_reviewed = boolish(row.get("is_reviewed"))
    saved_email = safe_str(row.get("saved_reviewer_email", "")).strip().lower()
    locked = is_reviewed and saved_email and saved_email != reviewer_email.strip().lower()
    return is_reviewed, locked

# =========================================================
# HTML render helpers
# =========================================================
def render_topbar(reviewer: Dict[str, str], reviewed: int, total: int) -> None:
    pct = format_pct(reviewed, total)
    st.markdown(
        f"""
        <div class="topbar">
            <div>
                <div class="topbar-title">Influencer / Content Creator Review</div>
                <div class="topbar-subtitle">Flashcard feedback loop · {reviewed} / {total} reviewed · {pct}% complete</div>
                <div class="progress-shell"><div class="progress-fill" style="width:{pct}%;"></div></div>
            </div>
            <div class="reviewer-badge">
                <div class="reviewer-name">{esc(reviewer['reviewer_name'])}</div>
                <div class="reviewer-email">{esc(reviewer['reviewer_email'])}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_flashcard(row: pd.Series, position: int, remaining: int) -> None:
    publisher = esc(row.get("Publisher", "Unknown publisher"))
    website = normalise_url(row.get("PublisherWebSite", ""))
    domain = esc(extract_domain(website))
    description = esc(row.get("PublisherDescription", "No description provided.")).replace("\n", "<br>")

    bucket = safe_str(row.get("priority_bucket", ""))
    bucket_label = esc(row.get("priority_bucket_label", bucket or "Priority bucket"))
    bucket_description = esc(row.get("priority_bucket_description", ""))
    confidence_raw = safe_str(row.get("review_confidence_hint", ""))
    confidence_label = esc(row.get("review_confidence_hint_label", confidence_raw or "No hint"))
    website_type = esc(row.get("website_type", "unknown"))
    evidence = esc(row.get("review_evidence_summary", "No evidence summary available."))
    guidance = esc(row.get("reviewer_guidance_hint", "Reviewer should inspect manually."))
    warning = esc(row.get("reviewer_warning_message", ""))

    current_type = esc(row.get("current_publisher_type_group", "")) or "—"
    current_subvertical = esc(row.get("current_publisher_subvertical", "")) or "—"
    creator_score = safe_float_str(row.get("creator_evidence_score", ""))
    risk_score = safe_float_str(row.get("non_creator_risk_score", ""))

    warning_html = f'<div class="warning-box">{warning}</div>' if warning else ""
    site_html = (
        f'<a href="{esc(website)}" target="_blank">{domain or esc(website)}</a>'
        if website
        else "No website provided"
    )

    st.markdown(
        f"""
        <div class="flashcard-wrap">
            <div class="flashcard">
                <div class="flashcard-kicker">Card {position} · {remaining} remaining</div>
                <div class="chips">
                    <span class="chip {bucket_chip_class(bucket)}">{bucket_label}</span>
                    <span class="chip {confidence_chip_class(confidence_raw)}">{confidence_label}</span>
                    <span class="chip chip-slate">Website: {website_type}</span>
                </div>
                <div class="publisher-title">{publisher}</div>
                <div class="publisher-site">{site_html}</div>

                <div class="description-card">{description}</div>

                <div class="evidence-box">
                    <div class="section-label">Reviewer guidance</div>
                    <div class="section-value">{guidance}</div>
                    {warning_html}
                    <div style="height:12px;"></div>
                    <div class="section-label">Evidence detected</div>
                    <div class="section-value">{evidence}</div>
                </div>

                <div class="card-footer-grid">
                    <div class="mini-stat">
                        <div class="mini-stat-label">Current type</div>
                        <div class="mini-stat-value">{current_type}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Current subvertical</div>
                        <div class="mini-stat-value">{current_subvertical}</div>
                    </div>
                    <div class="mini-stat">
                        <div class="mini-stat-label">Evidence / risk</div>
                        <div class="mini-stat-value">{creator_score} / {risk_score}</div>
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# =========================================================
# Session state
# =========================================================
reviewer = get_authenticated_reviewer()
st.session_state["reviewer_name"] = reviewer["reviewer_name"]
st.session_state["reviewer_email"] = reviewer["reviewer_email"]

if "current_idx" not in st.session_state:
    st.session_state.current_idx = 0
if "pending_decision" not in st.session_state:
    st.session_state.pending_decision = None
if "override_publisher_key" not in st.session_state:
    st.session_state.override_publisher_key = None

# =========================================================
# Load data
# =========================================================
try:
    rows = load_review_rows()
except Exception as exc:
    st.error("Could not load review data from Databricks.")
    st.exception(exc)
    st.stop()

if rows.empty:
    st.error("The review handoff table returned no rows.")
    st.stop()

total_rows = len(rows)
reviewed_rows = int(rows["is_reviewed"].apply(boolish).sum())
remaining_rows = total_rows - reviewed_rows

# =========================================================
# Tabs
# =========================================================
tab_review, tab_progress, tab_guide = st.tabs(["Review", "Progress", "Guide"])

with tab_review:
    render_topbar(reviewer, reviewed_rows, total_rows)

    reviewer_email = st.session_state["reviewer_email"]

    # Determine current row.
    override_key = st.session_state.get("override_publisher_key")
    current_row = None
    is_override_mode = False

    if override_key:
        match = rows[rows["PublisherKey"].astype(str) == str(override_key)]
        if not match.empty:
            current_row = match.iloc[0]
            is_override_mode = True

    if current_row is None:
        unreviewed = rows[~rows["is_reviewed"].apply(boolish)].reset_index(drop=True)

        if unreviewed.empty:
            st.markdown(
                """
                <div class="empty-state">
                    <h1>All cards reviewed</h1>
                    <p>The first review wave is complete. You can inspect the progress tab for decision breakdowns.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.stop()

        st.session_state.current_idx = min(st.session_state.current_idx, len(unreviewed) - 1)
        current_row = unreviewed.iloc[st.session_state.current_idx]

    is_reviewed, locked_by_other = review_status_for_row(current_row, reviewer_email)
    position = safe_int(current_row.get("review_sequence", st.session_state.current_idx + 1), st.session_state.current_idx + 1)

    render_flashcard(current_row, position=position, remaining=remaining_rows)

    with st.expander("Show full technical context"):
        technical_cols = [
            "PublisherKey", "review_batch_id", "review_sequence", "priority_bucket", "candidate_reason",
            "review_confidence_hint", "strong_creator_profile_flag", "creator_commercial_business_like_flag",
            "creator_evidence_score", "non_creator_risk_score", "signal_possible_business_entity",
            "signal_network_or_agency", "is_social_profile_url", "is_link_in_bio_url",
            "is_creator_storefront_url", "taxonomy_current_cluster_risk_flag",
        ]
        context = []
        for col in technical_cols:
            if col in current_row.index:
                context.append({"field": col, "value": safe_str(current_row.get(col, ""))})
        st.dataframe(pd.DataFrame(context), use_container_width=True, hide_index=True)

    if locked_by_other:
        st.warning(
            f"This card was already reviewed by {safe_str(current_row.get('saved_reviewer_name', 'another reviewer'))}. "
            "It is read-only for you."
        )
        if st.button("Next unreviewed card", use_container_width=True):
            st.session_state.override_publisher_key = None
            st.session_state.pending_decision = None
            st.rerun()

    else:
        # Decision buttons.
        if st.session_state.pending_decision is None:
            st.markdown('<div class="decision-zone">', unsafe_allow_html=True)
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("← Not creator", use_container_width=True):
                    st.session_state.pending_decision = "does_not_belong"
                    st.rerun()
            with c2:
                if st.button("Unsure", use_container_width=True):
                    st.session_state.pending_decision = "unsure"
                    st.rerun()
            with c3:
                if st.button("Creator →", use_container_width=True):
                    st.session_state.pending_decision = "belongs"
                    st.rerun()
            st.markdown('</div><div class="decision-help">Choose the simplest accurate decision. You can add a quick reason next.</div>', unsafe_allow_html=True)

            if is_override_mode:
                if st.button("Back to unreviewed queue", use_container_width=True):
                    st.session_state.override_publisher_key = None
                    st.session_state.pending_decision = None
                    st.rerun()

        else:
            label = st.session_state.pending_decision
            st.markdown(
                f"""
                <div class="pending-box">
                    <div class="pending-title">You chose: {esc(DECISION_LABELS[label])}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            reason_options = REASON_OPTIONS[label]
            reason_labels = [x[0] for x in reason_options]

            with st.form(key=f"decision_form_{safe_str(current_row.get('PublisherKey'))}_{label}"):
                reason_detail = st.radio(
                    "Why?",
                    reason_labels,
                    horizontal=False,
                )
                reason_category = dict(reason_options)[reason_detail]
                review_comment = st.text_area(
                    "Optional comment",
                    placeholder="Add context only if useful, especially for ambiguous cases…",
                    height=90,
                )

                save_col, back_col = st.columns([2, 1])
                save_clicked = save_col.form_submit_button("Save and show next card", use_container_width=True)
                back_clicked = back_col.form_submit_button("Change choice", use_container_width=True)

                if back_clicked:
                    st.session_state.pending_decision = None
                    st.rerun()

                if save_clicked:
                    decision_payload = {
                        "review_batch_id": safe_str(current_row.get("review_batch_id", "benchmark_v1_wave_001")),
                        "PublisherKey": safe_str(current_row.get("PublisherKey", "")),
                        "reviewed_cluster_label": label,
                        "review_outcome": get_review_outcome(label, current_row),
                        "review_reason_category": reason_category,
                        "review_reason_detail": reason_detail,
                        "review_comment": review_comment,
                        "reviewer_name": st.session_state["reviewer_name"],
                        "reviewer_email": st.session_state["reviewer_email"],
                        "decision_source": DECISION_SOURCE,
                        "review_sequence": safe_int(current_row.get("review_sequence", 0)),
                        "priority_bucket": safe_str(current_row.get("priority_bucket", "")),
                        "review_confidence_hint": safe_str(current_row.get("review_confidence_hint", "")),
                        "creator_evidence_score": safe_int(current_row.get("creator_evidence_score", 0)),
                        "non_creator_risk_score": safe_int(current_row.get("non_creator_risk_score", 0)),
                    }

                    ok, message = save_decision_to_delta(decision_payload)
                    if ok:
                        st.toast("Decision saved", icon="✅")
                        st.session_state.pending_decision = None
                        st.session_state.override_publisher_key = None
                        st.rerun()
                    else:
                        st.error(message)

with tab_progress:
    render_topbar(reviewer, reviewed_rows, total_rows)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total cards", total_rows)
    m2.metric("Reviewed", reviewed_rows)
    m3.metric("Remaining", remaining_rows)
    m4.metric("Completion", f"{format_pct(reviewed_rows, total_rows)}%")

    st.markdown("### Progress by bucket")
    bucket_progress = (
        rows.groupby(["priority_bucket_label", "priority_bucket"], dropna=False)
        .agg(total=("PublisherKey", "count"), reviewed=("is_reviewed", lambda s: int(s.apply(boolish).sum())))
        .reset_index()
    )
    bucket_progress["remaining"] = bucket_progress["total"] - bucket_progress["reviewed"]
    bucket_progress["completion_pct"] = (bucket_progress["reviewed"] / bucket_progress["total"] * 100).round(1)
    st.dataframe(bucket_progress, use_container_width=True, hide_index=True)

    reviewed_only = rows[rows["is_reviewed"].apply(boolish)].copy()
    if not reviewed_only.empty:
        st.markdown("### Decision breakdown")
        decision_breakdown = (
            reviewed_only.groupby(["saved_reviewed_cluster_label", "saved_review_reason_category"], dropna=False)
            .size()
            .reset_index(name="rows")
            .sort_values("rows", ascending=False)
        )
        st.dataframe(decision_breakdown, use_container_width=True, hide_index=True)

        my_rows = reviewed_only[
            reviewed_only["saved_reviewer_email"].fillna("").str.lower() == reviewer["reviewer_email"].lower()
        ].copy()
        if not my_rows.empty:
            st.markdown("### Edit one of your reviews")
            my_rows["open_label"] = my_rows["Publisher"].astype(str) + " (" + my_rows["PublisherKey"].astype(str) + ")"
            selected = st.selectbox("Choose a card to reopen", my_rows["open_label"].tolist())
            if st.button("Open selected card", use_container_width=True):
                selected_key = my_rows.loc[my_rows["open_label"] == selected, "PublisherKey"].iloc[0]
                st.session_state.override_publisher_key = str(selected_key)
                st.session_state.pending_decision = None
                st.rerun()
    else:
        st.info("No decisions have been saved yet.")

with tab_guide:
    render_topbar(reviewer, reviewed_rows, total_rows)

    st.markdown("### What you are deciding")
    st.markdown(
        """
        For each publisher, decide whether it genuinely belongs in the **Influencer / Content Creator** cluster.

        A publisher belongs if its value is mainly driven by a creator, personal brand, creator audience, social content, UGC, or creator-led affiliate/content activity.

        A publisher does not belong if it is mainly a business/company, agency/network, traditional media/editorial publisher, utility site, or non-creator profile.
        """
    )

    st.markdown("### The three decisions")
    guide_df = pd.DataFrame(
        [
            {"Decision": "Creator", "Use when": "The publisher looks genuinely creator-led.", "Output": "belongs"},
            {"Decision": "Not creator", "Use when": "The publisher is mainly a business, agency, media site, or non-creator.", "Output": "does_not_belong"},
            {"Decision": "Unsure", "Use when": "There is not enough evidence or the signals conflict.", "Output": "unsure"},
        ]
    )
    st.dataframe(guide_df, use_container_width=True, hide_index=True)

    st.markdown("### About the EDA evidence")
    st.markdown(
        """
        The card shows supporting evidence from the enriched benchmark table, such as website type, detected creator signals, business/agency risk, and a short reviewer guidance hint.

        These signals are **not the final answer**. They are shown to make the review easier and more consistent. The stakeholder review decision is still the ground-truth feedback we want to capture.
        """
    )
