import os
import re
import html
from textwrap import dedent
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config


# ============================================================
# Streamlit page config
# ============================================================

st.set_page_config(
    page_title="Influencer Review",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ============================================================
# App configuration
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
    "databricks_app_flashcard_ui",
)

SHOW_DEBUG_CONTEXT = os.getenv("SHOW_DEBUG_CONTEXT", "false").lower() == "true"


# ============================================================
# Basic helpers
# ============================================================

def safe_str(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def esc(value) -> str:
    return html.escape(safe_str(value), quote=True)


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    text = safe_str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def safe_int(value, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def safe_float_str(value) -> str:
    try:
        if value is None or pd.isna(value) or value == "":
            return "—"
        num = float(value)
        if num.is_integer():
            return str(int(num))
        return f"{num:.1f}"
    except Exception:
        text = safe_str(value).strip()
        return text if text else "—"


def format_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0"
    return f"{100.0 * numerator / denominator:.1f}"


def normalise_url(url: str) -> str:
    url = safe_str(url).strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        return f"https://{url}"
    return url


def extract_domain(url: str) -> str:
    url = normalise_url(url)
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        return domain.replace("www.", "")
    except Exception:
        return (
            url.replace("https://", "")
            .replace("http://", "")
            .replace("www.", "")
            .split("/")[0]
        )


def render_html(raw_html: str) -> None:
    """
    Renders compact HTML safely in Streamlit without markdown treating
    indented HTML as a code block.
    """
    cleaned = dedent(raw_html).strip()
    cleaned = "\n".join(line.strip() for line in cleaned.splitlines())
    st.markdown(cleaned, unsafe_allow_html=True)


def validate_table_name(table_name: str) -> str:
    """
    Small safety check for env-provided Unity Catalog table names.
    Supports catalog.schema.table format.
    """
    name = safe_str(table_name).strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$", name):
        st.error(f"Invalid table name configured: {name}")
        st.stop()
    return name


QUEUE_TABLE = validate_table_name(QUEUE_TABLE)
DECISIONS_TABLE = validate_table_name(DECISIONS_TABLE)


# ============================================================
# Databricks SQL connection
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
    host = os.getenv("DATABRICKS_HOST", "").strip() or safe_str(cfg.host).strip()
    host = host.replace("https://", "").replace("http://", "").strip("/")
    return host


def get_connection():
    http_path = get_http_path()

    if not http_path:
        st.error(
            "Databricks SQL warehouse is not configured. "
            "Set DATABRICKS_HTTP_PATH or DATABRICKS_WAREHOUSE_ID in the Databricks App environment."
        )
        st.stop()

    cfg = Config()
    server_hostname = get_server_hostname(cfg)

    if not server_hostname:
        st.error("Databricks host is not configured in the app runtime.")
        st.stop()

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def query_df(query: str, params: List = None) -> pd.DataFrame:
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


def execute_sql(query: str, params: List = None) -> None:
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
# Reviewer identity
# ============================================================

def get_authenticated_reviewer() -> Dict[str, str]:
    """
    Attempts to read reviewer identity from Databricks Apps / proxy headers.
    Falls back to local/dev identity if headers are unavailable.
    """

    raw_headers = {}
    try:
        raw_headers = dict(getattr(st.context, "headers", {}) or {})
    except Exception:
        raw_headers = {}

    headers = {str(k).lower(): str(v) for k, v in raw_headers.items()}

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
        email = "local_reviewer@local.dev"
        name = "Local Reviewer"

    return {
        "reviewer_name": name or email,
        "reviewer_email": email,
    }


# ============================================================
# Data loading
# ============================================================

@st.cache_data(ttl=30, show_spinner=False)
def load_queue() -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {QUEUE_TABLE}
        ORDER BY review_sequence
    """

    df = query_df(query)

    if df.empty:
        return df

    df["PublisherKey"] = df["PublisherKey"].astype(str)
    df["review_batch_id"] = df["review_batch_id"].astype(str)

    if "review_sequence" in df.columns:
        df["review_sequence_num"] = pd.to_numeric(df["review_sequence"], errors="coerce")
        df = (
            df.sort_values(["review_sequence_num", "PublisherKey"], na_position="last")
            .reset_index(drop=True)
        )

    return df


@st.cache_data(ttl=30, show_spinner=False)
def load_decisions() -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {DECISIONS_TABLE}
    """

    try:
        df = query_df(query)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    df["PublisherKey"] = df["PublisherKey"].astype(str)
    df["review_batch_id"] = df["review_batch_id"].astype(str)

    return df


def decision_key(df: pd.DataFrame) -> pd.Series:
    return df["review_batch_id"].astype(str) + "||" + df["PublisherKey"].astype(str)


def get_unreviewed_queue(queue_df: pd.DataFrame, decisions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes already-reviewed publishers from the active review queue.
    """
    if queue_df.empty:
        return queue_df

    if decisions_df.empty:
        return queue_df.copy()

    reviewed_keys = set(decision_key(decisions_df))
    q = queue_df.copy()
    q["_decision_key"] = decision_key(q)

    return (
        q[~q["_decision_key"].isin(reviewed_keys)]
        .drop(columns=["_decision_key"])
        .reset_index(drop=True)
    )


# ============================================================
# Decision configuration
# ============================================================

REASON_OPTIONS = {
    "creator": [
        ("Creator / personal brand", "creator_individual_or_creator_brand"),
        ("Social profile evidence", "creator_individual_or_creator_brand"),
        ("UGC / product review creator", "creator_individual_or_creator_brand"),
        ("Creator-commerce / affiliate", "creator_individual_or_creator_brand"),
        ("Other creator evidence", "other"),
    ],
    "not_creator": [
        ("Business / company", "business_or_company"),
        ("Agency / network", "agency_or_network"),
        ("Media / editorial publisher", "publisher_or_media"),
        ("Utility or non-creator site", "utility_or_non_creator"),
        ("Not enough creator evidence", "insufficient_information"),
        ("Other", "other"),
    ],
    "unsure": [
        ("Missing or weak website", "insufficient_information"),
        ("Missing or weak description", "insufficient_information"),
        ("Conflicting signals", "insufficient_information"),
        ("Needs manual escalation", "insufficient_information"),
        ("Other", "other"),
    ],
}

DECISION_DISPLAY = {
    "creator": {
        "title": "Creator",
        "subtitle": "You think this publisher belongs in the Influencer / Content Creator cluster.",
        "reviewed_cluster_label": "belongs",
    },
    "not_creator": {
        "title": "Not creator",
        "subtitle": "You think this publisher does not belong in the Influencer / Content Creator cluster.",
        "reviewed_cluster_label": "does_not_belong",
    },
    "unsure": {
        "title": "Unsure",
        "subtitle": "There is not enough evidence to make a confident decision.",
        "reviewed_cluster_label": "unsure",
    },
}


def infer_review_outcome(decision_type: str, row: pd.Series) -> str:
    current_cluster = parse_bool(row.get("signal_current_cluster", False))

    if decision_type == "creator":
        return "accepted_current_cluster" if current_cluster else "add_to_cluster"

    if decision_type == "not_creator":
        return "remove_from_cluster" if current_cluster else "unclear"

    return "unclear"


def set_pending_decision(decision_type: str) -> None:
    st.session_state.pending_decision = decision_type
    st.session_state.reason_choice = None
    st.session_state.review_comment = ""


def clear_pending_decision() -> None:
    st.session_state.pending_decision = None
    st.session_state.reason_choice = None
    st.session_state.review_comment = ""


# ============================================================
# Card navigation
# ============================================================

def init_card_navigation() -> None:
    if "card_idx" not in st.session_state:
        st.session_state.card_idx = 0


def clamp_card_index(total_cards: int) -> None:
    if total_cards <= 0:
        st.session_state.card_idx = 0
        return

    st.session_state.card_idx = max(
        0,
        min(int(st.session_state.card_idx), total_cards - 1),
    )


def handle_navigation_query_params(total_cards: int) -> None:
    """
    Handles card browsing using query params from the HTML arrow links.
    """
    if total_cards <= 0:
        return

    params = st.query_params

    if "idx" in params:
        try:
            requested_idx = int(params.get("idx", 0))
            st.session_state.card_idx = max(0, min(requested_idx, total_cards - 1))
        except Exception:
            st.session_state.card_idx = 0

        st.query_params.clear()
        st.rerun()


def handle_decision_query_params(current_row: pd.Series) -> None:
    """
    Handles coloured HTML decision links.
    """
    params = st.query_params

    if "decision" not in params:
        return

    decision_type = safe_str(params.get("decision", "")).strip()
    publisher_key_from_url = safe_str(params.get("pk", "")).strip()
    current_publisher_key = safe_str(current_row.get("PublisherKey", "")).strip()

    if (
        decision_type in DECISION_DISPLAY
        and publisher_key_from_url == current_publisher_key
    ):
        set_pending_decision(decision_type)

    st.query_params.clear()
    st.rerun()


# ============================================================
# Decision writing
# ============================================================

def save_decision_to_delta(decision: Dict) -> Tuple[bool, str]:
    """
    One decision per review_batch_id + PublisherKey.
    Same reviewer can update their own decision.
    Other reviewers cannot overwrite it.
    """

    existing_query = f"""
        SELECT
            reviewer_email,
            reviewer_name
        FROM {DECISIONS_TABLE}
        WHERE review_batch_id = ?
          AND PublisherKey = CAST(? AS INT)
        LIMIT 1
    """

    existing = query_df(
        existing_query,
        [
            decision["review_batch_id"],
            int(decision["PublisherKey"]),
        ],
    )

    if not existing.empty:
        existing_email = safe_str(existing.iloc[0].get("reviewer_email", "")).lower().strip()
        current_email = safe_str(decision.get("reviewer_email", "")).lower().strip()

        if existing_email and existing_email != current_email:
            existing_name = safe_str(existing.iloc[0].get("reviewer_name", "")) or existing_email
            return False, f"Already reviewed by {existing_name}. This row is read-only."

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

    reviewed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    params = [
        decision["review_batch_id"],
        int(decision["PublisherKey"]),
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
    ]

    execute_sql(merge_sql, params)
    st.cache_data.clear()

    return True, "Saved"


# ============================================================
# UI styling
# ============================================================

def inject_css() -> None:
    st.markdown(
        """
        <style>
        /* Hide Streamlit default chrome */
        header[data-testid="stHeader"] {
            display: none !important;
            height: 0 !important;
        }

        div[data-testid="stToolbar"] {
            display: none !important;
        }

        div[data-testid="stDecoration"] {
            display: none !important;
        }

        #MainMenu {
            visibility: hidden !important;
        }

        footer {
            visibility: hidden !important;
            height: 0 !important;
        }

        /* Global app background */
        html, body, [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at 18% 4%, rgba(91, 140, 255, 0.20), transparent 26%),
                radial-gradient(circle at 82% 8%, rgba(20, 184, 166, 0.14), transparent 24%),
                linear-gradient(135deg, #07111F 0%, #0A1424 48%, #11142A 100%) !important;
            color: #F8FAFC !important;
        }

        .stApp {
            background:
                radial-gradient(circle at 18% 4%, rgba(91, 140, 255, 0.20), transparent 26%),
                radial-gradient(circle at 82% 8%, rgba(20, 184, 166, 0.14), transparent 24%),
                linear-gradient(135deg, #07111F 0%, #0A1424 48%, #11142A 100%) !important;
            color: #F8FAFC !important;
        }

        section.main > div {
            padding-top: 0rem !important;
        }

        .block-container {
            max-width: 1220px !important;
            padding-top: 0.3rem !important;
            padding-bottom: 0.8rem !important;
            padding-left: 1.5rem !important;
            padding-right: 1.5rem !important;
        }

        div[data-testid="stVerticalBlock"] > div:first-child {
            margin-top: 0 !important;
            padding-top: 0 !important;
        }

        div[data-testid="stMarkdownContainer"] {
            color: #F8FAFC !important;
        }

        h1, h2, h3, h4, h5, h6, p, span, label {
            color: inherit;
        }

        /* Topbar */
        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 18px;
            background:
                radial-gradient(circle at 92% 12%, rgba(20,184,166,0.10), transparent 30%),
                linear-gradient(135deg, rgba(10, 23, 41, 0.96) 0%, rgba(9, 18, 32, 0.98) 100%);
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 22px;
            padding: 16px 20px;
            margin: 0.35rem auto 0.9rem auto;
            max-width: 1040px;
            box-shadow:
                0 18px 46px rgba(0,0,0,0.32),
                inset 0 1px 0 rgba(255,255,255,0.05);
            backdrop-filter: blur(14px);
        }

        .topbar-title {
            font-size: 1.12rem;
            font-weight: 950;
            color: #F8FAFC;
            letter-spacing: -0.025em;
            line-height: 1.15;
        }

        .topbar-subtitle {
            font-size: 0.82rem;
            color: #C7D2FE;
            margin-top: 5px;
            font-weight: 600;
        }

        .reviewer-badge {
            text-align: right;
            min-width: 240px;
        }

        .reviewer-name {
            font-size: 0.9rem;
            font-weight: 900;
            color: #E0F2FE;
            line-height: 1.2;
        }

        .reviewer-email {
            font-size: 0.78rem;
            color: #93C5FD;
            margin-top: 4px;
            font-weight: 600;
        }

        .progress-shell {
            width: 330px;
            height: 7px;
            background: rgba(148, 163, 184, 0.16);
            border-radius: 999px;
            overflow: hidden;
            margin-top: 10px;
            box-shadow: inset 0 1px 2px rgba(0,0,0,0.24);
        }

        .progress-fill {
            height: 7px;
            border-radius: 999px;
            background: linear-gradient(90deg, #60A5FA 0%, #22D3EE 50%, #2DD4BF 100%);
            box-shadow: 0 0 18px rgba(34, 211, 238, 0.32);
        }

        /* Review stage */
        .review-stage {
            display: grid;
            grid-template-columns: 72px minmax(0, 760px) 72px;
            align-items: center;
            justify-content: center;
            gap: 18px;
            margin: 0 auto 0.65rem auto;
        }

        .side-nav {
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .side-arrow {
            width: 58px;
            height: 58px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            text-decoration: none !important;
            font-size: 2.1rem;
            font-weight: 900;
            color: #E0F2FE !important;
            background:
                radial-gradient(circle at 30% 20%, rgba(96,165,250,0.22), transparent 45%),
                rgba(15, 23, 42, 0.82);
            border: 1px solid rgba(147,197,253,0.20);
            box-shadow: 0 14px 38px rgba(0,0,0,0.26);
            transition: all 0.18s ease;
        }

        .side-arrow:hover {
            transform: translateY(-2px) scale(1.03);
            border-color: rgba(96,165,250,0.65);
            box-shadow: 0 18px 46px rgba(37,99,235,0.22);
            color: #FFFFFF !important;
        }

        .side-arrow-disabled {
            opacity: 0.22;
            cursor: not-allowed;
            box-shadow: none;
        }

        /* Flashcard */
        .flashcard-wrap {
            max-width: 760px;
            margin: 0 auto;
        }

        .flashcard {
            position: relative;
            overflow: hidden;
            background:
                radial-gradient(circle at 92% 5%, rgba(20,184,166,0.18), transparent 26%),
                linear-gradient(145deg, rgba(18,31,52,0.96) 0%, rgba(9,18,32,0.98) 100%);
            border: 1px solid rgba(126,167,255,0.20);
            border-radius: 28px;
            padding: 22px 26px 20px 26px;
            box-shadow:
                0 18px 54px rgba(0,0,0,0.34),
                inset 0 1px 0 rgba(255,255,255,0.05);
            min-height: 370px;
        }

        .flashcard-kicker {
            font-size: 0.72rem;
            color: #A8BEDD;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 0.09em;
            margin-bottom: 10px;
        }

        .publisher-title {
            font-size: 1.78rem;
            line-height: 1.08;
            font-weight: 950;
            color: #F8FBFF;
            letter-spacing: -0.035em;
            margin: 8px 0 8px 0;
        }

        .publisher-site {
            margin: 0 0 12px 0;
            color: #C9D6EA;
            font-size: 0.88rem;
        }

        .publisher-site a {
            color: #93C5FD !important;
            font-weight: 800;
            text-decoration: none;
        }

        .publisher-site a:hover {
            color: #BFDBFE !important;
            text-decoration: underline;
        }

        .description-card {
            background: rgba(7, 17, 31, 0.62);
            border: 1px solid rgba(255,255,255,0.07);
            border-radius: 18px;
            padding: 14px 15px;
            color: #F1F6FF;
            font-size: 0.96rem;
            line-height: 1.5;
            margin: 12px 0;
            max-height: 120px;
            overflow-y: auto;
        }

        .chips {
            display: flex;
            flex-wrap: wrap;
            gap: 7px;
            margin: 8px 0;
        }

        .chip {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 850;
            border: 1px solid transparent;
            white-space: nowrap;
        }

        .chip-green {
            background: rgba(16,185,129,0.17);
            color: #BBF7D0;
            border-color: rgba(74,222,128,0.24);
        }

        .chip-blue {
            background: rgba(59,130,246,0.16);
            color: #BFD7FF;
            border-color: rgba(96,165,250,0.26);
        }

        .chip-amber {
            background: rgba(245,158,11,0.17);
            color: #FCD9A6;
            border-color: rgba(251,191,36,0.26);
        }

        .chip-red {
            background: rgba(239,68,68,0.16);
            color: #FECACA;
            border-color: rgba(248,113,113,0.24);
        }

        .chip-slate {
            background: rgba(148,163,184,0.13);
            color: #D7E0EC;
            border-color: rgba(203,213,225,0.13);
        }

        .card-footer-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 8px;
            margin-top: 12px;
        }

        .mini-stat {
            background: rgba(7, 17, 31, 0.52);
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 14px;
            padding: 9px 10px;
        }

        .mini-stat-label {
            color: #93C5FD;
            font-size: 0.67rem;
            font-weight: 850;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            margin-bottom: 4px;
        }

        .mini-stat-value {
            color: #F8FBFF;
            font-size: 0.82rem;
            font-weight: 800;
            line-height: 1.25;
        }

        /* Context expander */
        div[data-testid="stExpander"] {
            max-width: 900px;
            margin: 0.4rem auto 0 auto;
            background: rgba(8,18,32,0.74) !important;
            border: 1px solid rgba(148,163,184,0.14) !important;
            border-radius: 16px !important;
            color: #F8FAFC !important;
        }

        div[data-testid="stExpander"] summary {
            color: #E5E7EB !important;
            font-weight: 850 !important;
        }

        .reviewer-context-panel {
            background: rgba(8, 18, 32, 0.78);
            border: 1px solid rgba(148,163,184,0.14);
            border-radius: 18px;
            padding: 16px 18px;
            color: #EAF2FF;
        }

        .context-row {
            margin-bottom: 14px;
        }

        .context-label {
            font-size: 0.72rem;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #93C5FD;
            margin-bottom: 4px;
        }

        .context-value {
            font-size: 0.94rem;
            line-height: 1.45;
            color: #F8FAFC;
        }

        .context-note {
            margin-top: 12px;
            font-size: 0.82rem;
            color: #CBD5E1;
            line-height: 1.45;
        }

        .soft-warning {
            margin-top: 8px;
            margin-bottom: 12px;
            background: rgba(245,158,11,0.14);
            border: 1px solid rgba(251,191,36,0.25);
            color: #FCD9A6;
            border-radius: 14px;
            padding: 10px 12px;
            font-size: 0.9rem;
            font-weight: 700;
        }

        /* Swipe action buttons */
        .swipe-actions {
            max-width: 1020px;
            margin: 1rem auto 0.35rem auto;
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 16px;
        }

        .swipe-btn {
            min-height: 68px;
            border-radius: 22px;
            padding: 12px 18px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 14px;
            text-decoration: none !important;
            font-weight: 900;
            border: 1px solid transparent;
            box-shadow:
                0 16px 42px rgba(0,0,0,0.26),
                inset 0 1px 0 rgba(255,255,255,0.08);
            transition: all 0.18s ease;
        }

        .swipe-btn strong {
            display: block;
            font-size: 1rem;
            line-height: 1.1;
            color: inherit;
        }

        .swipe-btn small {
            display: block;
            margin-top: 4px;
            font-size: 0.72rem;
            font-weight: 750;
            opacity: 0.86;
            color: inherit;
        }

        .swipe-icon {
            font-size: 1.45rem;
            line-height: 1;
        }

        .swipe-reject {
            background:
                radial-gradient(circle at 20% 20%, rgba(248,113,113,0.28), transparent 42%),
                linear-gradient(135deg, rgba(127,29,29,0.90), rgba(69,10,10,0.90));
            color: #FEE2E2 !important;
            border-color: rgba(248,113,113,0.32);
        }

        .swipe-reject:hover {
            transform: translateY(-2px);
            color: #FFFFFF !important;
            border-color: rgba(252,165,165,0.70);
            box-shadow: 0 20px 48px rgba(220,38,38,0.28);
        }

        .swipe-unsure {
            background:
                radial-gradient(circle at 20% 20%, rgba(251,191,36,0.30), transparent 42%),
                linear-gradient(135deg, rgba(120,53,15,0.92), rgba(69,26,3,0.92));
            color: #FEF3C7 !important;
            border-color: rgba(251,191,36,0.34);
        }

        .swipe-unsure:hover {
            transform: translateY(-2px);
            color: #FFFFFF !important;
            border-color: rgba(253,224,71,0.72);
            box-shadow: 0 20px 48px rgba(245,158,11,0.25);
        }

        .swipe-accept {
            background:
                radial-gradient(circle at 20% 20%, rgba(45,212,191,0.28), transparent 42%),
                linear-gradient(135deg, rgba(6,95,70,0.92), rgba(20,83,45,0.92));
            color: #CCFBF1 !important;
            border-color: rgba(45,212,191,0.34);
        }

        .swipe-accept:hover {
            transform: translateY(-2px);
            color: #FFFFFF !important;
            border-color: rgba(94,234,212,0.72);
            box-shadow: 0 20px 48px rgba(20,184,166,0.25);
        }

        .decision-help {
            text-align: center;
            color: #CBD5E1;
            font-size: 0.82rem;
            margin-top: 8px;
        }

        .decision-panel {
            max-width: 720px;
            margin: 0.7rem auto 0 auto;
            background: rgba(8,18,32,0.78);
            border: 1px solid rgba(148,163,184,0.14);
            border-radius: 20px;
            padding: 18px;
            box-shadow: 0 18px 46px rgba(0,0,0,0.24);
        }

        .decision-title {
            font-size: 1.05rem;
            font-weight: 900;
            color: #F8FAFC;
            margin-bottom: 6px;
        }

        .decision-subtitle {
            color: #CBD5E1;
            font-size: 0.86rem;
            margin-bottom: 10px;
        }

        /* Streamlit widgets */
        .stButton > button {
            background: rgba(15, 23, 42, 0.78) !important;
            color: #F8FAFC !important;
            border: 1px solid rgba(148,163,184,0.20) !important;
            border-radius: 16px !important;
            font-weight: 850 !important;
            min-height: 48px !important;
            box-shadow: 0 10px 28px rgba(0,0,0,0.22);
        }

        .stButton > button:hover {
            border-color: rgba(96, 165, 250, 0.60) !important;
            background: rgba(30, 41, 59, 0.92) !important;
            color: #FFFFFF !important;
            transform: translateY(-1px);
        }

        input, textarea, select {
            background: rgba(15, 23, 42, 0.86) !important;
            color: #F8FAFC !important;
            border-color: rgba(148,163,184,0.20) !important;
        }

        textarea::placeholder,
        input::placeholder {
            color: #94A3B8 !important;
        }

        div[role="radiogroup"] label {
            color: #F8FAFC !important;
        }

        div[data-testid="stAlert"] {
            border-radius: 14px !important;
        }

        @media (max-width: 900px) {
            .topbar {
                flex-direction: column;
                align-items: flex-start;
            }

            .reviewer-badge {
                text-align: left;
                min-width: 0;
            }

            .progress-shell {
                width: 260px;
            }

            .review-stage {
                grid-template-columns: 44px minmax(0, 1fr) 44px;
                gap: 8px;
            }

            .side-arrow {
                width: 42px;
                height: 42px;
                font-size: 1.55rem;
            }

            .publisher-title {
                font-size: 1.45rem;
            }

            .card-footer-grid {
                grid-template-columns: 1fr;
            }

            .swipe-actions {
                grid-template-columns: 1fr;
                gap: 10px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# UI component helpers
# ============================================================

def bucket_chip_class(bucket: str) -> str:
    bucket = safe_str(bucket)

    if bucket in {"p1_current_cluster_strong", "p3_hidden_positive_strong"}:
        return "chip-green"

    if bucket in {"p2_current_cluster", "p4_hidden_positive"}:
        return "chip-blue"

    if bucket in {"p5_adjacent_supported", "p6_social_and_keyword"}:
        return "chip-amber"

    return "chip-slate"


def confidence_chip_class(confidence: str) -> str:
    confidence = safe_str(confidence)

    if confidence == "likely_creator":
        return "chip-green"

    if confidence == "possible_creator":
        return "chip-blue"

    if confidence == "creator_commercial_business_like":
        return "chip-amber"

    if confidence == "likely_not_creator":
        return "chip-red"

    return "chip-slate"


def render_topbar(reviewer: Dict[str, str], reviewed: int, total: int) -> None:
    pct = format_pct(reviewed, total)

    render_html(
        f"""
        <div class="topbar">
            <div>
                <div class="topbar-title">Influencer / Content Creator Review</div>
                <div class="topbar-subtitle">Flashcard feedback loop · {reviewed} / {total} reviewed · {pct}% complete</div>
                <div class="progress-shell">
                    <div class="progress-fill" style="width:{pct}%;"></div>
                </div>
            </div>

            <div class="reviewer-badge">
                <div class="reviewer-name">{esc(reviewer.get("reviewer_name", "Reviewer"))}</div>
                <div class="reviewer-email">{esc(reviewer.get("reviewer_email", ""))}</div>
            </div>
        </div>
        """
    )


def render_flashcard(
    row: pd.Series,
    position: int,
    remaining: int,
    card_idx: int,
    total_cards: int,
) -> None:
    publisher = esc(row.get("Publisher", "Unknown publisher"))
    website = normalise_url(row.get("PublisherWebSite", ""))
    domain = esc(extract_domain(website))
    description = esc(row.get("PublisherDescription", "No description provided.")).replace("\n", "<br>")

    bucket = safe_str(row.get("priority_bucket", ""))
    bucket_label = esc(row.get("priority_bucket_label", bucket or "Priority bucket"))

    confidence_raw = safe_str(row.get("review_confidence_hint", ""))
    confidence_label = esc(row.get("review_confidence_hint_label", confidence_raw or "No hint"))

    website_type = esc(row.get("website_type", "unknown"))

    current_type = esc(row.get("current_publisher_type_group", "")) or "—"
    current_subvertical = esc(row.get("current_publisher_subvertical", "")) or "—"

    creator_score = safe_float_str(row.get("creator_evidence_score", ""))
    risk_score = safe_float_str(row.get("non_creator_risk_score", ""))

    prev_idx = max(card_idx - 1, 0)
    next_idx = min(card_idx + 1, total_cards - 1)

    prev_arrow = (
        f'<a class="side-arrow" href="?idx={prev_idx}" title="Previous publisher">‹</a>'
        if card_idx > 0
        else '<span class="side-arrow side-arrow-disabled">‹</span>'
    )

    next_arrow = (
        f'<a class="side-arrow" href="?idx={next_idx}" title="Next publisher">›</a>'
        if card_idx < total_cards - 1
        else '<span class="side-arrow side-arrow-disabled">›</span>'
    )

    site_html = (
        f'<a href="{esc(website)}" target="_blank">{domain or esc(website)}</a>'
        if website
        else "No website provided"
    )

    render_html(
        f"""
        <div class="review-stage">
            <div class="side-nav">{prev_arrow}</div>

            <div class="flashcard-wrap">
                <div class="flashcard">
                    <div class="flashcard-kicker">Card {card_idx + 1} of {total_cards} unreviewed · {remaining} remaining</div>

                    <div class="chips">
                        <span class="chip {bucket_chip_class(bucket)}">{bucket_label}</span>
                        <span class="chip {confidence_chip_class(confidence_raw)}">{confidence_label}</span>
                        <span class="chip chip-slate">Website: {website_type}</span>
                    </div>

                    <div class="publisher-title">{publisher}</div>
                    <div class="publisher-site">{site_html}</div>

                    <div class="description-card">{description}</div>

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

            <div class="side-nav">{next_arrow}</div>
        </div>
        """
    )


def render_reviewer_context_panel(row: pd.Series) -> None:
    bucket_description = safe_str(
        row.get("priority_bucket_description", "No bucket explanation available.")
    )
    guidance = safe_str(
        row.get("reviewer_guidance_hint", "Reviewer should inspect manually.")
    )
    evidence = safe_str(
        row.get("review_evidence_summary", "No evidence summary available.")
    )
    warning = safe_str(row.get("reviewer_warning_message", ""))

    warning_html = ""
    if warning:
        warning_html = f"""
        <div class="soft-warning">
            {esc(warning)}
        </div>
        """

    render_html(
        f"""
        <div class="reviewer-context-panel">
            <div class="context-row">
                <div class="context-label">Why this publisher is here</div>
                <div class="context-value">{esc(bucket_description)}</div>
            </div>

            <div class="context-row">
                <div class="context-label">Reviewer guidance</div>
                <div class="context-value">{esc(guidance)}</div>
            </div>

            <div class="context-row">
                <div class="context-label">Evidence detected</div>
                <div class="context-value">{esc(evidence)}</div>
            </div>

            {warning_html}

            <div class="context-note">
                These are supporting signals only. The final decision should still be based on reviewer judgement.
            </div>
        </div>
        """
    )


def render_completion_state(reviewer: Dict[str, str], total: int) -> None:
    render_topbar(reviewer, total, total)
    st.success("All publishers in this review batch have been reviewed.")
    st.markdown(
        """
        The first review batch is complete. You can now analyse the captured decisions
        in the Delta decisions table and the joined review results view.
        """
    )


# ============================================================
# Decision UI
# ============================================================

def render_decision_buttons(row: pd.Series) -> None:
    publisher_key = esc(row.get("PublisherKey", ""))

    render_html(
        f"""
        <div class="swipe-actions">
            <a class="swipe-btn swipe-reject" href="?decision=not_creator&pk={publisher_key}">
                <span class="swipe-icon">←</span>
                <span>
                    <strong>Not creator</strong>
                    <small>Remove / exclude</small>
                </span>
            </a>

            <a class="swipe-btn swipe-unsure" href="?decision=unsure&pk={publisher_key}">
                <span class="swipe-icon">?</span>
                <span>
                    <strong>Unsure</strong>
                    <small>Needs judgement</small>
                </span>
            </a>

            <a class="swipe-btn swipe-accept" href="?decision=creator&pk={publisher_key}">
                <span>
                    <strong>Creator</strong>
                    <small>Keep / add</small>
                </span>
                <span class="swipe-icon">→</span>
            </a>
        </div>

        <div class="decision-help">
            Browse freely with the side arrows. Choose a decision only when you are ready.
        </div>
        """
    )


def render_decision_panel(row: pd.Series, reviewer: Dict[str, str]) -> None:
    decision_type = st.session_state.get("pending_decision")

    if not decision_type:
        return

    config = DECISION_DISPLAY[decision_type]
    reason_options = REASON_OPTIONS[decision_type]
    reason_labels = [label for label, _ in reason_options]

    render_html(
        f"""
        <div class="decision-panel">
            <div class="decision-title">{esc(config["title"])}</div>
            <div class="decision-subtitle">{esc(config["subtitle"])}</div>
        </div>
        """
    )

    selected_reason_label = st.radio(
        "Reason",
        reason_labels,
        index=0,
        horizontal=True,
        key=f"reason_radio_{safe_str(row.get('PublisherKey'))}_{decision_type}",
    )

    comment = st.text_area(
        "Optional comment",
        placeholder="Add a note only if this case is ambiguous, misleading, or useful for future model improvement.",
        height=90,
        key=f"comment_{safe_str(row.get('PublisherKey'))}_{decision_type}",
    )

    save_col, cancel_col = st.columns([3, 1])

    with save_col:
        if st.button("Save decision & next", type="primary", use_container_width=True):
            reason_category = dict(reason_options)[selected_reason_label]

            decision = {
                "review_batch_id": safe_str(row.get("review_batch_id", "")),
                "PublisherKey": safe_str(row.get("PublisherKey", "")),
                "reviewed_cluster_label": config["reviewed_cluster_label"],
                "review_outcome": infer_review_outcome(decision_type, row),
                "review_reason_category": reason_category,
                "review_reason_detail": selected_reason_label,
                "review_comment": comment.strip(),
                "reviewer_name": reviewer["reviewer_name"],
                "reviewer_email": reviewer["reviewer_email"],
                "decision_source": DECISION_SOURCE,
                "review_sequence": safe_int(row.get("review_sequence", 0)),
                "priority_bucket": safe_str(row.get("priority_bucket", "")),
                "review_confidence_hint": safe_str(row.get("review_confidence_hint", "")),
                "creator_evidence_score": safe_int(row.get("creator_evidence_score", 0)),
                "non_creator_risk_score": safe_int(row.get("non_creator_risk_score", 0)),
            }

            ok, msg = save_decision_to_delta(decision)

            if ok:
                clear_pending_decision()
                st.success("Saved. Loading next publisher...")
                st.rerun()
            else:
                st.error(msg)

    with cancel_col:
        if st.button("Cancel", use_container_width=True):
            clear_pending_decision()
            st.rerun()


# ============================================================
# Optional debug context
# ============================================================

def render_debug_context(row: pd.Series) -> None:
    if not SHOW_DEBUG_CONTEXT:
        return

    with st.expander("Developer debug context", expanded=False):
        st.json(
            {
                "PublisherKey": safe_str(row.get("PublisherKey", "")),
                "review_batch_id": safe_str(row.get("review_batch_id", "")),
                "review_sequence": safe_str(row.get("review_sequence", "")),
                "priority_bucket": safe_str(row.get("priority_bucket", "")),
                "candidate_reason": safe_str(row.get("candidate_reason", "")),
                "review_confidence_hint": safe_str(row.get("review_confidence_hint", "")),
                "strong_creator_profile_flag": safe_str(row.get("strong_creator_profile_flag", "")),
                "creator_commercial_business_like_flag": safe_str(row.get("creator_commercial_business_like_flag", "")),
                "creator_evidence_score": safe_str(row.get("creator_evidence_score", "")),
                "non_creator_risk_score": safe_str(row.get("non_creator_risk_score", "")),
                "signal_possible_business_entity": safe_str(row.get("signal_possible_business_entity", "")),
                "signal_network_or_agency": safe_str(row.get("signal_network_or_agency", "")),
            }
        )


# ============================================================
# Main app
# ============================================================

def main() -> None:
    inject_css()

    if "pending_decision" not in st.session_state:
        st.session_state.pending_decision = None

    init_card_navigation()

    reviewer = get_authenticated_reviewer()

    with st.spinner("Loading review queue..."):
        queue_df = load_queue()
        decisions_df = load_decisions()

    if queue_df.empty:
        st.error(
            f"No rows found in review queue table `{QUEUE_TABLE}`. "
            "Please check the table exists and the app has SELECT access."
        )
        st.stop()

    total_rows = len(queue_df)
    reviewed_rows = len(decisions_df) if not decisions_df.empty else 0

    unreviewed_df = get_unreviewed_queue(queue_df, decisions_df)
    remaining_rows = len(unreviewed_df)

    if remaining_rows == 0:
        render_completion_state(reviewer, total_rows)
        return

    handle_navigation_query_params(remaining_rows)
    clamp_card_index(remaining_rows)

    current_row = unreviewed_df.iloc[st.session_state.card_idx]

    handle_decision_query_params(current_row)

    render_topbar(reviewer, reviewed_rows, total_rows)

    render_flashcard(
        current_row,
        position=st.session_state.card_idx + 1,
        remaining=remaining_rows,
        card_idx=st.session_state.card_idx,
        total_cards=remaining_rows,
    )

    with st.expander("Why this publisher is in the queue", expanded=False):
        render_reviewer_context_panel(current_row)

    render_debug_context(current_row)

    render_decision_buttons(current_row)
    render_decision_panel(current_row, reviewer)

    with st.expander("Reviewer guide", expanded=False):
        st.markdown(
            """
            **Your task:** decide whether this publisher genuinely belongs in the Influencer / Content Creator cluster.

            **Choose Creator** when the publisher looks creator-led, audience-led, social-led, or driven by a personal/creator brand.

            **Choose Not creator** when it looks mainly like a business, agency, network, editorial/media publisher, utility site, or other non-creator profile.

            **Choose Unsure** when the evidence is not strong enough or the signals conflict.

            You can browse publishers using the side arrows before making a decision.
            """
        )


if __name__ == "__main__":
    main()
