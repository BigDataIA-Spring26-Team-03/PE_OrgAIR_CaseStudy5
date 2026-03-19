# app/routers/evidence.py
"""
CS2-compatible Evidence API endpoints.

Serves evidence from all Snowflake tables (SEC chunks, external signals,
Glassdoor reviews, board governance signals) as CS2Evidence-compatible JSON.

Tracks indexing state in Redis (SET cs4_indexed_ids) to avoid altering
existing Snowflake schemas.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.snowflake import SnowflakeService
from app.core.deps import cache

logger = structlog.get_logger()
router = APIRouter(prefix="/evidence", tags=["evidence"])

REDIS_INDEXED_KEY = "cs4_indexed_ids"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXTERNAL_SIGNAL_SOURCE_MAP = {
    "technology_hiring": "job_posting_linkedin",
    "innovation_activity": "patent_uspto",
    "digital_presence": "sec_10k_item_1",
    "leadership_signals": "board_proxy_def14a",
}


def _get_indexed_ids() -> set:
    """Return set of evidence IDs already indexed in CS4 (from Redis)."""
    try:
        members = cache.client.smembers(REDIS_INDEXED_KEY)
        return set(members) if members else set()
    except Exception:
        return set()


def _apply_indexed_filter(rows: List[dict], indexed: Optional[bool]) -> List[dict]:
    """Filter rows by their indexing state using the Redis SET."""
    if indexed is None:
        return rows
    indexed_ids = _get_indexed_ids()
    if indexed is False:
        return [r for r in rows if r["evidence_id"] not in indexed_ids]
    else:  # indexed is True
        return [r for r in rows if r["evidence_id"] in indexed_ids]


# ---------------------------------------------------------------------------
# GET /api/v1/evidence
# ---------------------------------------------------------------------------

@router.get("")
def get_evidence(
    company_id: str = Query(..., description="Company ticker, e.g. NVDA"),
    indexed: Optional[bool] = Query(None, description="True=only indexed, False=only unindexed, omit=all"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    since: Optional[datetime] = Query(None, description="Only evidence extracted/created after this datetime"),
) -> List[dict]:
    """
    Fetch CS2-compatible evidence for a company from all Snowflake sources.

    Sources:
    - document_chunks_sec (SEC 10-K filings)
    - external_signals (job postings, patents, digital presence, leadership)
    - glassdoor_reviews + culture_signals (culture/talent)
    - board_governance_signals (AI governance / leadership)
    """
    db = SnowflakeService()
    ticker = company_id.upper()
    rows: List[dict] = []

    # ------------------------------------------------------------------
    # 1. SEC document chunks
    # ------------------------------------------------------------------
    sec_query = """
        SELECT
            c.id                    AS evidence_id,
            d.ticker                AS company_id,
            'sec_10k_item_1'        AS source_type,
            'digital_presence'      AS signal_category,
            c.content               AS content,
            0.85                    AS confidence,
            YEAR(d.filing_date)     AS fiscal_year,
            d.source_url            AS source_url,
            d.created_at            AS created_at
        FROM document_chunks_sec c
        JOIN documents_sec d ON c.document_id = d.id
        WHERE d.ticker = %(ticker)s
          AND 0.85 >= %(min_confidence)s
    """
    if since:
        sec_query += " AND d.created_at >= %(since)s"
    try:
        sec_rows = db.execute_query(sec_query, {"ticker": ticker, "min_confidence": min_confidence, "since": since})
        rows.extend(sec_rows)
    except Exception as exc:
        logger.warning("evidence_sec_query_failed", ticker=ticker, error=str(exc))

    # ------------------------------------------------------------------
    # 2. External signals
    # ------------------------------------------------------------------
    ext_query = """
    SELECT
        es.id                   AS evidence_id,
        c.ticker                AS company_id,
        CASE es.category
            WHEN 'technology_hiring'  THEN 'job_posting_linkedin'
            WHEN 'innovation_activity' THEN 'patent_uspto'
            WHEN 'digital_presence'   THEN 'sec_10k_item_1'
            WHEN 'leadership_signals' THEN 'board_proxy_def14a'
            ELSE 'sec_10k_item_1'
        END                     AS source_type,
        es.category             AS signal_category,
        es.raw_value            AS content,
        es.confidence           AS confidence,
        YEAR(es.signal_date)    AS fiscal_year,
        NULL                    AS source_url,
        es.created_at           AS created_at
    FROM external_signals es
    JOIN companies c ON es.company_id = c.id
    WHERE c.ticker = %(ticker)s
      AND es.confidence >= %(min_confidence)s
      AND c.is_deleted = FALSE
    """
    if since:
        ext_query += " AND created_at >= %(since)s"
    try:
        ext_rows = db.execute_query(ext_query, {"ticker": ticker, "min_confidence": min_confidence, "since": since})
        rows.extend(ext_rows)
    except Exception as exc:
        logger.warning("evidence_external_signals_query_failed", ticker=ticker, error=str(exc))

    # ------------------------------------------------------------------
    # 3. Glassdoor reviews (via culture_signals for confidence + ticker)
    # ------------------------------------------------------------------
    glassdoor_query = """
        SELECT
            gr.id                   AS evidence_id,
            cs.ticker               AS company_id,
            'glassdoor_review'      AS source_type,
            'culture_signals'       AS signal_category,
            CONCAT(
                COALESCE(gr.title, ''), ': ',
                COALESCE(gr.pros, ''), ' / ',
                COALESCE(gr.cons, '')
            )                       AS content,
            cs.confidence           AS confidence,
            YEAR(gr.review_date)    AS fiscal_year,
            NULL                    AS source_url,
            gr.created_at           AS created_at
        FROM glassdoor_reviews gr
        JOIN culture_signals cs ON gr.culture_signal_id = cs.id
        WHERE cs.ticker = %(ticker)s
          AND cs.confidence >= %(min_confidence)s
    """
    if since:
        glassdoor_query += " AND gr.created_at >= %(since)s"
    try:
        gd_rows = db.execute_query(glassdoor_query, {"ticker": ticker, "min_confidence": min_confidence, "since": since})
        rows.extend(gd_rows)
    except Exception as exc:
        logger.warning("evidence_glassdoor_query_failed", ticker=ticker, error=str(exc))

    # ------------------------------------------------------------------
    # 4. Board governance signals
    # ------------------------------------------------------------------
    board_query = """
        SELECT
            id                      AS evidence_id,
            ticker                  AS company_id,
            'board_proxy_def14a'    AS source_type,
            'governance_signals'    AS signal_category,
            evidence                AS content,
            confidence              AS confidence,
            NULL                    AS fiscal_year,
            NULL                    AS source_url,
            created_at              AS created_at
        FROM board_governance_signals
        WHERE ticker = %(ticker)s
          AND confidence >= %(min_confidence)s
          AND evidence IS NOT NULL
    """
    if since:
        board_query += " AND created_at >= %(since)s"
    try:
        board_rows = db.execute_query(board_query, {"ticker": ticker, "min_confidence": min_confidence, "since": since})
        rows.extend(board_rows)
    except Exception as exc:
        logger.warning("evidence_board_query_failed", ticker=ticker, error=str(exc))

    # ------------------------------------------------------------------
    # Apply indexed filter via Redis
    # ------------------------------------------------------------------
    rows = _apply_indexed_filter(rows, indexed)

    logger.info("evidence_fetched", ticker=ticker, count=len(rows), indexed_filter=indexed)
    return rows


# ---------------------------------------------------------------------------
# POST /api/v1/evidence/mark-indexed
# ---------------------------------------------------------------------------

class MarkIndexedRequest(BaseModel):
    evidence_ids: List[str]


@router.post("/mark-indexed")
def mark_indexed(body: MarkIndexedRequest) -> dict:
    """
    Mark evidence items as indexed in CS4.
    Stores IDs in Redis SET cs4_indexed_ids.
    Returns {"updated_count": N}.
    """
    if not body.evidence_ids:
        return {"updated_count": 0}

    try:
        cache.client.sadd(REDIS_INDEXED_KEY, *body.evidence_ids)
    except Exception as exc:
        logger.error("mark_indexed_redis_failed", error=str(exc))
        raise HTTPException(status_code=500, detail="Failed to persist indexing state") from exc

    logger.info("evidence_marked_indexed", count=len(body.evidence_ids))
    return {"updated_count": len(body.evidence_ids)}
