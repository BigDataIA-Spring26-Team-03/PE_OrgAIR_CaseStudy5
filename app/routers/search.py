# app/routers/search.py

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.deps import retriever
from app.models.search import SearchResultResponse
from src.services.integration.cs3_client import Dimension

logger = logging.getLogger(__name__)

VALID_DIMENSIONS = {d.value for d in Dimension}
router = APIRouter(prefix="/search", tags=["Search"])


# ---------------------------------------------------------------------------
# GET /api/v1/search  — hybrid evidence search
# ---------------------------------------------------------------------------

@router.get("", response_model=List[SearchResultResponse])
def search_evidence(
    query: str = Query(..., min_length=1, description="Natural language search query"),
    company_id: Optional[str] = Query(None, description="Filter by company ticker e.g. NVDA"),
    dimension: Optional[str] = Query(
        None,
        description=(
            "Filter by CS3 dimension. One of: data_infrastructure, ai_governance, "
            "technology_stack, talent, leadership, use_case_portfolio, culture"
        ),
    ),
    source_types: Optional[List[str]] = Query(
        None, description="Filter by evidence source types e.g. sec_10k_item_1"
    ),
    top_k: int = Query(10, ge=1, le=50, description="Number of results to return"),
    min_confidence: float = Query(
        0.0, ge=0.0, le=1.0, description="Minimum evidence confidence (0.0–1.0)"
    ),
) -> List[SearchResultResponse]:
    """
    Hybrid evidence search combining dense ChromaDB vector search and BM25
    keyword search, merged via Reciprocal Rank Fusion (RRF).
    Results are sorted by combined RRF score (highest first).
    """
    if dimension and dimension not in VALID_DIMENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid dimension '{dimension}'. Valid: {sorted(VALID_DIMENSIONS)}"
        )
    try:
        results = retriever.search(
            query=query,
            top_k=top_k,
            company_id=company_id,
            dimension=dimension,
            source_types=source_types,
            min_confidence=min_confidence,
        )
        # Deduplicate by content fingerprint — keep highest-scoring result per unique content
        seen: set = set()
        deduped = []
        for r in results:
            h = hash(r.content[:200])
            if h not in seen:
                seen.add(h)
                deduped.append(r)
        results = deduped
        return [
            SearchResultResponse(
                doc_id=r.doc_id,
                content=r.content,
                metadata=r.metadata,
                score=r.score,
                retrieval_method=r.retrieval_method,
            )
            for r in results
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# POST /api/v1/search/seed/{ticker}  — fetch from Snowflake → index
# ---------------------------------------------------------------------------

@router.post("/seed/{ticker}")
def seed_evidence(ticker: str) -> dict:
    """
    Fetch all evidence for a ticker from Snowflake and index it into the
    shared HybridRetriever (ChromaDB + BM25).

    Call this once per ticker before searching. Safe to call again — ChromaDB
    upserts by doc_id so duplicates are not created.
    """
    from app.services.snowflake import SnowflakeService

    t = ticker.upper()
    db = SnowflakeService()
    docs: list = []

    # 1. SEC document chunks
    try:
        rows = db.execute_query(
            """
            SELECT c.id AS doc_id, c.content,
                   d.ticker AS company_id, d.source_url,
                   YEAR(d.filing_date) AS fiscal_year
            FROM document_chunks_sec c
            JOIN documents_sec d ON c.document_id = d.id
            WHERE d.ticker = %(ticker)s
            """,
            {"ticker": t},
        )
        for r in rows:
            if r.get("content"):
                docs.append({
                    "doc_id": str(r["doc_id"]),
                    "content": r["content"],
                    "metadata": {
                        "company_id": t,
                        "source_type": "sec_10k_item_1",
                        "signal_category": "digital_presence",
                        "dimension": "data_infrastructure",
                        "confidence": 0.85,
                        "fiscal_year": r.get("fiscal_year"),
                        "source_url": r.get("source_url", ""),
                    },
                })
    except Exception as exc:
        logger.warning("seed_sec_failed ticker=%s error=%s", t, exc)

    # 2. External signals (job postings, patents, digital presence, leadership)
    _SIGNAL_DIM = {
        "technology_hiring": ("job_posting_linkedin", "talent"),
        "innovation_activity": ("patent_uspto", "technology_stack"),
        "digital_presence": ("sec_10k_item_1", "data_infrastructure"),
        "leadership_signals": ("board_proxy_def14a", "leadership"),
    }
    try:
        rows = db.execute_query(
            """
            SELECT es.id AS doc_id, es.raw_value AS content,
                   es.category AS signal_category, es.confidence,
                   YEAR(es.signal_date) AS fiscal_year
            FROM external_signals es
            JOIN companies c ON es.company_id = c.id
            WHERE c.ticker = %(ticker)s AND c.is_deleted = FALSE
            """,
            {"ticker": t},
        )
        for r in rows:
            if r.get("content"):
                cat = r.get("signal_category", "digital_presence")
                src_type, dim = _SIGNAL_DIM.get(cat, ("sec_10k_item_1", "data_infrastructure"))
                docs.append({
                    "doc_id": str(r["doc_id"]),
                    "content": str(r["content"]),
                    "metadata": {
                        "company_id": t,
                        "source_type": src_type,
                        "signal_category": cat,
                        "dimension": dim,
                        "confidence": float(r.get("confidence", 0.7)),
                        "fiscal_year": r.get("fiscal_year"),
                        "source_url": "",
                    },
                })
    except Exception as exc:
        logger.warning("seed_signals_failed ticker=%s error=%s", t, exc)

    # 3. Glassdoor reviews
    try:
        rows = db.execute_query(
            """
            SELECT gr.id AS doc_id,
                   CONCAT(COALESCE(gr.title,''), ': ',
                          COALESCE(gr.pros,''), ' / ',
                          COALESCE(gr.cons,'')) AS content,
                   cs.confidence,
                   YEAR(gr.review_date) AS fiscal_year
            FROM glassdoor_reviews gr
            JOIN culture_signals cs ON gr.culture_signal_id = cs.id
            WHERE cs.ticker = %(ticker)s
            """,
            {"ticker": t},
        )
        for r in rows:
            if r.get("content"):
                docs.append({
                    "doc_id": str(r["doc_id"]),
                    "content": r["content"],
                    "metadata": {
                        "company_id": t,
                        "source_type": "glassdoor_review",
                        "signal_category": "culture_signals",
                        "dimension": "culture",
                        "confidence": float(r.get("confidence", 0.7)),
                        "fiscal_year": r.get("fiscal_year"),
                        "source_url": "",
                    },
                })
    except Exception as exc:
        logger.warning("seed_glassdoor_failed ticker=%s error=%s", t, exc)

    # 4. Board governance signals
    try:
        rows = db.execute_query(
            """
            SELECT id AS doc_id, evidence AS content, confidence
            FROM board_governance_signals
            WHERE ticker = %(ticker)s AND evidence IS NOT NULL
            """,
            {"ticker": t},
        )
        for r in rows:
            if r.get("content"):
                docs.append({
                    "doc_id": str(r["doc_id"]),
                    "content": r["content"],
                    "metadata": {
                        "company_id": t,
                        "source_type": "board_proxy_def14a",
                        "signal_category": "governance_signals",
                        "dimension": "ai_governance",
                        "confidence": float(r.get("confidence", 0.8)),
                        "fiscal_year": None,
                        "source_url": "",
                    },
                })
    except Exception as exc:
        logger.warning("seed_board_failed ticker=%s error=%s", t, exc)

    if not docs:
        logger.warning("seed_no_docs ticker=%s", t)
        return {"ticker": t, "indexed": 0, "message": f"No evidence found in Snowflake for {t}"}

    count = retriever.index_documents(docs)
    logger.info("seed_complete ticker=%s indexed=%d", t, count)
    return {"ticker": t, "indexed": count, "message": f"Indexed {count} documents for {t}"}
