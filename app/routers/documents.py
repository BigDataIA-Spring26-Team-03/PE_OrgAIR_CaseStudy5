from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.snowflake import SnowflakeService
from app.pipelines.sec_pipeline import SECPipeline

router = APIRouter(prefix="/api/v1/documents", tags=["Documents"])


# -----------------------------
# Utilities
# -----------------------------
def row_get(row: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def normalize_doc_row(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row_get(r, "id", "ID"),
        "company_id": row_get(r, "company_id", "COMPANY_ID"),
        "ticker": row_get(r, "ticker", "TICKER"),
        "filing_type": row_get(r, "filing_type", "FILING_TYPE"),
        "filing_date": row_get(r, "filing_date", "FILING_DATE"),
        "source_url": row_get(r, "source_url", "SOURCE_URL"),
        "local_path": row_get(r, "local_path", "LOCAL_PATH"),
        "s3_key": row_get(r, "s3_key", "S3_KEY"),
        "content_hash": row_get(r, "content_hash", "CONTENT_HASH"),
        "status": row_get(r, "status", "STATUS"),
        "chunk_count": row_get(r, "chunk_count", "CHUNK_COUNT"),
        "error_message": row_get(r, "error_message", "ERROR_MESSAGE"),
        "created_at": row_get(r, "created_at", "CREATED_AT"),
        "processed_at": row_get(r, "processed_at", "PROCESSED_AT"),
    }


# -----------------------------
# Schemas
# -----------------------------
class CollectDocumentsRequest(BaseModel):
    """
    Trigger the unified SEC pipeline for a company.
    Provide either ticker OR company_id.
    Runs: download → parse (edgartools / Mistral OCR) → chunk → store in S3 + Snowflake.
    """
    ticker: Optional[str] = Field(default=None, description="Company ticker (preferred)")
    company_id: Optional[str] = Field(default=None, description="Company UUID (if ticker not provided)")


class CollectDocumentsResponse(BaseModel):
    status: str
    ticker: str
    docs_processed: int
    chunks_created: int
    errors: list[str]


class DocumentListResponse(BaseModel):
    items: list[dict[str, Any]]
    limit: int
    offset: int


class ChunkListResponse(BaseModel):
    items: list[dict[str, Any]]
    limit: int
    offset: int


# -----------------------------
# Endpoints
# -----------------------------

@router.post("/collect/{ticker}", status_code=202, response_model=CollectDocumentsResponse)
def collect_by_ticker(ticker: str) -> CollectDocumentsResponse:
    """
    Run the unified SEC pipeline for a single ticker (path param).
    Downloads 10-K x2, 8-K x2, 10-Q x4 → parses → chunks → stores in S3 + Snowflake.
    """
    result = SECPipeline().run(ticker.upper().strip())
    return CollectDocumentsResponse(
        status="completed",
        ticker=result["ticker"],
        docs_processed=result["docs_processed"],
        chunks_created=result["chunks_created"],
        errors=result["errors"],
    )


@router.post("/collect", response_model=CollectDocumentsResponse)
def collect_documents(payload: CollectDocumentsRequest) -> CollectDocumentsResponse:
    """
    Run the unified SEC pipeline for a ticker or company_id (request body).
    """
    if not payload.ticker and not payload.company_id:
        raise HTTPException(status_code=400, detail="Provide either ticker or company_id")

    ticker = payload.ticker
    if not ticker:
        sf = SnowflakeService()
        rows = sf.execute_query(
            "SELECT ticker FROM companies WHERE id = %(id)s AND is_deleted = FALSE LIMIT 1",
            {"id": payload.company_id},
        )
        if not rows:
            raise HTTPException(status_code=404, detail="company_id not found")
        ticker = str(row_get(rows[0], "ticker", "TICKER")).upper()

    result = SECPipeline().run(ticker.upper().strip())
    return CollectDocumentsResponse(
        status="completed",
        ticker=result["ticker"],
        docs_processed=result["docs_processed"],
        chunks_created=result["chunks_created"],
        errors=result["errors"],
    )


@router.get("", response_model=DocumentListResponse)
def list_documents(
    company_id: Optional[str] = None,
    ticker: Optional[str] = None,
    filing_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> DocumentListResponse:
    sf = SnowflakeService()

    where = ["1=1"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if company_id:
        where.append("company_id = %(company_id)s")
        params["company_id"] = company_id
    if ticker:
        where.append("UPPER(ticker) = %(ticker)s")
        params["ticker"] = ticker.upper()
    if filing_type:
        where.append("filing_type = %(filing_type)s")
        params["filing_type"] = filing_type
    if status:
        where.append("status = %(status)s")
        params["status"] = status

    rows = sf.execute_query(
        f"""
        SELECT
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, s3_key, content_hash,
          status, chunk_count, error_message, created_at, processed_at
        FROM documents_sec
        WHERE {" AND ".join(where)}
        ORDER BY created_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        params,
    )

    return DocumentListResponse(
        items=[normalize_doc_row(r) for r in rows],
        limit=limit,
        offset=offset,
    )


@router.get("/{doc_id}")
def get_document(doc_id: str) -> dict[str, Any]:
    sf = SnowflakeService()
    rows = sf.execute_query(
        """
        SELECT
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, s3_key, content_hash,
          status, chunk_count, error_message, created_at, processed_at
        FROM documents_sec
        WHERE id = %(id)s
        LIMIT 1
        """,
        {"id": doc_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Document not found")
    return normalize_doc_row(rows[0])


@router.get("/{doc_id}/chunks", response_model=ChunkListResponse)
def get_document_chunks(
    doc_id: str,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> ChunkListResponse:
    sf = SnowflakeService()

    rows = sf.execute_query(
        """
        SELECT
          id, document_id, chunk_index, content,
          section, start_char, end_char, word_count
        FROM document_chunks_sec
        WHERE document_id = %(doc_id)s
        ORDER BY chunk_index
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {"doc_id": doc_id, "limit": limit, "offset": offset},
    )

    def norm_chunk(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row_get(r, "id", "ID"),
            "document_id": row_get(r, "document_id", "DOCUMENT_ID"),
            "chunk_index": row_get(r, "chunk_index", "CHUNK_INDEX"),
            "content": row_get(r, "content", "CONTENT"),
            "section": row_get(r, "section", "SECTION"),
            "start_char": row_get(r, "start_char", "START_CHAR"),
            "end_char": row_get(r, "end_char", "END_CHAR"),
            "word_count": row_get(r, "word_count", "WORD_COUNT"),
        }

    return ChunkListResponse(items=[norm_chunk(r) for r in rows], limit=limit, offset=offset)