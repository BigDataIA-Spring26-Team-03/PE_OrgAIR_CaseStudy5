# app/routers/portfolios.py
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from app.services.snowflake import SnowflakeService

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])


@router.get("/{fund_id}/companies")
def get_portfolio_companies(
    fund_id: str,
    fallback_all: bool = Query(
        default=False,
        description="If true and portfolio is empty, return all companies (dev convenience).",
    ),
):
    """Return all companies belonging to a fund."""
    db = SnowflakeService()
    rows = db.execute_query(
        """
        SELECT c.id, c.name, c.ticker, c.industry_id,
               i.name AS industry,
               i.sector AS sector,
               c.position_factor, c.is_deleted,
               c.created_at, c.updated_at,
               COALESCE(c.industry_id,
                   (SELECT id FROM industries
                    WHERE name = 'Business Services' LIMIT 1)
               ) AS industry_id
        FROM companies c
        LEFT JOIN industries i ON c.industry_id = i.id
        JOIN portfolio_companies pc ON c.id = pc.company_id
        WHERE pc.fund_id = %(fund_id)s
          AND c.is_deleted = FALSE
        ORDER BY c.ticker
        """,
        {"fund_id": fund_id},
    )
    if rows or not fallback_all:
        return rows or []

    return db.execute_query(
        """
        SELECT c.id, c.name, c.ticker, c.industry_id,
               i.name AS industry,
               i.sector AS sector,
               c.position_factor, c.is_deleted, c.created_at, c.updated_at
        FROM companies c
        LEFT JOIN industries i ON c.industry_id = i.id
        WHERE is_deleted = FALSE
        ORDER BY ticker
        LIMIT 100
        """,
        {},
    ) or []


@router.post("/{fund_id}/companies")
def set_portfolio_companies(
    fund_id: str,
    payload: dict = Body(...),
):
    """
    Replace the portfolio membership for a fund.

    Accepts either:
    - {"tickers": ["NVDA", "GOOGL", ...]}
    - {"company_ids": ["uuid", ...]}
    """
    tickers: Optional[List[str]] = payload.get("tickers")
    company_ids: Optional[List[str]] = payload.get("company_ids")

    if tickers == [] or company_ids == []:
        # explicit clear request
        tickers = None
        company_ids = None
        resolved_ids = []
    elif (not tickers) and (not company_ids):
        raise HTTPException(status_code=400, detail="Provide 'tickers' or 'company_ids'.")

    db = SnowflakeService()

    resolved_ids: List[str] = []
    if company_ids:
        resolved_ids = [str(x) for x in company_ids if str(x).strip()]
    elif tickers:
        tups = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
        if not tups:
            raise HTTPException(status_code=400, detail="Empty 'tickers' list.")

        placeholders = ",".join(["%s"] * len(tups))
        rows = db.execute_query(
            f"""
            SELECT id, ticker
            FROM companies
            WHERE UPPER(ticker) IN ({placeholders})
              AND is_deleted = FALSE
            """,
            tups,
        )

        found = {str(r.get("ticker") or "").upper(): str(r.get("id") or "") for r in rows}
        missing = [t for t in tups if t not in found]
        if missing:
            raise HTTPException(status_code=400, detail=f"Unknown tickers: {', '.join(missing)}")
        resolved_ids = [found[t] for t in tups]

    # Replace membership (best-effort transactional in Snowflake: delete then insert)
    db.execute_update(
        "DELETE FROM portfolio_companies WHERE fund_id = %(fund_id)s",
        {"fund_id": fund_id},
    )

    inserted = 0
    for cid in resolved_ids:
        inserted += db.execute_update(
            """
            INSERT INTO portfolio_companies (fund_id, company_id)
            VALUES (%(fund_id)s, %(company_id)s)
            """,
            {"fund_id": fund_id, "company_id": cid},
        )

    return {"fund_id": fund_id, "company_count": len(resolved_ids), "inserted": inserted}
