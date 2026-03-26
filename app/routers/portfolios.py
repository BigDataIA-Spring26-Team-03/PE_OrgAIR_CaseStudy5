# app/routers/portfolios.py
from fastapi import APIRouter
from app.services.snowflake import SnowflakeService

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])


@router.get("/{fund_id}/companies")
def get_portfolio_companies(fund_id: str):
    """Return all companies belonging to a fund."""
    db = SnowflakeService()
    rows = db.execute_query(
        """
        SELECT c.id, c.name, c.ticker, c.industry_id,
               c.position_factor, c.is_deleted,
               c.created_at, c.updated_at,
               COALESCE(c.industry_id,
                   (SELECT id FROM industries
                    WHERE name = 'Business Services' LIMIT 1)
               ) AS industry_id
        FROM companies c
        JOIN portfolio_companies pc ON c.id = pc.company_id
        WHERE pc.fund_id = %(fund_id)s
          AND c.is_deleted = FALSE
        ORDER BY c.ticker
        """,
        {"fund_id": fund_id},
    )
    return rows or []
