# app/routers/companies.py
from typing import List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status

from app.core.deps import cache
from app.models.company import CompanyCreate, CompanyResponse
from app.services.snowflake import db
from app.models.industry import IndustryListResponse, IndustryResponse

router = APIRouter(prefix="/companies", tags=["Companies"])

INDUSTRIES_CACHE_KEY = "industries:list"
COMPANY_CACHE_PREFIX = "company:"
COMPANY_TTL_SECONDS = 300       # 5 minutes
INDUSTRIES_TTL_SECONDS = 3600   # 1 hour


# ========================================
# GET /api/v1/companies/available-industries
# ========================================
@router.get("/available-industries", response_model=IndustryListResponse)
def list_industries():
    """
    List all available industries for company creation.
    Cached for 1 hour.
    """
    try:
        cached = cache.get(INDUSTRIES_CACHE_KEY, IndustryListResponse)
        if cached:
            return cached

        rows = db.list_industries()

        industries = IndustryListResponse(
            items=[IndustryResponse(**row) for row in rows]
        )

        cache.set(INDUSTRIES_CACHE_KEY, industries, ttl_seconds=INDUSTRIES_TTL_SECONDS)
        return industries

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# POST /api/v1/companies (CREATE)
# ========================================
@router.post("", response_model=CompanyResponse, status_code=status.HTTP_201_CREATED)
def create_company(payload: CompanyCreate) -> CompanyResponse:
    """Create a new company in Snowflake; warm cache for GET-by-id."""
    try:
        # Duplicate ticker check — prevent two companies with same ticker
        existing = db.execute_query(
            "SELECT id FROM companies WHERE UPPER(ticker) = %(ticker)s AND is_deleted = FALSE LIMIT 1",
            {"ticker": payload.ticker.upper()},
        )
        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"Company with ticker '{payload.ticker.upper()}' already exists.",
            )

        industry = db.get_industry(str(payload.industry_id))
        if not industry:
            raise HTTPException(status_code=404, detail="Industry not found")

        company_id = db.create_company(payload.model_dump())
        company_data = db.get_company(company_id)
        if not company_data:
            raise HTTPException(status_code=500, detail="Failed to create company")

        response = CompanyResponse(**company_data)

        cache_key = f"{COMPANY_CACHE_PREFIX}{response.id}"
        cache.set(cache_key, response, ttl_seconds=COMPANY_TTL_SECONDS)

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# GET /api/v1/companies (LIST)
# ========================================
@router.get("", response_model=List[CompanyResponse])
def list_companies(
    limit: int = Query(10, ge=1, le=500, description="Max 500 for portfolio bulk fetch"),
    offset: int = Query(0, ge=0),
) -> List[CompanyResponse]:
    """List companies with pagination from Snowflake."""
    try:
        companies = db.list_companies(limit=limit, offset=offset)
        return [CompanyResponse(**company) for company in companies]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# GET /api/v1/companies/{id} (READ ONE)
# ========================================
@router.get("/{company_id}", response_model=CompanyResponse)
def get_company(company_id: UUID) -> CompanyResponse:
    """Get company by ID (cached 5 minutes)."""
    cache_key = f"{COMPANY_CACHE_PREFIX}{company_id}"
    try:
        cached = cache.get(cache_key, CompanyResponse)
        if cached:
            return cached

        company = db.get_company(str(company_id))
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        response = CompanyResponse(**company)
        cache.set(cache_key, response, ttl_seconds=COMPANY_TTL_SECONDS)
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# PUT /api/v1/companies/{id} (UPDATE)
# ========================================
@router.put("/{company_id}", response_model=CompanyResponse)
def update_company(company_id: UUID, payload: CompanyCreate) -> CompanyResponse:
    """Update company and refresh cache."""
    try:
        existing = db.get_company(str(company_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Company not found")

        # If ticker is being changed, check the new ticker isn't already taken
        existing_ticker = (existing.get("TICKER") or existing.get("ticker", "")).upper()
        if payload.ticker.upper() != existing_ticker:
            ticker_conflict = db.execute_query(
                "SELECT id FROM companies WHERE UPPER(ticker) = %(ticker)s AND is_deleted = FALSE AND id != %(id)s LIMIT 1",
                {"ticker": payload.ticker.upper(), "id": str(company_id)},
            )
            if ticker_conflict:
                raise HTTPException(
                    status_code=409,
                    detail=f"Company with ticker '{payload.ticker.upper()}' already exists.",
                )

        industry = db.get_industry(str(payload.industry_id))
        if not industry:
            raise HTTPException(status_code=404, detail="Industry not found")

        success = db.update_company(str(company_id), payload.model_dump())
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update company")

        updated = db.get_company(str(company_id))
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to fetch updated company")

        response = CompanyResponse(**updated)

        cache_key = f"{COMPANY_CACHE_PREFIX}{company_id}"
        cache.delete(cache_key)
        cache.set(cache_key, response, ttl_seconds=COMPANY_TTL_SECONDS)

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# DELETE /api/v1/companies/{id} (DELETE)
# ========================================
@router.delete("/{company_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_company(company_id: UUID) -> None:
    """Soft delete company and invalidate cache."""
    try:
        success = db.delete_company(str(company_id))
        if not success:
            raise HTTPException(status_code=404, detail="Company not found")

        cache_key = f"{COMPANY_CACHE_PREFIX}{company_id}"
        cache.delete(cache_key)

        return None

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))