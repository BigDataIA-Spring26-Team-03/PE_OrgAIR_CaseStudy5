# app/routers/assessments.py
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.core.deps import cache
from app.models.assessment import AssessmentCreate, AssessmentResponse
from app.models.dimension import AssessmentStatus
from app.services.snowflake import db

router = APIRouter(prefix="/assessments", tags=["Assessments"])

ASSESSMENT_CACHE_PREFIX = "assessment:"
ASSESSMENT_TTL_SECONDS = 120  # 2 minutes


class StatusUpdate(BaseModel):
    status: AssessmentStatus = Field(...)


@router.post("", response_model=AssessmentResponse, status_code=status.HTTP_201_CREATED)
def create_assessment(payload: AssessmentCreate) -> AssessmentResponse:
    try:
        company = db.get_company(str(payload.company_id))
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        assessment_id = db.create_assessment(payload.model_dump(mode="json"))
        assessment = db.get_assessment(assessment_id)
        if not assessment:
            raise HTTPException(status_code=500, detail="Failed to create assessment")

        response = AssessmentResponse(**assessment)

        # warm cache
        cache_key = f"{ASSESSMENT_CACHE_PREFIX}{response.id}"
        cache.set(cache_key, response, ttl_seconds=ASSESSMENT_TTL_SECONDS)

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[AssessmentResponse])
def list_assessments(
    company_id: Optional[UUID] = Query(None, description="Filter by company"),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> List[AssessmentResponse]:
    try:
        items = db.list_assessments(
            limit=limit,
            offset=offset,
            company_id=str(company_id) if company_id else None,
        )
        return [AssessmentResponse(**item) for item in items]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{assessment_id}", response_model=AssessmentResponse)
def get_assessment(assessment_id: UUID) -> AssessmentResponse:
    cache_key = f"{ASSESSMENT_CACHE_PREFIX}{assessment_id}"

    try:
        cached = cache.get(cache_key, AssessmentResponse)
        if cached:
            return cached

        assessment = db.get_assessment(str(assessment_id))
        if not assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")

        response = AssessmentResponse(**assessment)
        cache.set(cache_key, response, ttl_seconds=ASSESSMENT_TTL_SECONDS)
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{assessment_id}/status", response_model=AssessmentResponse)
def update_assessment_status(assessment_id: UUID, payload: StatusUpdate) -> AssessmentResponse:
    try:
        existing = db.get_assessment(str(assessment_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Assessment not found")

        success = db.update_assessment_status(str(assessment_id), payload.status.value)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update status")

        updated = db.get_assessment(str(assessment_id))
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to fetch updated assessment")

        response = AssessmentResponse(**updated)

        # invalidate + refresh cache
        cache_key = f"{ASSESSMENT_CACHE_PREFIX}{assessment_id}"
        cache.delete(cache_key)
        cache.set(cache_key, response, ttl_seconds=ASSESSMENT_TTL_SECONDS)

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
