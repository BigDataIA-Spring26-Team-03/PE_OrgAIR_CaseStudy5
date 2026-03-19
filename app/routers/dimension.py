# app/routers/dimension.py
from typing import List
from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from app.models.dimension import DimensionScoreCreate, DimensionScoreResponse
from app.services.snowflake import db

# Nested under assessments
router = APIRouter(prefix="/assessments", tags=["Dimension Scores"])

@router.post(
    "/{assessment_id}/scores",
    response_model=DimensionScoreResponse,
    status_code=status.HTTP_201_CREATED,
)
def add_dimension_score(assessment_id: UUID, payload: DimensionScoreCreate) -> DimensionScoreResponse:
    try:
        assessment = db.get_assessment(str(assessment_id))
        if not assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")

        if str(payload.assessment_id) != str(assessment_id):
            raise HTTPException(status_code=400, detail="Assessment ID mismatch")

        score_id = db.create_dimension_score(payload.model_dump(mode="json"))
        score_data = db.get_dimension_score(score_id)
        if not score_data:
            raise HTTPException(status_code=500, detail="Failed to create dimension score")

        return DimensionScoreResponse(**score_data)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{assessment_id}/scores",
    response_model=List[DimensionScoreResponse],
)
def get_dimension_scores(assessment_id: UUID) -> List[DimensionScoreResponse]:
    try:
        assessment = db.get_assessment(str(assessment_id))
        if not assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")

        scores = db.get_dimension_scores(str(assessment_id))
        return [DimensionScoreResponse(**s) for s in scores]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{assessment_id}/scores/{dimension}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_dimension_score(assessment_id: UUID, dimension: str) -> None:
    """
    Deletes a dimension score by (assessment_id, dimension).
    """
    try:
        # verify assessment exists
        assessment = db.get_assessment(str(assessment_id))
        if not assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")

        success = db.delete_dimension_score_by_assessment_and_dimension(
            str(assessment_id), dimension
        )
        if not success:
            raise HTTPException(status_code=404, detail="Dimension score not found")

        return None

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Flat score routes (not nested under assessments)
scores_router = APIRouter(prefix="/scores", tags=["Dimension Scores"])

@scores_router.put("/{score_id}", response_model=DimensionScoreResponse)
def update_dimension_score(score_id: UUID, payload: DimensionScoreCreate) -> DimensionScoreResponse:
    try:
        existing = db.get_dimension_score(str(score_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Dimension score not found")

        update_data = payload.model_dump(mode="json", exclude={"assessment_id"})

        # Convert enum to string if needed
        if "dimension" in update_data and hasattr(update_data["dimension"], "value"):
            update_data["dimension"] = update_data["dimension"].value

        success = db.update_dimension_score(str(score_id), update_data)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update score")

        updated = db.get_dimension_score(str(score_id))
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to fetch updated score")

        return DimensionScoreResponse(**updated)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
