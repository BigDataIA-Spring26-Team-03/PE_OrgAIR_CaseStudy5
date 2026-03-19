# app/models/assessment.py
from uuid import UUID
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, model_validator, ConfigDict

from app.models.dimension import AssessmentType, AssessmentStatus


class AssessmentBase(BaseModel):
    company_id: UUID
    assessment_type: AssessmentType
    assessment_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    primary_assessor: Optional[str] = None
    secondary_assessor: Optional[str] = None


class AssessmentCreate(AssessmentBase):
    pass


class AssessmentResponse(AssessmentBase):
    id: UUID
    status: AssessmentStatus = AssessmentStatus.DRAFT
    vr_score: Optional[float] = Field(None, ge=0, le=100)
    confidence_lower: Optional[float] = Field(None, ge=0, le=100)
    confidence_upper: Optional[float] = Field(None, ge=0, le=100)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_confidence_interval(self) -> "AssessmentResponse":
        if (
            self.confidence_upper is not None
            and self.confidence_lower is not None
            and self.confidence_upper < self.confidence_lower
        ):
            raise ValueError("confidence_upper must be >= confidence_lower")
        return self

    model_config = ConfigDict(from_attributes=True)