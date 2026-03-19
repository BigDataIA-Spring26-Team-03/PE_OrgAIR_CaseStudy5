# app/models/dimension.py
from enum import Enum
from uuid import UUID
from datetime import datetime, timezone
from typing import Optional, Dict

from pydantic import BaseModel, Field, model_validator, ConfigDict


class AssessmentType(str, Enum):
    SCREENING = "screening"          # Quick external assessment
    DUE_DILIGENCE = "due_diligence"  # Deep dive with internal access
    QUARTERLY = "quarterly"          # Regular portfolio monitoring
    EXIT_PREP = "exit_prep"          # Pre-exit assessment


class AssessmentStatus(str, Enum):
    DRAFT = "draft"
    IN_PROGRESS = "in_progress"
    SUBMITTED = "submitted"
    APPROVED = "approved"
    SUPERSEDED = "superseded"


class Dimension(str, Enum):
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT_SKILLS = "talent_skills"
    LEADERSHIP_VISION = "leadership_vision"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE_CHANGE = "culture_change"


# Default weights per dimension
DIMENSION_WEIGHTS: Dict[Dimension, float] = {
    Dimension.DATA_INFRASTRUCTURE: 0.25,
    Dimension.AI_GOVERNANCE: 0.20,
    Dimension.TECHNOLOGY_STACK: 0.15,
    Dimension.TALENT_SKILLS: 0.15,
    Dimension.LEADERSHIP_VISION: 0.10,
    Dimension.USE_CASE_PORTFOLIO: 0.10,
    Dimension.CULTURE_CHANGE: 0.05,
}


class DimensionScoreBase(BaseModel):
    assessment_id: UUID
    dimension: Dimension
    score: float = Field(..., ge=0, le=100)
    weight: Optional[float] = Field(default=None, ge=0, le=1)
    confidence: float = Field(default=0.8, ge=0, le=1)
    evidence_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def set_default_weight(self) -> "DimensionScoreBase":
        if self.weight is None:
            self.weight = DIMENSION_WEIGHTS.get(self.dimension, 0.1)
        return self


class DimensionScoreCreate(DimensionScoreBase):
    pass


class DimensionScoreResponse(DimensionScoreBase):
    id: UUID
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(from_attributes=True)
