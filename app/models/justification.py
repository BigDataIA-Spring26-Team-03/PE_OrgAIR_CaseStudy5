# app/models/justification.py

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class CitedEvidenceResponse(BaseModel):
    """A single cited evidence item for IC presentation."""

    evidence_id: str
    content: str
    source_type: str
    source_url: Optional[str] = None
    confidence: float
    matched_keywords: List[str]
    relevance_score: float

    model_config = ConfigDict(from_attributes=True)


class ScoreJustificationResponse(BaseModel):
    """
    IC-ready score justification for one dimension.

    """

    company_id: str
    dimension: str
    score: float
    level: int
    level_name: str
    confidence_interval: List[float]   # [lower_95, upper_95]

    rubric_criteria: str
    rubric_keywords: List[str]

    supporting_evidence: List[CitedEvidenceResponse]
    gaps_identified: List[str]

    generated_summary: str
    evidence_strength: str             # "strong", "moderate", or "weak"

    model_config = ConfigDict(from_attributes=True)


class ICPrepRequest(BaseModel):
    """Optional request body for IC meeting prep — restrict to specific dimensions."""

    focus_dimensions: Optional[List[str]] = None  # e.g. ["talent", "data_infrastructure"]


class ICMeetingPackageResponse(BaseModel):
    """
    Complete IC meeting evidence package covering all (or selected) dimensions.

    """

    # Company info
    company_id: str
    company_ticker: str
    company_name: str

    # Assessment summary 
    org_air_score: float
    vr_score: float
    hr_score: float

    # IC findings
    executive_summary: str
    key_strengths: List[str]
    key_gaps: List[str]
    risk_factors: List[str]
    recommendation: str        # PROCEED / PROCEED WITH CAUTION / FURTHER DILIGENCE

    # Metadata
    generated_at: str
    total_evidence_count: int
    avg_evidence_strength: str  # "strong", "moderate", or "weak"

    # Full per-dimension justifications
    dimension_justifications: Dict[str, ScoreJustificationResponse]

    model_config = ConfigDict(from_attributes=True)
