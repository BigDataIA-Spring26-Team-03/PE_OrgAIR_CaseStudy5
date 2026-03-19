# app/models/board.py

from uuid import UUID
from datetime import datetime
from typing import Optional, List
from decimal import Decimal

from pydantic import BaseModel, Field, ConfigDict


class BoardGovernanceSignalResponse(BaseModel):
    """Full board governance signal for detail view."""
    id: UUID
    company_id: UUID
    ticker: str
    governance_score: Decimal = Field(..., ge=0, le=100)
    has_tech_committee: bool = False
    has_ai_expertise: bool = False
    has_data_officer: bool = False
    has_independent_majority: bool = False
    has_risk_tech_oversight: bool = False
    has_ai_strategy: bool = False
    ai_experts: List[str] = Field(default_factory=list)
    evidence: List[str] = Field(default_factory=list)
    confidence: Decimal = Field(..., ge=0, le=1)
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class BoardGovernanceSignalSummary(BaseModel):
    """Lightweight model for list views."""
    id: UUID
    company_id: UUID
    ticker: str
    governance_score: Decimal
    confidence: Decimal
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class BoardMemberResponse(BaseModel):
    """Individual board member detail."""
    id: UUID
    company_id: UUID
    governance_signal_id: UUID
    name: str
    title: Optional[str] = None
    committees: List[str] = Field(default_factory=list)
    bio: Optional[str] = None
    is_independent: bool = False
    tenure_years: Decimal = Decimal("0")
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class BoardGovernanceListResponse(BaseModel):
    """Paginated list response."""
    items: List[BoardGovernanceSignalSummary]
    total: int
    page: int
    limit: int
    pages: int
