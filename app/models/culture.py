# app/models/culture_score.py

from uuid import UUID
from datetime import datetime
from typing import Optional, List
from decimal import Decimal

from pydantic import BaseModel, Field, ConfigDict


# ============================================================================
# INDIVIDUAL REVIEW MODEL
# ============================================================================

class GlassdoorReview(BaseModel):
    """A single Glassdoor review."""
    company_id: UUID
    review_id: str
    rating: float = Field(..., ge=1, le=5)
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str] = None
    is_current_employee: bool
    job_title: str
    review_date: datetime
    
    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# CULTURE SIGNAL MODELS (for endpoints/UI)
# ============================================================================

class CultureSignalBase(BaseModel):
    """Base culture signal data."""
    company_id: UUID
    ticker: str
    
    # Component scores (0-100)
    innovation_score: Decimal = Field(..., ge=0, le=100)
    data_driven_score: Decimal = Field(..., ge=0, le=100)
    change_readiness_score: Decimal = Field(..., ge=0, le=100)
    ai_awareness_score: Decimal = Field(..., ge=0, le=100)
    
    # Aggregate
    overall_score: Decimal = Field(..., ge=0, le=100)
    
    # Metadata
    review_count: int = Field(..., ge=0)
    avg_rating: Decimal = Field(..., ge=0, le=5)
    current_employee_ratio: Decimal = Field(..., ge=0, le=1)
    confidence: Decimal = Field(..., ge=0, le=1)
    
    # Evidence
    positive_keywords_found: List[str] = Field(default_factory=list)
    negative_keywords_found: List[str] = Field(default_factory=list)


class CultureSignalCreate(CultureSignalBase):
    """
    POST /api/culture-signals
    Create a new culture signal from scraper data.
    """
    pass


class CultureSignalUpdate(BaseModel):
    """
    PATCH /api/culture-signals/{id}
    Update specific fields (if needed for corrections).
    """
    innovation_score: Optional[Decimal] = Field(None, ge=0, le=100)
    data_driven_score: Optional[Decimal] = Field(None, ge=0, le=100)
    change_readiness_score: Optional[Decimal] = Field(None, ge=0, le=100)
    ai_awareness_score: Optional[Decimal] = Field(None, ge=0, le=100)
    overall_score: Optional[Decimal] = Field(None, ge=0, le=100)
    confidence: Optional[Decimal] = Field(None, ge=0, le=1)
    positive_keywords_found: Optional[List[str]] = None
    negative_keywords_found: Optional[List[str]] = None


class CultureSignalResponse(CultureSignalBase):
    """
    GET /api/culture-signals/{id}
    Full culture signal with metadata for detail view.
    """
    id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    # Include company details for UI
    company_name: Optional[str] = None
    sector: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)


class CultureSignalSummary(BaseModel):
    """
    GET /api/culture-signals (list)
    Lightweight model for table/list views in UI.
    """
    id: UUID
    company_id: UUID
    company_name: Optional[str] = None
    ticker: str
    overall_score: Decimal
    avg_rating: Decimal
    review_count: int
    confidence: Decimal
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# LIST RESPONSE (for paginated endpoints)
# ============================================================================

class CultureSignalListResponse(BaseModel):
    """
    GET /api/culture-signals?page=1&limit=10
    Paginated list response for UI tables.
    """
    items: List[CultureSignalSummary]
    total: int
    page: int
    limit: int
    pages: int