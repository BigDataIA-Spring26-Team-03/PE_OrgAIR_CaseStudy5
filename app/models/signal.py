# app/models/signal.py

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SignalCategory(str, Enum):
    """
    Must match the values stored in the external_signals.category column
    in Snowflake.
    """
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


class SignalSource(str, Enum):
    external = "external"
    internal = "internal"


class ExternalSignal(BaseModel):
    """External signal model - matches external_signals table"""
    id: str
    company_id: str
    category: SignalCategory
    source: SignalSource = SignalSource.external
    signal_date: datetime
    score: int = Field(..., ge=0, le=100)
    title: Optional[str] = Field(default=None, max_length=300)
    url: Optional[str] = Field(default=None, max_length=500)
    metadata_json: Optional[str] = None
    created_at: Optional[datetime] = None


class CompanySignalSummary(BaseModel):
    """Company signal summary - matches company_signal_summaries table"""
    company_id: str
    jobs_score: int = Field(..., ge=0, le=100)
    tech_score: int = Field(..., ge=0, le=100)
    patents_score: int = Field(..., ge=0, le=100)
    leadership_score: int = Field(default=0, ge=0, le=100)
    composite_score: int = Field(..., ge=0, le=100)
    last_updated_at: Optional[datetime] = None


class ExternalSignalResponse(BaseModel):
    """For API responses"""
    pass