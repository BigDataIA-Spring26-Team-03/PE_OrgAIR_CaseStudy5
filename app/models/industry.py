# app/models/industry.py
from uuid import UUID
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field, ConfigDict


class IndustryBase(BaseModel):
    """
    Matches INDUSTRIES table in Snowflake.
    Snowflake column is h_r_base.
    """
    name: str = Field(..., min_length=2, max_length=100)
    sector: Optional[str] = Field(default=None, max_length=100)
    hr_baseline: Optional[float] = Field(default=None, ge=0, le=100, alias="h_r_base")

    model_config = ConfigDict(populate_by_name=True)


class IndustryResponse(IndustryBase):
    id: UUID
    created_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class IndustryListResponse(BaseModel):
    items: List[IndustryResponse]
