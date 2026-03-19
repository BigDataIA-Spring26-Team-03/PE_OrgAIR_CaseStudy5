from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class DocumentChunk(BaseModel):
    # matches document_chunks table columns
    id: str
    document_id: str

    chunk_index: int = Field(..., ge=0)
    section: Optional[str] = Field(default=None, max_length=100)

    content: str
    content_hash: str = Field(..., max_length=64)

    token_count: Optional[int] = None

    created_at: Optional[datetime] = None