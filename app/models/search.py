# app/models/search.py

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict


class SearchResultResponse(BaseModel):
    """Response model for a single hybrid-search result."""

    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float               # final RRF score
    retrieval_method: str      # "dense", "sparse", or "hybrid"

    model_config = ConfigDict(from_attributes=True)
