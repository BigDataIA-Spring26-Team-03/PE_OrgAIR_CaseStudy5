from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class DocumentStatus(str, Enum):
    """Document processing status lifecycle."""
    pending = "pending"
    downloaded = "downloaded"
    parsed = "parsed"
    chunked = "chunked"
    indexed = "indexed"
    failed = "failed"


class DocumentRecord(BaseModel):
    """
    Represents a SEC filing document.
    
    Matches documents_sec table schema.
    Tracks document through processing pipeline: 
    pending → downloaded → parsed → chunked → indexed
    """
    
    # Core identity
    id: str = Field(default_factory=lambda: str(uuid4()))
    company_id: str
    ticker: str = Field(..., max_length=10)
    filing_type: str = Field(..., max_length=20)
    filing_date: date

    # Source tracking
    source_url: Optional[str] = Field(default=None, max_length=500)
    local_path: Optional[str] = Field(default=None, max_length=500)
    s3_key: Optional[str] = Field(default=None, max_length=500)

    # Content metadata
    content_hash: Optional[str] = Field(default=None, max_length=64)
    word_count: Optional[int] = None
    chunk_count: Optional[int] = None

    # Status tracking
    status: DocumentStatus = DocumentStatus.pending
    error_message: Optional[str] = Field(default=None, max_length=1000)

    # Timestamps
    created_at: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    processed_at: Optional[datetime] = None

    # Validators
    @field_validator('ticker')
    @classmethod
    def ticker_must_be_uppercase(cls, v: str) -> str:
        """Ensure ticker is uppercase."""
        return v.upper()
    
    @field_validator('filing_type')
    @classmethod
    def validate_filing_type(cls, v: str) -> str:
        """Ensure filing type is valid."""
        valid_types = ['10-K', '10-Q', '8-K', 'DEF 14A']
        if v not in valid_types:
            raise ValueError(f'Invalid filing type. Must be one of {valid_types}')
        return v

    # Helper methods
    def mark_downloaded(self, local_path: str) -> None:
        """Mark document as downloaded."""
        self.status = DocumentStatus.downloaded
        self.local_path = local_path

    def mark_parsed(self, content_hash: str, word_count: int) -> None:
        """Mark document as parsed."""
        self.status = DocumentStatus.parsed
        self.content_hash = content_hash
        self.word_count = word_count

    def mark_chunked(self, chunk_count: int) -> None:
        """Mark document as chunked."""
        self.status = DocumentStatus.chunked
        self.chunk_count = chunk_count

    def mark_indexed(self) -> None:
        """Mark document as indexed."""
        self.status = DocumentStatus.indexed
        self.processed_at = datetime.now(timezone.utc)

    def mark_failed(self, error: str) -> None:
        """Mark document as failed."""
        self.status = DocumentStatus.failed
        self.error_message = error
        self.processed_at = datetime.now(timezone.utc)

    def is_processed(self) -> bool:
        """Check if document is fully processed."""
        return self.status == DocumentStatus.indexed

    def processing_duration(self) -> Optional[float]:
        """Calculate processing time in seconds."""
        if self.created_at and self.processed_at:
            return (self.processed_at - self.created_at).total_seconds()
        return None

    @classmethod
    def create_new(
        cls,
        company_id: str,
        ticker: str,
        filing_type: str,
        filing_date: date,
        source_url: str
    ) -> 'DocumentRecord':
        """Create a new document record with defaults."""
        return cls(
            company_id=company_id,
            ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            source_url=source_url
        )

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "company_id": "comp-123",
                "ticker": "CAT",
                "filing_type": "10-K",
                "filing_date": "2024-02-15",
                "source_url": "https://sec.gov/...",
                "status": "pending"
            }
        }
    }