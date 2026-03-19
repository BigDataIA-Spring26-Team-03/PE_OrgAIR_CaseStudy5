# src/services/collection/analyst_notes.py
# Collects and indexes analyst-generated primary source evidence into ChromaDB.

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from src.services.retrieval.hybrid import HybridRetriever

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class NoteType(str, Enum):
    """Types of analyst-generated primary source evidence (CS4 DD sources)."""
    ANALYST_INTERVIEW  = "analyst_interview"    # interview transcripts
    MANAGEMENT_MEETING = "management_meeting"
    SITE_VISIT         = "site_visit"
    DD_FINDING         = "dd_finding"
    DD_DATA_ROOM       = "dd_data_room"         # data room document summaries


class DDSeverity(str, Enum):
    """Severity levels for due diligence findings."""
    CRITICAL = "critical"    # blocks investment or requires immediate remediation
    MODERATE = "moderate"    # addressable gap, needs investment plan
    LOW      = "low"         # minor observation, monitor only


# ---------------------------------------------------------------------------
# AnalystNote dataclass
# ---------------------------------------------------------------------------

@dataclass
class AnalystNote:
    """
    A single analyst-generated evidence item.
    Represents primary source evidence collected post-LOI during due diligence.
    """
    note_id: str
    company_id: str
    note_type: NoteType
    title: str
    content: str

    # Interview metadata (populated for INTERVIEW_TRANSCRIPT notes)
    interviewee: Optional[str] = None
    interviewee_title: Optional[str] = None

    # Assessment context
    dimensions_discussed: List[str] = field(default_factory=list)
    key_findings: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)

    # DD finding context (populated for DD_FINDING notes)
    severity: Optional[DDSeverity] = None

    # Data room context (populated for DATA_ROOM_SUMMARY notes)
    document_name: Optional[str] = None

    # Provenance
    assessor: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    confidence: float = 1.0     # primary source — always high confidence

    def to_chromadb_doc(self) -> Dict[str, Any]:
        """
        Convert this note to the flat dict format expected by
        HybridRetriever.index_documents().

        """
        primary_dim = (
            self.dimensions_discussed[0]
            if self.dimensions_discussed
            else "leadership"
        )

        metadata: Dict[str, Any] = {
            "company_id":       self.company_id,
            "source_type":      self.note_type.value,
            "dimension":        primary_dim,
            "all_dimensions":   ",".join(self.dimensions_discussed),
            "confidence":       self.confidence,
            "assessor":         self.assessor,
            "note_type":        self.note_type.value,
            "created_at":       self.created_at.isoformat(),
        }

        # Add type-specific metadata fields
        if self.interviewee_title:
            metadata["interviewee_title"] = self.interviewee_title
        if self.interviewee:
            metadata["interviewee"] = self.interviewee
        if self.severity:
            metadata["severity"] = self.severity.value
        if self.document_name:
            metadata["document_name"] = self.document_name
        if self.key_findings:
            metadata["key_findings"] = " | ".join(self.key_findings)
        if self.risk_flags:
            metadata["risk_flags"] = " | ".join(self.risk_flags)

        return {
            "doc_id":   self.note_id,
            "content":  self.content,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# AnalystNotesCollector
# ---------------------------------------------------------------------------

class AnalystNotesCollector:
    """
    Structured API for analysts to submit primary source evidence.

    The retriever instance must be shared with the rest of the system —
    passing a fresh HybridRetriever() here would create an empty BM25
    corpus disconnected from the already-indexed CS2 evidence.

    """

    def __init__(self, retriever: HybridRetriever) -> None:
        self.retriever = retriever

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def submit_interview(
        self,
        company_id: str,
        interviewee: str,
        interviewee_title: str,
        transcript: str,
        assessor: str,
        dimensions_discussed: List[str],
        key_findings: Optional[List[str]] = None,
        risk_flags: Optional[List[str]] = None,
    ) -> str:
        #Submit an interview transcript for indexing.

        note_id = _generate_note_id("interview", company_id)

        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type=NoteType.ANALYST_INTERVIEW,
            title=f"Interview: {interviewee_title} — {interviewee}",
            content=f"Interview with {interviewee_title} ({interviewee})\n\n{transcript}",
            interviewee=interviewee,
            interviewee_title=interviewee_title,
            dimensions_discussed=dimensions_discussed,
            key_findings=key_findings or [],
            risk_flags=risk_flags or [],
            assessor=assessor,
        )

        self._index_note(note)
        return note_id

    async def submit_dd_finding(
        self,
        company_id: str,
        title: str,
        finding: str,
        dimension: str,
        severity: str,
        assessor: str,
        key_findings: Optional[List[str]] = None,
        risk_flags: Optional[List[str]] = None,
    ) -> str:
        #Submit a due diligence finding.
        note_id = _generate_note_id("dd", company_id)

        try:
            severity_enum = DDSeverity(severity.lower())
        except ValueError:
            logger.warning(
                "dd_finding_unknown_severity",
                extra={"severity": severity, "defaulting_to": "moderate"},
            )
            severity_enum = DDSeverity.MODERATE

        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type=NoteType.DD_FINDING,
            title=title,
            content=f"{title}\n\n{finding}",
            dimensions_discussed=[dimension],
            severity=severity_enum,
            key_findings=key_findings or [],
            risk_flags=risk_flags or [],
            assessor=assessor,
        )

        self._index_note(note)
        return note_id

    async def submit_data_room_summary(
        self,
        company_id: str,
        document_name: str,
        summary: str,
        dimension: str,
        assessor: str,
        key_findings: Optional[List[str]] = None,
    ) -> str:

        #Submit a data room document summary.

        note_id = _generate_note_id("dataroom", company_id)

        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type=NoteType.DD_DATA_ROOM,
            title=f"Data Room: {document_name}",
            content=f"Data Room Document: {document_name}\n\n{summary}",
            dimensions_discussed=[dimension],
            document_name=document_name,
            key_findings=key_findings or [],
            assessor=assessor,
        )

        self._index_note(note)
        return note_id

    async def submit_management_meeting(
        self,
        company_id: str,
        meeting_title: str,
        notes: str,
        assessor: str,
        dimensions_discussed: List[str],
        key_findings: Optional[List[str]] = None,
        risk_flags: Optional[List[str]] = None,
    ) -> str:
        """
        Submit management meeting notes.

        Returns the generated note_id.
        """
        note_id = _generate_note_id("meeting", company_id)

        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type=NoteType.MANAGEMENT_MEETING,
            title=meeting_title,
            content=f"Management Meeting: {meeting_title}\n\n{notes}",
            dimensions_discussed=dimensions_discussed,
            key_findings=key_findings or [],
            risk_flags=risk_flags or [],
            assessor=assessor,
        )

        self._index_note(note)
        return note_id

    async def submit_site_visit(
        self,
        company_id: str,
        location: str,
        observations: str,
        assessor: str,
        dimensions_discussed: List[str],
        key_findings: Optional[List[str]] = None,
        risk_flags: Optional[List[str]] = None,
    ) -> str:
        """
        Submit site visit observations.

        Returns the generated note_id.
        """
        note_id = _generate_note_id("sitevisit", company_id)

        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type=NoteType.SITE_VISIT,
            title=f"Site Visit: {location}",
            content=f"Site Visit at {location}\n\n{observations}",
            dimensions_discussed=dimensions_discussed,
            key_findings=key_findings or [],
            risk_flags=risk_flags or [],
            assessor=assessor,
        )

        self._index_note(note)
        return note_id

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _index_note(self, note: AnalystNote) -> None:
        """Index a note into ChromaDB via the shared HybridRetriever."""
        doc = note.to_chromadb_doc()
        count = self.retriever.index_documents([doc])
        logger.info(
            "analyst_note_indexed",
            extra={
                "note_id":    note.note_id,
                "note_type":  note.note_type.value,
                "company_id": note.company_id,
                "dimension":  doc["metadata"]["dimension"],
                "count":      count,
            },
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _generate_note_id(prefix: str, company_id: str) -> str:
    """
    Generate a human-readable, sortable note ID.
    Format: {prefix}_{company_id}_{YYYYmmddHHMMSS}
    Example: interview_NVDA_20260310143022
    """
    return f"{prefix}_{company_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"