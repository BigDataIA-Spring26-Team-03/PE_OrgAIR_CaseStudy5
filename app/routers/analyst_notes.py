# app/routers/analyst_notes.py

from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.services.collection.analyst_notes import (
    AnalystNotesCollector,
    DDSeverity,
    NoteType,
)
from app.core.deps import retriever
from src.services.integration.cs3_client import Dimension

router = APIRouter(prefix="/analyst-notes", tags=["Analyst Notes"])

VALID_DIMENSIONS = [d.value for d in Dimension]


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_collector: Optional[AnalystNotesCollector] = None


def get_collector() -> AnalystNotesCollector:
    global _collector
    if _collector is None:
        _collector = AnalystNotesCollector(retriever=retriever)
    return _collector


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class InterviewRequest(BaseModel):
    interviewee: str = Field(..., description="Full name of person interviewed e.g. Jensen Huang")
    interviewee_title: str = Field(..., description="Job title e.g. CTO, CDO, CFO")
    transcript: str = Field(..., min_length=50, description="Full interview transcript or notes")
    assessor: str = Field(..., description="Analyst email or name who conducted the interview")
    dimensions_discussed: List[str] = Field(
        ...,
        description=f"CS3 dimensions discussed. Valid values: {VALID_DIMENSIONS}"
    )
    key_findings: Optional[List[str]] = Field(None, description="Key findings from the interview")
    risk_flags: Optional[List[str]] = Field(None, description="Risk flags identified")


class DDFindingRequest(BaseModel):
    title: str = Field(..., description="Short title for the finding e.g. 'No data quality monitoring'")
    finding: str = Field(..., min_length=20, description="Full description of the finding")
    dimension: str = Field(..., description=f"CS3 dimension this finding relates to. Valid: {VALID_DIMENSIONS}")
    severity: str = Field(..., description="critical | moderate | low")
    assessor: str = Field(..., description="Analyst who identified this finding")
    key_findings: Optional[List[str]] = Field(None, description="Structured key findings")
    risk_flags: Optional[List[str]] = Field(None, description="Risk flags")


class DataRoomRequest(BaseModel):
    document_name: str = Field(..., description="Name of the data room document")
    summary: str = Field(..., min_length=50, description="Summary of the document contents")
    dimension: str = Field(..., description=f"Primary CS3 dimension this document covers. Valid: {VALID_DIMENSIONS}")
    assessor: str = Field(..., description="Analyst who reviewed this document")
    key_findings: Optional[List[str]] = Field(None, description="Key findings from the document")


class ManagementMeetingRequest(BaseModel):
    meeting_title: str = Field(..., description="Title/description of the meeting")
    notes: str = Field(..., min_length=50, description="Full meeting notes")
    assessor: str = Field(..., description="Analyst who attended")
    dimensions_discussed: List[str] = Field(
        ...,
        description=f"CS3 dimensions discussed. Valid: {VALID_DIMENSIONS}"
    )
    key_findings: Optional[List[str]] = Field(None)
    risk_flags: Optional[List[str]] = Field(None)


class SiteVisitRequest(BaseModel):
    location: str = Field(..., description="Location visited e.g. 'NVIDIA HQ, Santa Clara'")
    observations: str = Field(..., min_length=50, description="Detailed observations from the visit")
    assessor: str = Field(..., description="Analyst who conducted the visit")
    dimensions_discussed: List[str] = Field(
        ...,
        description=f"CS3 dimensions observed. Valid: {VALID_DIMENSIONS}"
    )
    key_findings: Optional[List[str]] = Field(None)
    risk_flags: Optional[List[str]] = Field(None)


# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------

class NoteSubmittedResponse(BaseModel):
    note_id: str
    note_type: str
    company_id: str
    status: str = "indexed"
    primary_dimension: str
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_dimension(dimension: str) -> str:
    if dimension not in VALID_DIMENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid dimension '{dimension}'. Valid values: {VALID_DIMENSIONS}",
        )
    return dimension


def _validate_dimensions(dimensions: List[str]) -> List[str]:
    for d in dimensions:
        _validate_dimension(d)
    return dimensions


def _validate_severity(severity: str) -> str:
    valid = [s.value for s in DDSeverity]
    if severity.lower() not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid severity '{severity}'. Valid values: {valid}",
        )
    return severity.lower()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/{ticker}/interview", response_model=NoteSubmittedResponse)
async def submit_interview(
    ticker: str,
    payload: InterviewRequest,
) -> NoteSubmittedResponse:
    """
    Submit an interview transcript for indexing.

    Indexes into ChromaDB with confidence=1.0 (primary source).
    The note surfaces in justifications for all dimensions_discussed.

    Example: POST /api/v1/analyst-notes/NVDA/interview
    """
    _validate_dimensions(payload.dimensions_discussed)
    try:
        note_id = await get_collector().submit_interview(
            company_id=ticker.upper(),
            interviewee=payload.interviewee,
            interviewee_title=payload.interviewee_title,
            transcript=payload.transcript,
            assessor=payload.assessor,
            dimensions_discussed=payload.dimensions_discussed,
            key_findings=payload.key_findings,
            risk_flags=payload.risk_flags,
        )
        return NoteSubmittedResponse(
            note_id=note_id,
            note_type=NoteType.ANALYST_INTERVIEW.value,
            company_id=ticker.upper(),
            primary_dimension=payload.dimensions_discussed[0],
            message=f"Interview with {payload.interviewee_title} indexed successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{ticker}/dd-finding", response_model=NoteSubmittedResponse)
async def submit_dd_finding(
    ticker: str,
    payload: DDFindingRequest,
) -> NoteSubmittedResponse:
    """
    Submit a due diligence finding.

    Severity is stored in metadata — critical findings flag key risks
    in IC reports. Indexes with confidence=1.0.

    Example: POST /api/v1/analyst-notes/NVDA/dd-finding
    """
    _validate_dimension(payload.dimension)
    severity = _validate_severity(payload.severity)
    try:
        note_id = await get_collector().submit_dd_finding(
            company_id=ticker.upper(),
            title=payload.title,
            finding=payload.finding,
            dimension=payload.dimension,
            severity=severity,
            assessor=payload.assessor,
            key_findings=payload.key_findings,
            risk_flags=payload.risk_flags,
        )
        return NoteSubmittedResponse(
            note_id=note_id,
            note_type=NoteType.DD_FINDING.value,
            company_id=ticker.upper(),
            primary_dimension=payload.dimension,
            message=f"DD finding '{payload.title}' indexed with severity={severity}",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{ticker}/data-room", response_model=NoteSubmittedResponse)
async def submit_data_room(
    ticker: str,
    payload: DataRoomRequest,
) -> NoteSubmittedResponse:
    """
    Submit a data room document summary.

    Document name is stored in metadata for citation trail.
    Indexes with confidence=1.0.

    Example: POST /api/v1/analyst-notes/NVDA/data-room
    """
    _validate_dimension(payload.dimension)
    try:
        note_id = await get_collector().submit_data_room_summary(
            company_id=ticker.upper(),
            document_name=payload.document_name,
            summary=payload.summary,
            dimension=payload.dimension,
            assessor=payload.assessor,
            key_findings=payload.key_findings,
        )
        return NoteSubmittedResponse(
            note_id=note_id,
            note_type=NoteType.DD_DATA_ROOM.value,
            company_id=ticker.upper(),
            primary_dimension=payload.dimension,
            message=f"Data room document '{payload.document_name}' indexed successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{ticker}/management-meeting", response_model=NoteSubmittedResponse)
async def submit_management_meeting(
    ticker: str,
    payload: ManagementMeetingRequest,
) -> NoteSubmittedResponse:
    """
    Submit management meeting notes.

    Example: POST /api/v1/analyst-notes/NVDA/management-meeting
    """
    _validate_dimensions(payload.dimensions_discussed)
    try:
        note_id = await get_collector().submit_management_meeting(
            company_id=ticker.upper(),
            meeting_title=payload.meeting_title,
            notes=payload.notes,
            assessor=payload.assessor,
            dimensions_discussed=payload.dimensions_discussed,
            key_findings=payload.key_findings,
            risk_flags=payload.risk_flags,
        )
        return NoteSubmittedResponse(
            note_id=note_id,
            note_type=NoteType.MANAGEMENT_MEETING.value,
            company_id=ticker.upper(),
            primary_dimension=payload.dimensions_discussed[0],
            message=f"Management meeting '{payload.meeting_title}' indexed successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{ticker}/site-visit", response_model=NoteSubmittedResponse)
async def submit_site_visit(
    ticker: str,
    payload: SiteVisitRequest,
) -> NoteSubmittedResponse:
    """
    Submit site visit observations.

    Example: POST /api/v1/analyst-notes/NVDA/site-visit
    """
    _validate_dimensions(payload.dimensions_discussed)
    try:
        note_id = await get_collector().submit_site_visit(
            company_id=ticker.upper(),
            location=payload.location,
            observations=payload.observations,
            assessor=payload.assessor,
            dimensions_discussed=payload.dimensions_discussed,
            key_findings=payload.key_findings,
            risk_flags=payload.risk_flags,
        )
        return NoteSubmittedResponse(
            note_id=note_id,
            note_type=NoteType.SITE_VISIT.value,
            company_id=ticker.upper(),
            primary_dimension=payload.dimensions_discussed[0],
            message=f"Site visit at '{payload.location}' indexed successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))