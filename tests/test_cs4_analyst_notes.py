# tests/test_cs4_analyst_notes.py
# Unit tests for AnalystNotesCollector and supporting types.
# All tests are pure — no I/O, no ChromaDB, no LLM calls.

from unittest.mock import MagicMock, patch

import pytest

from src.services.collection.analyst_notes import (
    AnalystNote,
    AnalystNotesCollector,
    DDSeverity,
    NoteType,
)


# ---------------------------------------------------------------------------
# NoteType enum — sanity checks after rename
# ---------------------------------------------------------------------------

def test_note_type_analyst_interview_value():
    assert NoteType.ANALYST_INTERVIEW.value == "analyst_interview"


def test_note_type_dd_data_room_value():
    assert NoteType.DD_DATA_ROOM.value == "dd_data_room"


def test_note_type_dd_finding_value():
    assert NoteType.DD_FINDING.value == "dd_finding"


def test_note_type_management_meeting_value():
    assert NoteType.MANAGEMENT_MEETING.value == "management_meeting"


def test_note_type_site_visit_value():
    assert NoteType.SITE_VISIT.value == "site_visit"


# ---------------------------------------------------------------------------
# DDSeverity enum
# ---------------------------------------------------------------------------

def test_dd_severity_values():
    assert DDSeverity.CRITICAL.value == "critical"
    assert DDSeverity.MODERATE.value == "moderate"
    assert DDSeverity.LOW.value == "low"


# ---------------------------------------------------------------------------
# AnalystNote.to_chromadb_doc() — structure checks
# ---------------------------------------------------------------------------

def _make_interview_note() -> AnalystNote:
    return AnalystNote(
        note_id="interview_NVDA_20260310143022",
        company_id="NVDA",
        note_type=NoteType.ANALYST_INTERVIEW,
        title="Interview: CTO — Jensen Huang",
        content="Interview content here.",
        interviewee="Jensen Huang",
        interviewee_title="CTO",
        dimensions_discussed=["talent", "technology_stack"],
        key_findings=["Strong ML hiring pipeline"],
        assessor="analyst@firm.com",
    )


def test_to_chromadb_doc_has_required_keys():
    doc = _make_interview_note().to_chromadb_doc()
    assert "doc_id" in doc
    assert "content" in doc
    assert "metadata" in doc


def test_to_chromadb_doc_source_type_is_analyst_interview():
    doc = _make_interview_note().to_chromadb_doc()
    assert doc["metadata"]["source_type"] == "analyst_interview"


def test_to_chromadb_doc_confidence_is_one():
    doc = _make_interview_note().to_chromadb_doc()
    assert doc["metadata"]["confidence"] == 1.0


def test_to_chromadb_doc_id_matches_note_id():
    note = _make_interview_note()
    doc = note.to_chromadb_doc()
    assert doc["doc_id"] == note.note_id


def test_to_chromadb_doc_all_dimensions_joined():
    doc = _make_interview_note().to_chromadb_doc()
    assert doc["metadata"]["all_dimensions"] == "talent,technology_stack"


def test_to_chromadb_doc_primary_dimension_first():
    doc = _make_interview_note().to_chromadb_doc()
    assert doc["metadata"]["dimension"] == "talent"


def test_to_chromadb_doc_data_room_source_type():
    note = AnalystNote(
        note_id="dataroom_NVDA_123",
        company_id="NVDA",
        note_type=NoteType.DD_DATA_ROOM,
        title="Data Room: AI Roadmap.pdf",
        content="Summary of AI roadmap document.",
        dimensions_discussed=["data_infrastructure"],
        assessor="analyst@firm.com",
    )
    doc = note.to_chromadb_doc()
    assert doc["metadata"]["source_type"] == "dd_data_room"


def test_to_chromadb_doc_no_dimensions_defaults_to_leadership():
    note = AnalystNote(
        note_id="dd_NVDA_123",
        company_id="NVDA",
        note_type=NoteType.DD_FINDING,
        title="Gap: No data governance",
        content="Finding content.",
        dimensions_discussed=[],
        assessor="analyst@firm.com",
    )
    doc = note.to_chromadb_doc()
    assert doc["metadata"]["dimension"] == "leadership"


# ---------------------------------------------------------------------------
# AnalystNotesCollector — unit tests with mock HybridRetriever
# ---------------------------------------------------------------------------

def _make_collector():
    mock_retriever = MagicMock()
    mock_retriever.index_documents.return_value = 1
    collector = AnalystNotesCollector(retriever=mock_retriever)
    return collector, mock_retriever


@pytest.mark.asyncio
async def test_submit_interview_returns_note_id():
    collector, _ = _make_collector()
    note_id = await collector.submit_interview(
        company_id="NVDA",
        interviewee="Jensen Huang",
        interviewee_title="CTO",
        transcript="We are investing heavily in AI infrastructure...",
        assessor="analyst@firm.com",
        dimensions_discussed=["talent"],
    )
    assert isinstance(note_id, str)
    assert note_id.startswith("interview_NVDA_")


@pytest.mark.asyncio
async def test_submit_interview_calls_index_documents_once():
    collector, mock_retriever = _make_collector()
    await collector.submit_interview(
        company_id="NVDA",
        interviewee="Jensen Huang",
        interviewee_title="CTO",
        transcript="We are investing heavily in AI infrastructure...",
        assessor="analyst@firm.com",
        dimensions_discussed=["talent"],
    )
    mock_retriever.index_documents.assert_called_once()


@pytest.mark.asyncio
async def test_submit_dd_finding_returns_note_id():
    collector, _ = _make_collector()
    note_id = await collector.submit_dd_finding(
        company_id="NVDA",
        title="No data quality monitoring",
        finding="The company lacks automated data quality checks...",
        dimension="data_infrastructure",
        severity="critical",
        assessor="analyst@firm.com",
    )
    assert isinstance(note_id, str)
    assert note_id.startswith("dd_NVDA_")


@pytest.mark.asyncio
async def test_submit_dd_finding_accepts_valid_severity():
    collector, _ = _make_collector()
    for severity in ("critical", "moderate", "low"):
        note_id = await collector.submit_dd_finding(
            company_id="NVDA",
            title=f"Finding ({severity})",
            finding="Some finding content here that is descriptive.",
            dimension="data_infrastructure",
            severity=severity,
            assessor="analyst@firm.com",
        )
        assert note_id is not None


@pytest.mark.asyncio
async def test_submit_dd_finding_unknown_severity_defaults_to_moderate():
    """Unknown severity string should not raise — defaults to MODERATE."""
    collector, mock_retriever = _make_collector()
    note_id = await collector.submit_dd_finding(
        company_id="NVDA",
        title="Test finding",
        finding="Some finding content here that is descriptive.",
        dimension="data_infrastructure",
        severity="unknown_severity",
        assessor="analyst@firm.com",
    )
    assert note_id is not None
    # Verify it still indexed (didn't crash)
    mock_retriever.index_documents.assert_called_once()


@pytest.mark.asyncio
async def test_submit_data_room_summary_returns_note_id():
    collector, _ = _make_collector()
    note_id = await collector.submit_data_room_summary(
        company_id="NVDA",
        document_name="AI Strategy 2026.pdf",
        summary="Document covers NVIDIA's five-year AI investment roadmap...",
        dimension="data_infrastructure",
        assessor="analyst@firm.com",
    )
    assert isinstance(note_id, str)
    assert note_id.startswith("dataroom_NVDA_")


@pytest.mark.asyncio
async def test_submit_data_room_summary_indexes_with_dd_data_room_type():
    collector, mock_retriever = _make_collector()
    await collector.submit_data_room_summary(
        company_id="NVDA",
        document_name="AI Strategy 2026.pdf",
        summary="Document covers NVIDIA's five-year AI investment roadmap...",
        dimension="data_infrastructure",
        assessor="analyst@firm.com",
    )
    call_args = mock_retriever.index_documents.call_args[0][0]  # first positional arg
    assert call_args[0]["metadata"]["source_type"] == "dd_data_room"


@pytest.mark.asyncio
async def test_submit_management_meeting_returns_note_id():
    collector, _ = _make_collector()
    note_id = await collector.submit_management_meeting(
        company_id="MSFT",
        meeting_title="Q4 AI Strategy Review",
        notes="Management discussed cloud AI adoption plans in detail...",
        assessor="analyst@firm.com",
        dimensions_discussed=["leadership", "ai_governance"],
    )
    assert isinstance(note_id, str)
    assert note_id.startswith("meeting_MSFT_")


@pytest.mark.asyncio
async def test_submit_site_visit_returns_note_id():
    collector, _ = _make_collector()
    note_id = await collector.submit_site_visit(
        company_id="NVDA",
        location="NVIDIA HQ, Santa Clara",
        observations="Observed GPU clusters and data center facilities...",
        assessor="analyst@firm.com",
        dimensions_discussed=["technology_stack"],
    )
    assert isinstance(note_id, str)
    assert note_id.startswith("sitevisit_NVDA_")
