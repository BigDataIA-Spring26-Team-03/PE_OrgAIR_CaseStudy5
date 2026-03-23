"""
MCP Integration Tests (Task 9.2 / grading requirement).

Verifies that MCP tools actually call CS1-CS4 clients and do NOT
return hardcoded data when those services are unavailable.

Run:
    pytest tests/test_mcp_integration.py -v --tb=short
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers — minimal mock objects matching CS3 / CS2 shapes
# ---------------------------------------------------------------------------

def _make_assessment(org_air: float = 72.5):
    from src.services.integration.cs3_client import Dimension
    dim_score = SimpleNamespace(score=org_air, level=3, evidence_count=5)
    return SimpleNamespace(
        org_air_score=org_air,
        vr_score=65.0,
        hr_score=70.0,
        synergy_score=5.0,
        confidence_interval=(org_air - 5, org_air + 5),
        evidence_count=10,
        dimension_scores={d: dim_score for d in Dimension},
    )


def _make_evidence_item():
    from enum import Enum

    class FakeCat(str, Enum):
        INNOVATION_ACTIVITY = "innovation_activity"

    class FakeSrc(str, Enum):
        SEC_FILING = "sec_filing"

    return SimpleNamespace(
        source_type=FakeSrc.SEC_FILING,
        content="Sample evidence content for testing purposes.",
        confidence=0.85,
        signal_category=FakeCat.INNOVATION_ACTIVITY,
        filing_type="10-K",
        extracted_at=None,
    )


# ---------------------------------------------------------------------------
# Task 9.2 — calculate_org_air_score calls CS3
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_calculate_org_air_calls_cs3():
    """Verify calculate_org_air_score routes to cs3_client.get_assessment."""
    import pe_mcp.server as srv

    with patch.object(srv.cs3_client, "get_assessment", new_callable=AsyncMock) as mock_cs3:
        mock_cs3.return_value = _make_assessment(72.5)
        result = await srv.call_tool("calculate_org_air_score", {"company_id": "NVDA"})

    # CS3 called at least once (main flow) and again by record_assessment for history
    assert mock_cs3.call_count >= 1
    mock_cs3.assert_any_call("NVDA")

    import json
    data = json.loads(result[0].text)
    assert data["org_air"] == 72.5
    assert "dimension_scores" in data


@pytest.mark.asyncio
async def test_no_hardcoded_data_when_cs3_down():
    """
    When CS3 and on-demand fallback both fail, the tool must surface an error,
    NOT silently return hardcoded scores.
    """
    import pe_mcp.server as srv

    with patch.object(srv.cs3_client, "get_assessment", new_callable=AsyncMock) as mock_cs3, \
         patch.object(srv.on_demand, "get_or_score_company", new_callable=AsyncMock) as mock_ods:
        mock_cs3.side_effect = ConnectionError("CS3 not running")
        mock_ods.side_effect = ConnectionError("On-demand scoring unavailable")
        result = await srv.call_tool("calculate_org_air_score", {"company_id": "NVDA"})

    text = result[0].text
    assert "Error" in text or "error" in text.lower(), (
        f"Expected error response when CS3 and on-demand are down, got: {text}"
    )


# ---------------------------------------------------------------------------
# Task 9.2 — get_company_evidence calls CS2
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_company_evidence_calls_cs2():
    """Verify get_company_evidence routes to cs2_client.get_evidence."""
    import pe_mcp.server as srv

    with patch.object(srv.cs2_client, "get_evidence", new_callable=AsyncMock) as mock_cs2:
        mock_cs2.return_value = [_make_evidence_item()]
        result = await srv.call_tool(
            "get_company_evidence",
            {"company_id": "NVDA", "dimension": "talent", "limit": 5},
        )

    mock_cs2.assert_called_once()

    import json
    items = json.loads(result[0].text)
    assert isinstance(items, list)
    assert len(items) == 1


@pytest.mark.asyncio
async def test_no_hardcoded_data_when_cs2_down():
    """Tool must error (not return fake evidence) when CS2 is down."""
    import pe_mcp.server as srv

    with patch.object(srv.cs2_client, "get_evidence", new_callable=AsyncMock) as mock_cs2:
        mock_cs2.side_effect = ConnectionError("CS2 not running")
        result = await srv.call_tool("get_company_evidence", {"company_id": "NVDA"})

    text = result[0].text
    assert "Error" in text or "error" in text.lower()


# ---------------------------------------------------------------------------
# Task 10.5 — FundAIRCalculator unit tests
# ---------------------------------------------------------------------------

def test_fund_air_weighted_average():
    """EV-weighted average should match manual calculation."""
    from src.services.analytics.fund_air import FundAIRCalculator

    companies = [
        SimpleNamespace(company_id="A", org_air=80.0, sector="technology",
                        delta_since_entry=5.0),
        SimpleNamespace(company_id="B", org_air=60.0, sector="retail",
                        delta_since_entry=-2.0),
    ]
    ev   = {"A": 200.0, "B": 100.0}
    calc = FundAIRCalculator()
    metrics = calc.calculate_fund_metrics("test_fund", companies, ev)

    # (200*80 + 100*60) / 300 = 73.33...
    assert abs(metrics.fund_air - 73.3) < 0.2
    assert metrics.company_count == 2
    assert metrics.ai_leaders_count == 1   # only A >= 70
    assert metrics.ai_laggards_count == 0  # neither < 50


def test_fund_air_empty_portfolio_raises():
    from src.services.analytics.fund_air import FundAIRCalculator
    calc = FundAIRCalculator()
    with pytest.raises(ValueError, match="empty portfolio"):
        calc.calculate_fund_metrics("test_fund", [], {})


# ---------------------------------------------------------------------------
# Task 9.4 — AssessmentHistoryService unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_assessment_history_cache_and_trend():
    """Manually seed cache + calculate_trend should return correct deltas."""
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal
    from src.services.tracking.assessment_history import (
        AssessmentHistoryService, AssessmentSnapshot,
    )

    svc = AssessmentHistoryService(cs1_client=None, cs3_client=None)

    old_ts = datetime.now(timezone.utc) - timedelta(days=100)

    snap1 = AssessmentSnapshot(
        company_id="NVDA", timestamp=old_ts,
        org_air=Decimal("50.0"), vr_score=Decimal("45.0"),
        hr_score=Decimal("55.0"), synergy_score=Decimal("3.0"),
        dimension_scores={}, confidence_interval=(45.0, 55.0),
        evidence_count=5, assessor_id="test", assessment_type="full",
    )
    snap2 = AssessmentSnapshot(
        company_id="NVDA", timestamp=datetime.now(timezone.utc),
        org_air=Decimal("62.0"), vr_score=Decimal("57.0"),
        hr_score=Decimal("67.0"), synergy_score=Decimal("4.0"),
        dimension_scores={}, confidence_interval=(57.0, 67.0),
        evidence_count=8, assessor_id="test", assessment_type="full",
    )

    svc._cache["NVDA"] = [snap1, snap2]

    trend = await svc.calculate_trend("NVDA")

    assert trend.company_id == "NVDA"
    assert trend.entry_org_air == 50.0
    assert trend.current_org_air == 62.0
    assert trend.delta_since_entry == 12.0
    assert trend.trend_direction == "improving"
    assert trend.snapshot_count == 2


# ---------------------------------------------------------------------------
# Task 10.3 — HITL triggers correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hitl_triggers_for_extreme_scores():
    """ScoringAgent must set requires_approval=True for scores outside [40, 85]."""
    from agents.specialists import ScoringAgent

    agent = ScoringAgent()

    # Patch the underlying tool caller
    async def _fake_tool(tool_name, arguments):
        return '{"org_air": 92.0, "vr_score": 88.0, "hr_score": 75.0, ' \
               '"synergy_score": 5.0, "confidence_interval": [87, 97], ' \
               '"dimension_scores": {}}'

    agent_state = {
        "company_id": "NVDA",
        "assessment_type": "full",
        "requested_by": "test",
        "messages": [],
        "sec_analysis": None, "talent_analysis": None,
        "scoring_result": None, "evidence_justifications": None,
        "value_creation_plan": None, "next_agent": None,
        "requires_approval": False, "approval_reason": None,
        "approval_status": None, "approved_by": None,
        "started_at": "2026-01-01T00:00:00", "completed_at": None,
        "total_tokens": 0, "error": None,
    }

    from agents import specialists
    original = specialists.mcp_client.call_tool
    specialists.mcp_client.call_tool = _fake_tool

    try:
        result = await agent.calculate(agent_state)
    finally:
        specialists.mcp_client.call_tool = original

    assert result["requires_approval"] is True
    assert result["approval_status"] == "pending"
    assert "92" in result["approval_reason"]


@pytest.mark.asyncio
async def test_hitl_does_not_trigger_for_normal_scores():
    """ScoringAgent must NOT set requires_approval for scores in [40, 85]."""
    from agents.specialists import ScoringAgent

    agent = ScoringAgent()

    async def _fake_tool(tool_name, arguments):
        return '{"org_air": 68.0, "vr_score": 65.0, "hr_score": 70.0, ' \
               '"synergy_score": 4.0, "confidence_interval": [63, 73], ' \
               '"dimension_scores": {}}'

    agent_state = {
        "company_id": "NVDA", "assessment_type": "full",
        "requested_by": "test", "messages": [],
        "sec_analysis": None, "talent_analysis": None,
        "scoring_result": None, "evidence_justifications": None,
        "value_creation_plan": None, "next_agent": None,
        "requires_approval": False, "approval_reason": None,
        "approval_status": None, "approved_by": None,
        "started_at": "2026-01-01T00:00:00", "completed_at": None,
        "total_tokens": 0, "error": None,
    }

    from agents import specialists
    original = specialists.mcp_client.call_tool
    specialists.mcp_client.call_tool = _fake_tool

    try:
        result = await agent.calculate(agent_state)
    finally:
        specialists.mcp_client.call_tool = original

    assert result["requires_approval"] is False
    assert result["approval_status"] is None


# ---------------------------------------------------------------------------
# On-demand scoring — unknown ticker triggers pipeline; known ticker skips it
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_ticker_triggers_pipeline():
    """
    When CS3 returns 404/exception for an unknown ticker,
    calculate_org_air_score must call on_demand.get_or_score_company.
    """
    import pe_mcp.server as srv

    with patch.object(
        srv.cs3_client, "get_assessment", new_callable=AsyncMock
    ) as mock_cs3, patch.object(
        srv.on_demand, "get_or_score_company", new_callable=AsyncMock
    ) as mock_ods:
        mock_cs3.side_effect = Exception("HTTP 404: ticker AAPL not found")
        mock_ods.return_value = _make_assessment(61.0)

        result = await srv.call_tool("calculate_org_air_score", {"company_id": "AAPL"})

    # on_demand must have been called because CS3 raised
    mock_ods.assert_called_once_with("AAPL")

    import json
    data = json.loads(result[0].text)
    assert data["org_air"] == 61.0
    assert data["freshly_scored"] is True


@pytest.mark.asyncio
async def test_known_ticker_skips_pipeline():
    """
    When CS3 returns a cached assessment, on_demand must NOT be called.
    """
    import pe_mcp.server as srv

    with patch.object(
        srv.cs3_client, "get_assessment", new_callable=AsyncMock
    ) as mock_cs3, patch.object(
        srv.on_demand, "get_or_score_company", new_callable=AsyncMock
    ) as mock_ods:
        mock_cs3.return_value = _make_assessment(72.5)

        result = await srv.call_tool("calculate_org_air_score", {"company_id": "NVDA"})

    # on_demand must NOT have been called — CS3 cache was sufficient
    mock_ods.assert_not_called()

    import json
    data = json.loads(result[0].text)
    assert data["org_air"] == 72.5
    assert data["freshly_scored"] is False