"""
MCP Integration Tests (Task 9.2 / grading requirement)

Verifies that MCP tools actually call CS1-CS4 clients and do NOT
return hardcoded data when those services are unavailable.

Run: pytest tests/test_mcp_integration.py -v --tb=short
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers — minimal mock objects matching CS3 / CS2 shapes
# ---------------------------------------------------------------------------

def _make_assessment(org_air: float = 72.5):
    from types import SimpleNamespace
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
    from types import SimpleNamespace
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
    import mcp.server as srv

    with patch.object(srv.cs3_client, "get_assessment", new_callable=AsyncMock) as mock_cs3:
        mock_cs3.return_value = _make_assessment(72.5)

        result = await srv.call_tool("calculate_org_air_score", {"company_id": "NVDA"})

    # CS3 must have been called exactly once with the right ticker
    mock_cs3.assert_called_once_with("NVDA")

    import json
    data = json.loads(result[0].text)
    assert data["org_air"] == 72.5
    assert "dimension_scores" in data


@pytest.mark.asyncio
async def test_no_hardcoded_data_when_cs3_down():
    """
    When CS3 is unavailable the tool must raise/return an error,
    NOT silently return hardcoded scores.
    """
    import mcp.server as srv

    with patch.object(srv.cs3_client, "get_assessment", new_callable=AsyncMock) as mock_cs3:
        mock_cs3.side_effect = ConnectionError("CS3 not running")

        result = await srv.call_tool("calculate_org_air_score", {"company_id": "NVDA"})

    # Must surface the error — must NOT return a valid score dict
    import json
    text = result[0].text
    assert "Error" in text or "error" in text.lower(), (
        f"Expected error response when CS3 is down, got: {text}"
    )


# ---------------------------------------------------------------------------
# Task 9.2 — get_company_evidence calls CS2
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_company_evidence_calls_cs2():
    """Verify get_company_evidence routes to cs2_client.get_evidence."""
    import mcp.server as srv

    with patch.object(srv.cs2_client, "get_evidence", new_callable=AsyncMock) as mock_cs2:
        mock_cs2.return_value = [_make_evidence_item()]

        result = await srv.call_tool(
            "get_company_evidence",
            {"company_id": "NVDA", "dimension": "talent", "limit": 5},
        )

    mock_cs2.assert_called_once()
    call_kwargs = mock_cs2.call_args
    assert call_kwargs.kwargs.get("company_id") == "NVDA" or call_kwargs.args[0] == "NVDA"

    import json
    items = json.loads(result[0].text)
    assert isinstance(items, list)
    assert len(items) == 1


@pytest.mark.asyncio
async def test_no_hardcoded_data_when_cs2_down():
    """Tool must error (not return fake evidence) when CS2 is down."""
    import mcp.server as srv

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
    from types import SimpleNamespace

    companies = [
        SimpleNamespace(company_id="A", org_air=80.0, sector="technology",
                        delta_since_entry=5.0),
        SimpleNamespace(company_id="B", org_air=60.0, sector="retail",
                        delta_since_entry=-2.0),
    ]
    ev = {"A": 200.0, "B": 100.0}
    calc = FundAIRCalculator()
    metrics = calc.calculate_fund_metrics("test_fund", companies, ev)

    # (200*80 + 100*60) / 300 = 22000/300 ≈ 73.3
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
async def test_assessment_history_record_and_trend():
    """record_from_data + calculate_trend should return correct deltas."""
    from src.services.tracking.assessment_history import AssessmentHistoryService
    from datetime import datetime, timedelta

    svc = AssessmentHistoryService()

    # Seed two snapshots: entry (older) and current
    old_ts = datetime.utcnow() - timedelta(days=100)
    await svc.record_from_data(
        "NVDA", org_air=50.0, vr_score=45.0, hr_score=55.0,
        synergy_score=3.0, dimension_scores={}, confidence_interval=(45, 55),
        evidence_count=5, timestamp=old_ts,
    )
    await svc.record_from_data(
        "NVDA", org_air=62.0, vr_score=57.0, hr_score=67.0,
        synergy_score=4.0, dimension_scores={}, confidence_interval=(57, 67),
        evidence_count=8,
    )

    trend = await svc.calculate_trend("NVDA")

    assert trend.company_id == "NVDA"
    assert trend.entry_org_air == 50.0
    assert trend.current_org_air == 62.0
    assert trend.delta_since_entry == 12.0
    assert trend.trend_direction == "improving"
    assert trend.snapshot_count == 2