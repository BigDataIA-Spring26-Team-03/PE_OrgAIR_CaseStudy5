# tests/test_cs4_api.py
#
# FastAPI endpoint tests using TestClient.
# - Search endpoint: uses seeded_retriever (no server needed)
# - Justification endpoint: monkeypatches get_generator() to avoid real CS3/LLM calls
#
# The search tests run completely offline (real ChromaDB in tmp_path).
# The justification tests verify routing + response shape, not real LLM output.

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

from app.main import app
from src.services.justification.generator import (
    CitedEvidence,
    ScoreJustification,
)
from src.services.integration.cs3_client import Dimension, ScoreLevel


# ---------------------------------------------------------------------------
# Helpers — build fake ScoreJustification for mocking
# ---------------------------------------------------------------------------

def _fake_justification(company_id: str = "NVDA", dimension: Dimension = Dimension.DATA_INFRASTRUCTURE):
    return ScoreJustification(
        company_id=company_id,
        dimension=dimension,
        score=72.0,
        level=4,
        level_name="Good",
        confidence_interval=(65.0, 79.0),
        rubric_criteria="Company has cloud data platform with real-time pipelines.",
        rubric_keywords=["cloud", "data lake", "ml pipeline"],
        supporting_evidence=[
            CitedEvidence(
                evidence_id="e1",
                content="NVIDIA GPU data center with cloud ML pipelines.",
                source_type="sec_10k_item_1",
                source_url=None,
                confidence=0.9,
                matched_keywords=["cloud", "data lake"],
                relevance_score=0.85,
            )
        ],
        gaps_identified=["No evidence of 'data mesh' (Level 5 criterion)"],
        generated_summary=(
            "NVIDIA scored 72/100 (Level 4 - Good) on Data Infrastructure. "
            "Evidence from SEC 10-K confirms cloud-native GPU data center with ML pipelines. "
            "Key gap: data mesh architecture not yet evidenced."
        ),
        evidence_strength="strong",
    )


# ---------------------------------------------------------------------------
# Search endpoint tests (uses seeded_retriever via monkeypatch)
# ---------------------------------------------------------------------------

@pytest.fixture
def search_client(seeded_retriever, monkeypatch):
    """TestClient with get_retriever() patched to return the seeded retriever."""
    import app.routers.search as search_module
    # Clear lru_cache so our patch takes effect
    search_module.get_retriever.cache_clear()
    monkeypatch.setattr(search_module, "get_retriever", lambda: seeded_retriever)
    return TestClient(app)


def test_search_basic_returns_200(search_client):
    resp = search_client.get("/api/v1/search?query=data+center")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_search_respects_top_k(search_client):
    resp = search_client.get("/api/v1/search?query=cloud&top_k=2")
    assert resp.status_code == 200
    assert len(resp.json()) <= 2


def test_search_result_shape(search_client):
    resp = search_client.get("/api/v1/search?query=NVIDIA+AI&top_k=3")
    assert resp.status_code == 200
    results = resp.json()
    if results:
        r = results[0]
        assert "doc_id" in r
        assert "content" in r
        assert "score" in r
        assert "retrieval_method" in r
        assert "metadata" in r


def test_search_company_id_filter(search_client):
    resp = search_client.get("/api/v1/search?query=cloud+analytics&company_id=NVDA&top_k=10")
    assert resp.status_code == 200
    for r in resp.json():
        assert r["metadata"].get("company_id") == "NVDA"


def test_search_top_k_too_low_returns_422(search_client):
    resp = search_client.get("/api/v1/search?query=test&top_k=0")
    assert resp.status_code == 422


def test_search_top_k_too_high_returns_422(search_client):
    resp = search_client.get("/api/v1/search?query=test&top_k=51")
    assert resp.status_code == 422


def test_search_min_confidence_filter(search_client):
    # min_confidence=0.9 should exclude e3 (conf=0.75)
    resp = search_client.get("/api/v1/search?query=culture+innovation&min_confidence=0.9")
    assert resp.status_code == 200
    for r in resp.json():
        meta_conf = r["metadata"].get("confidence")
        if meta_conf is not None:
            assert float(meta_conf) >= 0.9


def test_search_missing_query_returns_422(search_client):
    resp = search_client.get("/api/v1/search")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Justification endpoint tests (generator mocked)
# ---------------------------------------------------------------------------

@pytest.fixture
def justification_client(monkeypatch):
    """TestClient with get_generator() and get_ic_workflow() patched."""
    import app.routers.justification as jrouter

    fake_j = _fake_justification()

    mock_generator = MagicMock()
    mock_generator.generate_justification = AsyncMock(return_value=fake_j)

    jrouter.get_generator.cache_clear()
    monkeypatch.setattr(jrouter, "get_generator", lambda: mock_generator)

    return TestClient(app)


def test_justification_single_dimension_200(justification_client):
    resp = justification_client.get("/api/v1/justification/NVDA/data_infrastructure")
    assert resp.status_code == 200


def test_justification_response_shape(justification_client):
    resp = justification_client.get("/api/v1/justification/NVDA/data_infrastructure")
    body = resp.json()
    assert body["company_id"] == "NVDA"
    assert body["dimension"] == "data_infrastructure"
    assert "score" in body
    assert "level" in body
    assert "level_name" in body
    assert "generated_summary" in body
    assert "evidence_strength" in body
    assert "supporting_evidence" in body
    assert "gaps_identified" in body


def test_justification_invalid_dimension_returns_400(justification_client):
    resp = justification_client.get("/api/v1/justification/NVDA/not_a_dimension")
    assert resp.status_code == 400


def test_justification_evidence_list_structure(justification_client):
    resp = justification_client.get("/api/v1/justification/NVDA/data_infrastructure")
    evidence = resp.json()["supporting_evidence"]
    assert isinstance(evidence, list)
    if evidence:
        e = evidence[0]
        assert "evidence_id" in e
        assert "content" in e
        assert "confidence" in e
        assert "matched_keywords" in e


# ---------------------------------------------------------------------------
# IC Prep endpoint tests (ic_workflow mocked)
# ---------------------------------------------------------------------------

@pytest.fixture
def ic_client(monkeypatch):
    """TestClient with get_ic_workflow() patched."""
    import app.routers.justification as jrouter
    from src.services.workflows.ic_prep import ICMeetingPackage
    from src.services.integration.cs1_client import Company, Sector
    from src.services.integration.cs3_client import CompanyAssessment
    from datetime import datetime

    # minimal fake company
    fake_company = Company(
        company_id="nvda-uuid", ticker="NVDA", name="NVIDIA Corporation",
        sector=Sector.TECHNOLOGY, sub_sector="Semiconductors",
        market_cap_percentile=0.95, revenue_millions=60000.0,
        employee_count=26000, fiscal_year_end="January",
        industry_id="semi-123", position_factor=0.2,
    )
    # minimal fake assessment
    fake_assessment = CompanyAssessment(
        company_id="nvda-uuid", vr_score=78.0, hr_score=72.0,
        synergy_score=0.8, org_air_score=76.0,
        confidence_interval=(70.0, 82.0), dimension_scores={},
        talent_concentration=0.15, position_factor=0.2,
        assessment_date="2024-01-01",
    )

    fake_pkg = ICMeetingPackage(
        company=fake_company,
        assessment=fake_assessment,
        dimension_justifications={Dimension.DATA_INFRASTRUCTURE: _fake_justification()},
        executive_summary="NVIDIA demonstrates strong AI infrastructure.",
        key_strengths=["Data Infrastructure (72)"],
        key_gaps=[],
        risk_factors=[],
        recommendation="PROCEED",
        generated_at=datetime.now().isoformat(),
        total_evidence_count=1,
        avg_evidence_strength="strong",
    )

    mock_workflow = MagicMock()
    mock_workflow.prepare_meeting = AsyncMock(return_value=fake_pkg)

    jrouter.get_ic_workflow.cache_clear()
    monkeypatch.setattr(jrouter, "get_ic_workflow", lambda: mock_workflow)

    return TestClient(app)


def test_ic_prep_returns_200(ic_client):
    resp = ic_client.post("/api/v1/justification/NVDA/ic-prep", json={})
    assert resp.status_code == 200


def test_ic_prep_response_shape(ic_client):
    resp = ic_client.post("/api/v1/justification/NVDA/ic-prep", json={})
    body = resp.json()
    assert "executive_summary" in body
    assert "recommendation" in body
    assert "key_strengths" in body
    assert "key_gaps" in body
    assert "risk_factors" in body
    assert "org_air_score" in body
    assert "dimension_justifications" in body


def test_ic_prep_recommendation_valid_value(ic_client):
    resp = ic_client.post("/api/v1/justification/NVDA/ic-prep", json={})
    rec = resp.json()["recommendation"]
    assert rec in ("PROCEED", "PROCEED WITH CAUTION", "FURTHER DILIGENCE")


def test_ic_prep_with_focus_dimensions(ic_client):
    resp = ic_client.post(
        "/api/v1/justification/NVDA/ic-prep",
        json={"focus_dimensions": ["data_infrastructure"]},
    )
    assert resp.status_code == 200


def test_ic_prep_invalid_focus_dimension_returns_400(ic_client):
    resp = ic_client.post(
        "/api/v1/justification/NVDA/ic-prep",
        json={"focus_dimensions": ["invalid_dimension"]},
    )
    assert resp.status_code == 400
