# tests/test_evidence_api.py
#
# Tests for GET /api/v1/evidence and related helpers:
# - _looks_like_job_posting heuristic
# - signal_categories filter
# - job-posting exclusion when filtering for leadership
# - limit and max_content_length (truncation)
# - board_composition -> governance_signals mapping

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from app.main import app


# ---------------------------------------------------------------------------
# Unit tests: _looks_like_job_posting
# ---------------------------------------------------------------------------

def test_looks_like_job_posting_detects_engineer():
    from app.routers.evidence import _looks_like_job_posting
    assert _looks_like_job_posting("Senior Deep Learning Algorithm Engineer") is True
    assert _looks_like_job_posting("ASIC Clocks Verification Engineer") is True
    assert _looks_like_job_posting("Senior Solutions Architect") is True


def test_looks_like_job_posting_detects_intern_and_new_grad():
    from app.routers.evidence import _looks_like_job_posting
    assert _looks_like_job_posting("Applied Deep Learning Scientist Intern, Bio Foundation Model Research - Summer 2026") is True
    assert _looks_like_job_posting("Power Architect - New College Grad 2026") is True


def test_looks_like_job_posting_rejects_executive_format():
    from app.routers.evidence import _looks_like_job_posting
    assert _looks_like_job_posting("Jensen Huang — CEO") is False
    assert _looks_like_job_posting("Jane Smith – Chief Technology Officer") is False


def test_looks_like_job_posting_rejects_short_or_empty():
    from app.routers.evidence import _looks_like_job_posting
    assert _looks_like_job_posting("") is False
    assert _looks_like_job_posting("CEO") is False  # too short


def test_looks_like_job_posting_rejects_non_job_content():
    from app.routers.evidence import _looks_like_job_posting
    assert _looks_like_job_posting("The board established a technology committee in 2023.") is False
    assert _looks_like_job_posting("Director of AI Governance and Compliance") is False  # no engineer/dev/intern


# ---------------------------------------------------------------------------
# Fixtures — mock Snowflake + Redis
# ---------------------------------------------------------------------------

MOCK_SEC_ROWS = [
    {
        "evidence_id": "sec-1",
        "company_id": "NVDA",
        "source_type": "sec_10k_item_1",
        "signal_category": "digital_presence",
        "content": "NVIDIA operates GPU data centers with ML pipelines.",
        "confidence": 0.85,
        "fiscal_year": 2024,
        "source_url": None,
        "created_at": None,
    }
]

MOCK_LEADERSHIP_EXEC = [
    {
        "evidence_id": "lead-1",
        "company_id": "NVDA",
        "source_type": "board_proxy_def14a",
        "signal_category": "leadership_signals",
        "content": "Jensen Huang — CEO and co-founder",
        "confidence": 0.9,
        "fiscal_year": 2024,
        "source_url": None,
        "created_at": None,
    }
]

MOCK_LEADERSHIP_JOB_NOISE = [
    {
        "evidence_id": "lead-2",
        "company_id": "NVDA",
        "source_type": "board_proxy_def14a",
        "signal_category": "leadership_signals",
        "content": "Senior Deep Learning Algorithm Engineer",
        "confidence": 0.8,
        "fiscal_year": 2024,
        "source_url": None,
        "created_at": None,
    }
]

MOCK_GOVERNANCE_ROWS = [
    {
        "evidence_id": "gov-1",
        "company_id": "NVDA",
        "source_type": "board_proxy_def14a",
        "signal_category": "governance_signals",
        "content": "Board established AI oversight committee.",
        "confidence": 0.85,
        "fiscal_year": None,
        "source_url": None,
        "created_at": None,
    }
]

MOCK_TECH_HIRING = [
    {
        "evidence_id": "job-1",
        "company_id": "NVDA",
        "source_type": "job_posting_linkedin",
        "signal_category": "technology_hiring",
        "content": "Senior ML Engineer - AI Platform",
        "confidence": 0.8,
        "fiscal_year": 2024,
        "source_url": None,
        "created_at": None,
    }
]


def _make_snowflake_execute_side_effect(sec, ext, glassdoor, board):
    """Return a side_effect that yields different results per query (by SQL content)."""

    def execute_query(query, params):
        if "document_chunks_sec" in query or "documents_sec" in query:
            return sec or []
        if "external_signals" in query:
            return ext or []
        if "glassdoor_reviews" in query:
            return glassdoor or []
        if "board_governance_signals" in query:
            return board or []
        return []

    return execute_query


@pytest.fixture
def mock_snowflake():
    """Patch SnowflakeService to avoid real DB."""
    with patch("app.routers.evidence.SnowflakeService") as MockSF:
        svc = MagicMock()
        MockSF.return_value = svc
        yield svc


@pytest.fixture
def mock_cache():
    """Patch Redis cache for indexed filter."""
    with patch("app.routers.evidence.cache") as mock_cache_mod:
        mock_client = MagicMock()
        mock_client.smembers.return_value = set()
        mock_cache_mod.client = mock_client
        yield mock_client


# ---------------------------------------------------------------------------
# API tests
# ---------------------------------------------------------------------------

def test_evidence_returns_200_and_list(mock_snowflake, mock_cache):
    mock_snowflake.execute_query.side_effect = _make_snowflake_execute_side_effect(
        MOCK_SEC_ROWS, [], [], []
    )
    client = TestClient(app)
    resp = client.get("/api/v1/evidence?company_id=NVDA")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert data[0]["evidence_id"] == "sec-1"
    assert data[0]["signal_category"] == "digital_presence"


def test_evidence_respects_limit(mock_snowflake, mock_cache):
    many_rows = [dict(MOCK_SEC_ROWS[0], evidence_id=f"sec-{i}") for i in range(100)]
    mock_snowflake.execute_query.side_effect = _make_snowflake_execute_side_effect(
        many_rows, [], [], []
    )
    client = TestClient(app)
    resp = client.get("/api/v1/evidence?company_id=NVDA&limit=5")
    assert resp.status_code == 200
    assert len(resp.json()) <= 5


def test_evidence_filters_by_signal_categories(mock_snowflake, mock_cache):
    mock_snowflake.execute_query.side_effect = _make_snowflake_execute_side_effect(
        MOCK_SEC_ROWS,
        MOCK_LEADERSHIP_EXEC + MOCK_TECH_HIRING,
        [],
        MOCK_GOVERNANCE_ROWS,
    )
    client = TestClient(app)
    resp = client.get(
        "/api/v1/evidence?company_id=NVDA"
        "&signal_categories=leadership_signals,board_composition&limit=50"
    )
    assert resp.status_code == 200
    data = resp.json()
    cats = {r["signal_category"] for r in data}
    assert "digital_presence" not in cats
    assert "technology_hiring" not in cats
    assert "leadership_signals" in cats or "governance_signals" in cats


def test_evidence_excludes_job_posting_noise_from_leadership(mock_snowflake, mock_cache):
    mock_snowflake.execute_query.side_effect = _make_snowflake_execute_side_effect(
        [],
        MOCK_LEADERSHIP_EXEC + MOCK_LEADERSHIP_JOB_NOISE,
        [],
        [],
    )
    client = TestClient(app)
    resp = client.get(
        "/api/v1/evidence?company_id=NVDA"
        "&signal_categories=leadership_signals&limit=50"
    )
    assert resp.status_code == 200
    data = resp.json()
    contents = [r["content"] for r in data]
    assert any("Jensen Huang" in c for c in contents)
    assert not any("Senior Deep Learning Algorithm Engineer" in c for c in contents)


def test_evidence_truncates_long_content(mock_snowflake, mock_cache):
    long_content = "A" * 2000
    mock_snowflake.execute_query.side_effect = _make_snowflake_execute_side_effect(
        [dict(MOCK_SEC_ROWS[0], content=long_content)],
        [],
        [],
        [],
    )
    client = TestClient(app)
    resp = client.get("/api/v1/evidence?company_id=NVDA&limit=10&max_content_length=200")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    assert len(data[0]["content"]) <= 203  # 200 + "…"
    assert data[0]["content"].endswith("…") or len(data[0]["content"]) <= 200


def test_evidence_board_composition_includes_governance_signals(mock_snowflake, mock_cache):
    mock_snowflake.execute_query.side_effect = _make_snowflake_execute_side_effect(
        [],
        [],
        [],
        MOCK_GOVERNANCE_ROWS,
    )
    client = TestClient(app)
    resp = client.get(
        "/api/v1/evidence?company_id=NVDA"
        "&signal_categories=board_composition&limit=20"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    assert data[0]["signal_category"] == "governance_signals"
    assert "AI oversight" in data[0]["content"]


def test_mark_indexed_returns_updated_count(mock_cache):
    client = TestClient(app)
    resp = client.post(
        "/api/v1/evidence/mark-indexed",
        json={"evidence_ids": ["e1", "e2", "e3"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "updated_count" in body
    assert body["updated_count"] == 3


def test_mark_indexed_empty_ids_returns_zero(mock_cache):
    client = TestClient(app)
    resp = client.post("/api/v1/evidence/mark-indexed", json={"evidence_ids": []})
    assert resp.status_code == 200
    assert resp.json()["updated_count"] == 0
