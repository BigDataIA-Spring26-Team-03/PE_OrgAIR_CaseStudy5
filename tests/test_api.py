from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.snowflake import db


# -----------------------------
# Test client fixture
# -----------------------------
@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


# -----------------------------
# Cleanup tracker
# -----------------------------
@dataclass
class CreatedIds:
    company_ids: List[str] = field(default_factory=list)
    assessment_ids: List[str] = field(default_factory=list)
    score_ids: List[str] = field(default_factory=list)

    def cleanup(self) -> None:
        """
        Hard-delete created rows so Snowflake stays clean.
        Delete order matters because of FKs:
          dimension_scores -> assessments -> companies
        """
        # 1) Dimension scores
        for sid in self.score_ids:
            try:
                db.execute_update("DELETE FROM dimension_scores WHERE id = %(id)s", {"id": sid})
            except Exception:
                pass

        # 2) Assessments
        for aid in self.assessment_ids:
            try:
                db.execute_update("DELETE FROM assessments WHERE id = %(id)s", {"id": aid})
            except Exception:
                pass

        # 3) Companies
        for cid in self.company_ids:
            try:
                db.execute_update("DELETE FROM companies WHERE id = %(id)s", {"id": cid})
            except Exception:
                pass


@pytest.fixture()
def created() -> CreatedIds:
    tracker = CreatedIds()
    try:
        yield tracker
    finally:
        tracker.cleanup()


# -----------------------------
# Helpers
# -----------------------------
def get_valid_industry_id(client: TestClient) -> str:
    """
    GET /api/v1/companies/available-industries
    returns IndustryListResponse:
      { "items": [ { "id": "...", "name": "...", ... }, ... ] }
    """
    r = client.get("/api/v1/companies/available-industries")
    assert r.status_code == 200

    data = r.json()
    assert isinstance(data, dict)
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) > 0
    return data["items"][0]["id"]


def create_company(
    client: TestClient,
    created: CreatedIds,
    name: str = "Test Company",
    ticker: str = "test",
    position_factor: float = 0.2,
) -> dict:
    payload = {
        "name": name,
        "ticker": ticker,
        "industry_id": get_valid_industry_id(client),
        "position_factor": position_factor,
    }
    r = client.post("/api/v1/companies", json=payload)
    assert r.status_code == 201
    body = r.json()
    created.company_ids.append(body["id"])
    return body


def create_assessment(
    client: TestClient,
    created: CreatedIds,
    company_id: str,
    assessment_type: str = "screening",
) -> dict:
    payload = {
        "company_id": company_id,
        "assessment_type": assessment_type,
    }
    r = client.post("/api/v1/assessments", json=payload)
    assert r.status_code == 201
    body = r.json()
    created.assessment_ids.append(body["id"])
    return body


def create_dimension_score(
    client: TestClient,
    created: CreatedIds,
    assessment_id: str,
    dimension: str = "talent_skills",
    score: int = 85,
) -> dict:
    payload = {
        "assessment_id": assessment_id,
        "dimension": dimension,
        "score": score,
    }
    r = client.post(f"/api/v1/assessments/{assessment_id}/scores", json=payload)
    assert r.status_code == 201
    body = r.json()
    # DimensionScoreResponse should include "id"
    created.score_ids.append(body["id"])
    return body


# -----------------------------
# Health
# -----------------------------
def test_health_endpoint(client: TestClient):
    r = client.get("/health")
    assert r.status_code in (200, 503)  # depending on how strict you make it
    data = r.json()
    assert "status" in data
    assert "dependencies" in data


# -----------------------------
# Companies
# -----------------------------
def test_create_company_success(client: TestClient, created: CreatedIds):
    payload = {
        "name": "Apple",
        "ticker": "aapl",
        "industry_id": get_valid_industry_id(client),
        "position_factor": 0.2,
    }

    response = client.post("/api/v1/companies", json=payload)
    assert response.status_code == 201

    body = response.json()
    created.company_ids.append(body["id"])

    assert body["name"] == "Apple"
    assert body["ticker"] == "AAPL"
    assert "id" in body
    assert "created_at" in body
    assert "updated_at" in body


def test_create_company_invalid_position_factor(client: TestClient):
    payload = {
        "name": "Bad Corp",
        "ticker": "bad",
        "industry_id": get_valid_industry_id(client),
        "position_factor": 2.0,  # invalid (must be <= 1.0)
    }
    response = client.post("/api/v1/companies", json=payload)
    assert response.status_code == 422


def test_get_company_by_id(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="Microsoft", ticker="msft", position_factor=0.1)
    company_id = company["id"]

    response = client.get(f"/api/v1/companies/{company_id}")
    assert response.status_code == 200
    assert response.json()["ticker"] == "MSFT"


def test_list_companies_pagination(client: TestClient):
    response = client.get("/api/v1/companies?limit=1&offset=0")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) <= 1


def test_delete_company_soft_delete(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="Delete Me", ticker="del", position_factor=0.0)
    company_id = company["id"]

    delete_response = client.delete(f"/api/v1/companies/{company_id}")
    assert delete_response.status_code == 204

    get_response = client.get(f"/api/v1/companies/{company_id}")
    assert get_response.status_code == 404


# -----------------------------
# Assessments (Snowflake-backed)
# -----------------------------
def test_create_assessment_defaults_to_draft(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="Assess Co", ticker="asst")
    a = client.post(
        "/api/v1/assessments",
        json={"company_id": company["id"], "assessment_type": "screening"},
    )
    assert a.status_code == 201
    body = a.json()
    created.assessment_ids.append(body["id"])
    assert body["status"] == "draft"


def test_update_assessment_status_patch(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="Status Co", ticker="stat")
    assessment = create_assessment(client, created, company["id"], "screening")
    assessment_id = assessment["id"]

    r2 = client.patch(
        f"/api/v1/assessments/{assessment_id}/status",
        json={"status": "submitted"},
    )
    assert r2.status_code == 200
    assert r2.json()["status"] == "submitted"


def test_list_assessments_filter_by_company(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="Filter Co", ticker="filt")
    company_id = company["id"]

    create_assessment(client, created, company_id, "screening")
    create_assessment(client, created, company_id, "quarterly")

    r = client.get(f"/api/v1/assessments?company_id={company_id}&limit=10&offset=0")
    assert r.status_code == 200
    items = r.json()
    assert isinstance(items, list)
    assert len(items) >= 2
    assert all(a["company_id"] == company_id for a in items)


# -----------------------------
# Dimension Scores (Snowflake-backed)
# -----------------------------
def test_add_dimension_score(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="Dim Co", ticker="dimc")
    assessment = create_assessment(client, created, company["id"], "screening")
    assessment_id = assessment["id"]

    score = create_dimension_score(client, created, assessment_id, "talent_skills", 85)

    assert score["score"] == 85
    assert score["dimension"] == "talent_skills"
    assert "id" in score


def test_list_dimension_scores(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="ListDim Co", ticker="ldim")
    assessment = create_assessment(client, created, company["id"], "screening")
    assessment_id = assessment["id"]

    create_dimension_score(client, created, assessment_id, "data_infrastructure", 70)

    r = client.get(f"/api/v1/assessments/{assessment_id}/scores")
    assert r.status_code == 200
    scores = r.json()
    assert isinstance(scores, list)
    assert any(s["dimension"] == "data_infrastructure" and float(s["score"]) == 70 for s in scores)


def test_delete_dimension_score(client: TestClient, created: CreatedIds):
    company = create_company(client, created, name="DelDim Co", ticker="ddim")
    assessment = create_assessment(client, created, company["id"], "screening")
    assessment_id = assessment["id"]

    create_dimension_score(client, created, assessment_id, "ai_governance", 90)

    d = client.delete(f"/api/v1/assessments/{assessment_id}/scores/ai_governance")
    assert d.status_code == 204

    r = client.get(f"/api/v1/assessments/{assessment_id}/scores")
    assert r.status_code == 200
    scores = r.json()
    assert all(s["dimension"] != "ai_governance" for s in scores)
