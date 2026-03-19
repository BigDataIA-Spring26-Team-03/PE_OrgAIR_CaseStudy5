import pytest
from uuid import uuid4

from app.models.company import CompanyCreate
from app.models.assessment import AssessmentResponse
from app.models.dimension import DimensionScoreCreate, Dimension


def test_company_ticker_is_uppercased():
    company = CompanyCreate(
        name="Test Corp",
        ticker="aapl",
        industry_id=uuid4(),
        position_factor=0.2,
    )
    assert company.ticker == "AAPL"


def test_company_position_factor_bounds():
    with pytest.raises(ValueError):
        CompanyCreate(
            name="Bad Corp",
            ticker="BAD",
            industry_id=uuid4(),
            position_factor=2.0,  # invalid, must be <= 1.0
        )


def test_assessment_confidence_interval_validation():
    with pytest.raises(ValueError):
        AssessmentResponse(
            id=uuid4(),
            company_id=uuid4(),
            assessment_type="screening",
            confidence_lower=80,
            confidence_upper=70,
        )


def test_dimension_score_default_weight():
    score = DimensionScoreCreate(
        assessment_id=uuid4(),
        dimension=Dimension.TALENT_SKILLS,
        score=85,
    )
    assert score.weight is not None
    assert score.weight > 0
