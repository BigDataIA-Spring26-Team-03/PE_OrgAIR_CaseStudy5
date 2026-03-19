# tests/test_cs4_dimension_mapper.py
# Pure unit tests — no I/O, no server, no cost. Runs in < 1 second.

import pytest

from src.scoring.evidence_mapper import Dimension
from src.services.integration.cs2_client import SignalCategory, SourceType
from src.services.retrieval.dimension_mapper import DimensionMapper


@pytest.fixture
def mapper():
    return DimensionMapper()


# ---------------------------------------------------------------------------
# Primary dimension correctness
# ---------------------------------------------------------------------------

def test_digital_presence_primary(mapper):
    assert mapper.get_primary_dimension(SignalCategory.DIGITAL_PRESENCE) == Dimension.DATA_INFRASTRUCTURE


def test_technology_hiring_primary(mapper):
    assert mapper.get_primary_dimension(SignalCategory.TECHNOLOGY_HIRING) == Dimension.TALENT


def test_governance_signals_primary(mapper):
    assert mapper.get_primary_dimension(SignalCategory.GOVERNANCE_SIGNALS) == Dimension.AI_GOVERNANCE


def test_culture_signals_primary(mapper):
    assert mapper.get_primary_dimension(SignalCategory.CULTURE_SIGNALS) == Dimension.CULTURE


def test_innovation_activity_primary(mapper):
    assert mapper.get_primary_dimension(SignalCategory.INNOVATION_ACTIVITY) == Dimension.TECHNOLOGY_STACK


def test_leadership_signals_primary(mapper):
    assert mapper.get_primary_dimension(SignalCategory.LEADERSHIP_SIGNALS) == Dimension.LEADERSHIP


# ---------------------------------------------------------------------------
# Specific weight values
# ---------------------------------------------------------------------------

def test_digital_presence_weight(mapper):
    weights = mapper.get_dimension_weights(SignalCategory.DIGITAL_PRESENCE)
    assert abs(weights[Dimension.DATA_INFRASTRUCTURE] - 0.60) < 0.01


def test_governance_signals_weights(mapper):
    weights = mapper.get_dimension_weights(SignalCategory.GOVERNANCE_SIGNALS)
    assert abs(weights[Dimension.AI_GOVERNANCE] - 0.70) < 0.01
    assert abs(weights[Dimension.LEADERSHIP] - 0.30) < 0.01


def test_culture_signals_weights(mapper):
    weights = mapper.get_dimension_weights(SignalCategory.CULTURE_SIGNALS)
    assert abs(weights[Dimension.CULTURE] - 0.80) < 0.01
    assert abs(weights[Dimension.TALENT] - 0.10) < 0.01
    assert abs(weights[Dimension.LEADERSHIP] - 0.10) < 0.01


# ---------------------------------------------------------------------------
# All weights sum to 1.0 for every category
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("category", list(SignalCategory))
def test_weights_sum_to_one(mapper, category):
    weights = mapper.get_dimension_weights(category)
    if not weights:
        pytest.skip(f"No weights defined for {category}")
    total = sum(weights.values())
    assert abs(total - 1.0) < 0.01, f"{category}: weights sum to {total}, expected 1.0"


# ---------------------------------------------------------------------------
# get_all_dimensions_for_evidence — sorted descending
# ---------------------------------------------------------------------------

def test_all_dimensions_sorted_descending(mapper):
    dims = mapper.get_all_dimensions_for_evidence(SignalCategory.TECHNOLOGY_HIRING)
    weights = [w for _, w in dims]
    assert weights == sorted(weights, reverse=True), "Should be sorted descending by weight"


def test_all_dimensions_first_is_primary(mapper):
    dims = mapper.get_all_dimensions_for_evidence(SignalCategory.DIGITAL_PRESENCE)
    assert dims[0][0] == Dimension.DATA_INFRASTRUCTURE


# ---------------------------------------------------------------------------
# SourceType → SignalCategory mapping
# ---------------------------------------------------------------------------

def test_sec_10k_item1_maps_to_digital_presence(mapper):
    assert mapper.source_type_to_category(SourceType.SEC_10K_ITEM_1) == SignalCategory.DIGITAL_PRESENCE


def test_glassdoor_maps_to_culture(mapper):
    assert mapper.source_type_to_category(SourceType.GLASSDOOR_REVIEW) == SignalCategory.CULTURE_SIGNALS


def test_linkedin_maps_to_technology_hiring(mapper):
    assert mapper.source_type_to_category(SourceType.JOB_POSTING_LINKEDIN) == SignalCategory.TECHNOLOGY_HIRING


def test_patent_maps_to_innovation(mapper):
    assert mapper.source_type_to_category(SourceType.PATENT_USPTO) == SignalCategory.INNOVATION_ACTIVITY


def test_board_proxy_maps_to_governance(mapper):
    assert mapper.source_type_to_category(SourceType.BOARD_PROXY_DEF14A) == SignalCategory.GOVERNANCE_SIGNALS


def test_sec_10k_item1a_maps_to_governance(mapper):
    assert mapper.source_type_to_category(SourceType.SEC_10K_ITEM_1A) == SignalCategory.GOVERNANCE_SIGNALS


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_unknown_category_returns_empty(mapper):
    # Passing None should not crash, return empty
    result = mapper.get_dimension_weights(None)  # type: ignore
    assert result == {}


def test_unknown_source_type_returns_none(mapper):
    result = mapper.source_type_to_category(None)  # type: ignore
    assert result is None
