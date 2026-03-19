# tests/test_cs4_justification_generator.py


import os

import pytest

from src.services.integration.cs3_client import CS3Client, Dimension

requires_live_services = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY") and not os.getenv("ANTHROPIC_API_KEY"),
    reason="No LLM API key — live integration tests skipped",
)
from src.services.justification.generator import (
    CitedEvidence,
    JustificationGenerator,
    ScoreJustification,
)
from src.services.llm.router import ModelRouter
from src.services.retrieval.hybrid import HybridRetriever
from src.services.retrieval.dimension_mapper import DimensionMapper


TICKER = "NVDA"
CS3_BASE_URL = "http://localhost:8000"


# ---------------------------------------------------------------------------
# _assess_strength — pure logic, no I/O
# ---------------------------------------------------------------------------

def test_assess_strength_no_evidence():
    assert JustificationGenerator._assess_strength([]) == "weak"


def test_assess_strength_strong():
    evidence = [
        CitedEvidence("e1", "content", "sec", None, 0.9, ["cloud", "data lake"], 0.8),
        CitedEvidence("e2", "content", "sec", None, 0.85, ["ml pipeline", "feature store"], 0.75),
    ]
    assert JustificationGenerator._assess_strength(evidence) == "strong"


def test_assess_strength_moderate_by_confidence():
    evidence = [
        CitedEvidence("e1", "content", "sec", None, 0.65, [], 0.6),
    ]
    # avg_conf=0.65 >= 0.6, avg_matches=0 → moderate
    assert JustificationGenerator._assess_strength(evidence) == "moderate"


def test_assess_strength_moderate_by_matches():
    evidence = [
        CitedEvidence("e1", "content", "sec", None, 0.5, ["cloud"], 0.5),
    ]
    # avg_conf=0.5 < 0.6, avg_matches=1 >= 1 → moderate
    assert JustificationGenerator._assess_strength(evidence) == "moderate"


def test_assess_strength_weak():
    evidence = [
        CitedEvidence("e1", "content", "sec", None, 0.4, [], 0.3),
    ]
    assert JustificationGenerator._assess_strength(evidence) == "weak"


# ---------------------------------------------------------------------------
# _match_to_rubric — pure logic, no I/O
# ---------------------------------------------------------------------------

def test_match_to_rubric_no_rubric_returns_empty():
    from src.services.retrieval.hybrid import RetrievedDocument
    docs = [RetrievedDocument("d1", "some content about cloud", {}, 0.8, "hybrid")]
    result = JustificationGenerator(
        cs3_client=None, retriever=None, router=None
    )._match_to_rubric(docs, None)
    assert result == []


def test_match_to_rubric_keyword_match():
    from src.services.retrieval.hybrid import RetrievedDocument
    from src.services.integration.cs3_client import RubricCriteria, ScoreLevel
    docs = [
        RetrievedDocument(
            "d1", "NVIDIA deploys cloud data lake with ML pipeline",
            {"source_type": "sec_10k_item_1", "confidence": "0.9"}, 0.5, "hybrid"
        ),
        RetrievedDocument(
            "d2", "unrelated content about marketing campaigns",
            {"source_type": "press_release", "confidence": "0.6"}, 0.3, "hybrid"
        ),
    ]
    rubric = RubricCriteria(
        dimension=Dimension.DATA_INFRASTRUCTURE,
        level=ScoreLevel.LEVEL_4,
        criteria_text="Company has cloud data platform",
        keywords=["cloud", "data lake", "ml pipeline"],
        quantitative_thresholds={},
    )
    gen = JustificationGenerator(cs3_client=None, retriever=None, router=None)
    cited = gen._match_to_rubric(docs, rubric)
    assert len(cited) == 1
    assert cited[0].evidence_id == "d1"
    assert "cloud" in cited[0].matched_keywords


def test_match_to_rubric_high_score_included():
    """Doc with no keyword match but score > 0.7 should still be cited."""
    from src.services.retrieval.hybrid import RetrievedDocument
    from src.services.integration.cs3_client import RubricCriteria, ScoreLevel
    docs = [
        RetrievedDocument(
            "d1", "no matching keywords here at all",
            {"source_type": "sec", "confidence": "0.8"}, 0.75, "hybrid"
        ),
    ]
    rubric = RubricCriteria(
        dimension=Dimension.TALENT,
        level=ScoreLevel.LEVEL_3,
        criteria_text="Basic talent",
        keywords=["hiring", "engineers"],
        quantitative_thresholds={},
    )
    gen = JustificationGenerator(cs3_client=None, retriever=None, router=None)
    cited = gen._match_to_rubric(docs, rubric)
    assert len(cited) == 1  # included because score=0.75 > 0.7


# ---------------------------------------------------------------------------
# Full integration — real CS3 + real LLM + seeded ChromaDB
# Requires: server running on port 8000, ticker with Snowflake data
# ---------------------------------------------------------------------------

@requires_live_services
@pytest.mark.asyncio
async def test_generate_justification_returns_score_justification(seeded_retriever, tmp_path):
    """
    Full pipeline test. Connects to real CS3 (port 8000) and real LLM.

    NOTE: If this fails with AttributeError: 'HybridRetriever' object has no attribute 'retrieve'
    → generator.py calls self.retriever.retrieve() but the method is named search().
      Fix: change retriever.retrieve() to retriever.search() in generator.py line ~154.
    """
    router = ModelRouter(daily_limit_usd=2.0)
    generator = JustificationGenerator(
        cs3_client=CS3Client(base_url=CS3_BASE_URL),
        retriever=seeded_retriever,
        router=router,
    )

    justification = await generator.generate_justification(TICKER, Dimension.DATA_INFRASTRUCTURE)

    assert isinstance(justification, ScoreJustification)
    assert justification.company_id == TICKER
    assert 0 <= justification.score <= 100
    assert justification.level in (1, 2, 3, 4, 5)
    assert justification.level_name != ""
    assert isinstance(justification.generated_summary, str)
    assert len(justification.generated_summary.strip()) > 0
    assert justification.evidence_strength in ("strong", "moderate", "weak")
    assert isinstance(justification.supporting_evidence, list)
    assert isinstance(justification.gaps_identified, list)


@requires_live_services
@pytest.mark.asyncio
async def test_generate_justification_summary_is_substantial(seeded_retriever):
    """LLM summary should be at least 50 words for IC use."""
    router = ModelRouter(daily_limit_usd=2.0)
    generator = JustificationGenerator(
        cs3_client=CS3Client(base_url=CS3_BASE_URL),
        retriever=seeded_retriever,
        router=router,
    )

    justification = await generator.generate_justification(TICKER, Dimension.TALENT)
    word_count = len(justification.generated_summary.split())
    assert word_count >= 50, f"Summary too short: {word_count} words"


@requires_live_services
@pytest.mark.asyncio
async def test_generate_justification_level5_has_no_gaps(seeded_retriever):
    """If a company scores Level 5, there should be no gaps."""
    cs3 = CS3Client(base_url=CS3_BASE_URL)
    async with cs3:
        score = await cs3.get_dimension_score(TICKER, Dimension.DATA_INFRASTRUCTURE)

    if score.level.value < 5:
        pytest.skip(f"{TICKER} is Level {score.level.value}, not Level 5 — skip gap test")

    router = ModelRouter(daily_limit_usd=2.0)
    generator = JustificationGenerator(
        cs3_client=cs3,
        retriever=seeded_retriever,
        router=router,
    )
    justification = await generator.generate_justification(TICKER, Dimension.DATA_INFRASTRUCTURE)
    assert justification.gaps_identified == []
