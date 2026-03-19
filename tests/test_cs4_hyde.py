# tests/test_cs4_hyde.py
# Unit tests for HyDEQueryEnhancer.
# All tests mock the LLM — no real API calls, no ChromaDB.

from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

import pytest

from src.services.retrieval.hyde import HyDEQueryEnhancer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_router(response_text: str) -> MagicMock:
    """Build a mock ModelRouter whose complete() returns response_text."""
    mock_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=response_text)
            )
        ]
    )
    router = MagicMock()
    router.complete = AsyncMock(return_value=mock_response)
    return router


# ---------------------------------------------------------------------------
# HyDEQueryEnhancer.enhance()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enhance_returns_llm_response_not_original_query():
    """When LLM succeeds, enhance() returns the hypothetical doc, not the query."""
    hypothetical = "NVDA employs 500 AI engineers with expertise in CUDA and ML pipelines."
    router = _mock_router(hypothetical)
    enhancer = HyDEQueryEnhancer(router=router)

    result = await enhancer.enhance(
        query="Why did NVDA score high on Talent?",
        dimension="talent",
        company_id="NVDA",
    )

    assert result == hypothetical
    router.complete.assert_called_once()


@pytest.mark.asyncio
async def test_enhance_returns_non_empty_string():
    router = _mock_router("Some hypothetical evidence paragraph here.")
    enhancer = HyDEQueryEnhancer(router=router)

    result = await enhancer.enhance(
        query="Why did MSFT score well on AI Governance?",
        dimension="ai_governance",
        company_id="MSFT",
    )

    assert isinstance(result, str)
    assert len(result.strip()) > 0


@pytest.mark.asyncio
async def test_enhance_falls_back_to_original_query_on_llm_error():
    """When LLM raises an exception, enhance() falls back to the raw query."""
    router = MagicMock()
    router.complete = AsyncMock(side_effect=RuntimeError("LLM unavailable"))
    enhancer = HyDEQueryEnhancer(router=router)

    original_query = "Why did NVDA score high on Talent?"
    result = await enhancer.enhance(
        query=original_query,
        dimension="talent",
        company_id="NVDA",
    )

    assert result == original_query


@pytest.mark.asyncio
async def test_enhance_strips_whitespace_from_llm_response():
    """Trailing newlines/spaces in LLM output should be stripped."""
    router = _mock_router("  Hypothetical evidence paragraph.  \n")
    enhancer = HyDEQueryEnhancer(router=router)

    result = await enhancer.enhance(
        query="query",
        dimension="talent",
        company_id="NVDA",
    )

    assert result == "Hypothetical evidence paragraph."


# ---------------------------------------------------------------------------
# HyDEQueryEnhancer.enhance_with_score()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enhance_with_score_returns_llm_response():
    hypothetical = "NVDA has invested $2B in cloud data infrastructure over the past 3 years."
    router = _mock_router(hypothetical)
    enhancer = HyDEQueryEnhancer(router=router)

    result = await enhancer.enhance_with_score(
        query="Why did NVDA score 78 on Data Infrastructure?",
        dimension="data_infrastructure",
        company_id="NVDA",
        score=78.0,
        level=4,
        level_name="Good",
    )

    assert result == hypothetical
    router.complete.assert_called_once()


@pytest.mark.asyncio
async def test_enhance_with_score_falls_back_on_error():
    router = MagicMock()
    router.complete = AsyncMock(side_effect=ConnectionError("API down"))
    enhancer = HyDEQueryEnhancer(router=router)

    original_query = "Why did NVDA score 78 on Data Infrastructure?"
    result = await enhancer.enhance_with_score(
        query=original_query,
        dimension="data_infrastructure",
        company_id="NVDA",
        score=78.0,
        level=4,
        level_name="Good",
    )

    assert result == original_query


# ---------------------------------------------------------------------------
# HyDEQueryEnhancer.enhance_batch()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enhance_batch_returns_one_result_per_query():
    router = _mock_router("Hypothetical evidence text.")
    enhancer = HyDEQueryEnhancer(router=router)

    queries = [
        ("Why talent score high?", "talent", "NVDA"),
        ("Why data infra score low?", "data_infrastructure", "MSFT"),
        ("Why governance score medium?", "ai_governance", "NVDA"),
    ]
    results = await enhancer.enhance_batch(queries)

    assert len(results) == 3


@pytest.mark.asyncio
async def test_enhance_batch_falls_back_for_failed_items():
    """If one item fails, that item returns the original query; others succeed."""
    call_count = 0

    async def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("LLM error on second call")
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="Hypothetical doc"))]
        )

    router = MagicMock()
    router.complete = AsyncMock(side_effect=side_effect)
    enhancer = HyDEQueryEnhancer(router=router)

    queries = [
        ("query A", "talent", "NVDA"),
        ("query B", "data_infrastructure", "MSFT"),  # this one fails
        ("query C", "ai_governance", "NVDA"),
    ]
    results = await enhancer.enhance_batch(queries)

    assert len(results) == 3
    assert results[0] == "Hypothetical doc"
    assert results[1] == "query B"          # fallback to original
    assert results[2] == "Hypothetical doc"


@pytest.mark.asyncio
async def test_enhance_batch_empty_returns_empty():
    router = _mock_router("anything")
    enhancer = HyDEQueryEnhancer(router=router)

    results = await enhancer.enhance_batch([])
    assert results == []
