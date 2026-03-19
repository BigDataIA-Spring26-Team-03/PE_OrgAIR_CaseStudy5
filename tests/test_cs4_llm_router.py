# tests/test_cs4_llm_router.py

import os
from decimal import Decimal

import pytest

from src.services.llm.router import DailyBudget, ModelRouter, TaskType

requires_llm_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY") and not os.getenv("ANTHROPIC_API_KEY"),
    reason="No LLM API key set (OPENAI_API_KEY or ANTHROPIC_API_KEY required)",
)


# ---------------------------------------------------------------------------
# DailyBudget — pure logic, no API calls
# ---------------------------------------------------------------------------

def test_daily_budget_starts_at_zero():
    budget = DailyBudget()
    assert budget.spent_usd == Decimal("0")


def test_daily_budget_default_limit_is_two():
    budget = DailyBudget()
    assert budget.limit_usd == Decimal("2.00")


def test_daily_budget_can_spend_small_amount():
    budget = DailyBudget()
    assert budget.can_spend(Decimal("0.001")) is True


def test_daily_budget_cannot_spend_over_limit():
    budget = DailyBudget()
    assert budget.can_spend(Decimal("999")) is False


def test_daily_budget_remaining_equals_limit_at_start():
    budget = DailyBudget()
    assert budget.budget_remaining == Decimal("2.00")


@pytest.mark.asyncio
async def test_daily_budget_record_spend_reduces_remaining():
    budget = DailyBudget()
    await budget.record_spend(Decimal("0.50"))
    assert budget.spent_usd == Decimal("0.50")
    assert budget.budget_remaining == Decimal("1.50")


@pytest.mark.asyncio
async def test_daily_budget_cannot_spend_after_exhausted():
    budget = DailyBudget()
    await budget.record_spend(Decimal("1.99"))
    # Only $0.01 left — a $0.02 request should fail
    assert budget.can_spend(Decimal("0.02")) is False


# ---------------------------------------------------------------------------
# ModelRouter — instantiation
# ---------------------------------------------------------------------------

def test_model_router_default_limit():
    router = ModelRouter(daily_limit_usd=2.0)
    assert router.daily_budget.limit_usd == Decimal("2.0")


def test_model_router_custom_limit():
    router = ModelRouter(daily_limit_usd=0.50)
    assert router.daily_budget.limit_usd == Decimal("0.50")


# ---------------------------------------------------------------------------
# _estimate_cost — no API call
# ---------------------------------------------------------------------------

def test_estimate_cost_returns_positive():
    router = ModelRouter(daily_limit_usd=2.0)
    from src.services.llm.router import MODEL_ROUTING
    config = MODEL_ROUTING[TaskType.CHAT_RESPONSE]
    messages = [{"role": "user", "content": "Hello, this is a test message for cost estimation."}]
    cost = router._estimate_cost(messages, config)
    assert cost > Decimal("0")


def test_estimate_cost_empty_message():
    router = ModelRouter(daily_limit_usd=2.0)
    from src.services.llm.router import MODEL_ROUTING
    config = MODEL_ROUTING[TaskType.CHAT_RESPONSE]
    # Even with empty content, max_tokens drives a minimum cost
    cost = router._estimate_cost([{"role": "user", "content": ""}], config)
    assert cost >= Decimal("0")


# ---------------------------------------------------------------------------
# Real API call 
# ---------------------------------------------------------------------------

@requires_llm_key
@pytest.mark.asyncio
async def test_real_llm_completion_returns_text():
    """
    Makes ONE real LLM call using claude-haiku (cheapest model, ~$0.0001).
    Verifies: routing works, API key is valid, response is non-empty.
    """
    router = ModelRouter(daily_limit_usd=2.0)
    response = await router.complete(
        TaskType.CHAT_RESPONSE,
        [{"role": "user", "content": "Reply with just the single word: OK"}],
    )
    text = response.choices[0].message.content
    assert text is not None
    assert len(text.strip()) > 0


@requires_llm_key
@pytest.mark.asyncio
async def test_real_llm_call_records_spend():
    """After a real call, spent_usd should be > 0."""
    router = ModelRouter(daily_limit_usd=2.0)
    await router.complete(
        TaskType.CHAT_RESPONSE,
        [{"role": "user", "content": "Say: pong"}],
    )
    assert router.daily_budget.spent_usd > Decimal("0")


@pytest.mark.asyncio
async def test_budget_blocks_when_exhausted():
    """If budget is set to $0, all calls should raise RuntimeError."""
    router = ModelRouter(daily_limit_usd=0.0)
    with pytest.raises(RuntimeError, match="All models failed"):
        await router.complete(
            TaskType.CHAT_RESPONSE,
            [{"role": "user", "content": "This should be blocked by budget"}],
        )
