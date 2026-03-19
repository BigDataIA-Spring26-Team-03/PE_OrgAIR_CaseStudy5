# src/services/llm/router.py


from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional

import structlog
from litellm import acompletion

logger = structlog.get_logger()


def _configure_litellm() -> None:
    """Propagate env vars into os.environ so LiteLLM can read them."""
    try:
        from app.config import settings
        if settings.OPENAI_API_KEY:
            os.environ.setdefault("OPENAI_API_KEY", settings.OPENAI_API_KEY)
        if settings.ANTHROPIC_API_KEY:
            os.environ.setdefault("ANTHROPIC_API_KEY", settings.ANTHROPIC_API_KEY)
    except Exception:
        pass  # config unavailable in test environments; rely on env vars directly


_configure_litellm()


# ---------------------------------------------------------------------------
# Enums & types
# ---------------------------------------------------------------------------

class TaskType(str, Enum):
    EVIDENCE_EXTRACTION      = "evidence_extraction"
    DIMENSION_SCORING        = "dimension_scoring"
    JUSTIFICATION_GENERATION = "justification_generation"
    CHAT_RESPONSE            = "chat_response"


# ---------------------------------------------------------------------------
# ModelConfig
# ---------------------------------------------------------------------------

@dataclass
class ModelConfig:
    primary: str
    fallbacks: List[str]
    temperature: float
    max_tokens: int
    cost_per_1k_tokens: float
    #Pre-baked system prompt injected automatically per task
    system_prompt: str = ""


# ---------------------------------------------------------------------------
# Routing table  (exact models from CLAUDE.md spec)
# ---------------------------------------------------------------------------

MODEL_ROUTING: Dict[TaskType, ModelConfig] = {
    TaskType.EVIDENCE_EXTRACTION: ModelConfig(
        primary="gpt-4o-mini",
        fallbacks=["claude-haiku-4-5-20251001"],
        temperature=0.3,
        max_tokens=2000,
        cost_per_1k_tokens=0.000150,
        system_prompt=(
            "You are a private equity research analyst specializing in AI/technology "
            "due diligence. Extract structured evidence from documents with precision. "
            "Cite specific passages and quantify confidence where possible."
        ),
    ),
    TaskType.DIMENSION_SCORING: ModelConfig(
        primary="gpt-4o-mini",
        fallbacks=["claude-haiku-4-5-20251001"],
        temperature=0.2,
        max_tokens=1000,
        cost_per_1k_tokens=0.000150,
        system_prompt=(
            "You are a rigorous PE scoring analyst. Evaluate evidence against rubric "
            "criteria and assign scores with clear justification. Be objective and "
            "consistent across companies."
        ),
    ),
    TaskType.JUSTIFICATION_GENERATION: ModelConfig(
        primary="claude-haiku-4-5-20251001",
        fallbacks=["gpt-4o-mini"],
        temperature=0.2,
        max_tokens=1000,
        cost_per_1k_tokens=0.000250,
        system_prompt=(
            "You are a senior PE investment analyst preparing materials for an "
            "Investment Committee. Write concise, evidence-backed score justifications "
            "(150-200 words). Always cite specific evidence. Highlight gaps that could "
            "move the score to the next level."
        ),
    ),
    TaskType.CHAT_RESPONSE: ModelConfig(
        primary="claude-haiku-4-5-20251001",
        fallbacks=["gpt-4o-mini"],
        temperature=0.7,
        max_tokens=500,
        cost_per_1k_tokens=0.000250,
        system_prompt=(
            "You are a helpful assistant for the PE OrgAIR platform. Answer questions "
            "about company assessments, scores, and evidence clearly and concisely."
        ),
    ),
}


# ---------------------------------------------------------------------------
# DailyBudget 
# ---------------------------------------------------------------------------

@dataclass
class DailyBudget:
    date: date = field(default_factory=date.today)
    spent_usd: Decimal = Decimal("0")
    limit_usd: Decimal = Decimal("2.00")
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False, compare=False)

    def _reset_if_new_day(self) -> None:
        if self.date != date.today():
            self.date = date.today()
            self.spent_usd = Decimal("0")

    def can_spend(self, amount: Decimal) -> bool:
        self._reset_if_new_day()
        return self.spent_usd + amount <= self.limit_usd

    async def record_spend(self, amount: Decimal) -> None:
        """Thread-safe spend recording via asyncio.Lock."""
        async with self._lock:
            self._reset_if_new_day()
            self.spent_usd += amount

    @property
    def budget_remaining(self) -> Decimal:
        self._reset_if_new_day()
        return self.limit_usd - self.spent_usd


# ---------------------------------------------------------------------------
# ModelRouter
# ---------------------------------------------------------------------------

class ModelRouter:
    """
    Route LLM completion requests across providers with:
    - Automatic fallback on provider failure
    - Daily budget enforcement
    - Pre-baked system prompts per task type
    - Structured cost + token logging
    """

    def __init__(self, daily_limit_usd: Optional[float] = None) -> None:
        if daily_limit_usd is None:
            try:
                from app.config import settings
                daily_limit_usd = settings.LITELLM_BUDGET_USD_PER_DAY
            except Exception:
                daily_limit_usd = 2.0
        self.daily_budget = DailyBudget(limit_usd=Decimal(str(daily_limit_usd)))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def complete(
        self,
        task: TaskType,
        messages: List[Dict[str, str]],
        stream: bool = False,
        **kwargs: Any,
    ) -> Any:
        """
        Route a completion request with automatic fallback.

        """
        config = MODEL_ROUTING[task]
        prepared = self._inject_system_prompt(messages, config)

        for model in [config.primary] + config.fallbacks:
            # Pre-call budget gate
            estimated = self._estimate_cost(prepared, config)
            if not self.daily_budget.can_spend(estimated):
                logger.warning(
                    "llm_budget_exceeded",
                    task=task.value,
                    model=model,
                    estimated_usd=float(estimated),
                    budget_remaining=float(self.daily_budget.budget_remaining),
                )
                continue

            try:
                if stream:
                    return self._stream_complete(model, prepared, config, **kwargs)

                response = await acompletion(
                    model=model,
                    messages=prepared,
                    temperature=config.temperature,
                    max_tokens=config.max_tokens,
                    **kwargs,
                )

                # Record actual spend from response token usage
                await self._record_response_cost(response, config, task, model)
                return response

            except Exception as exc:
                logger.warning(
                    "llm_fallback",
                    task=task.value,
                    failed_model=model,
                    reason=str(exc),
                )

        raise RuntimeError(
            f"All models failed for task '{task.value}'. "
            f"Tried: {[config.primary] + config.fallbacks}"
        )

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    async def _stream_complete(
        self,
        model: str,
        messages: List[Dict[str, str]],
        config: ModelConfig,
        **kwargs: Any,
    ) -> AsyncIterator[str]:
        """Async generator that yields content chunks from a streaming response."""
        estimated = self._estimate_cost(messages, config)
        if not self.daily_budget.can_spend(estimated):
            raise RuntimeError(
                f"Daily budget exhausted. Remaining: ${self.daily_budget.budget_remaining:.4f}"
            )

        response = await acompletion(
            model=model,
            messages=messages,
            stream=True,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            **kwargs,
        )

        total_chars = 0
        async for chunk in response:
            content = chunk.choices[0].delta.content
            if content:
                total_chars += len(content)
                yield content

        # Approximate spend recording for streamed responses (no token count available)
        approx_tokens = Decimal(str(total_chars // 4))
        approx_cost = approx_tokens * Decimal(str(config.cost_per_1k_tokens)) / Decimal("1000")
        await self.daily_budget.record_spend(approx_cost)

        logger.info(
            "llm_stream_complete",
            task="stream",
            model=model,
            approx_tokens=int(approx_tokens),
            cost_usd=float(approx_cost),
            budget_remaining=float(self.daily_budget.budget_remaining),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _inject_system_prompt(
        messages: List[Dict[str, str]], config: ModelConfig
    ) -> List[Dict[str, str]]:
        """Prepend task system prompt if the caller hasn't already provided one."""
        if not config.system_prompt:
            return messages
        if messages and messages[0].get("role") == "system":
            return messages  # caller-supplied system message takes precedence
        return [{"role": "system", "content": config.system_prompt}] + messages

    @staticmethod
    def _estimate_cost(
        messages: List[Dict[str, str]], config: ModelConfig
    ) -> Decimal:
        """
        Rough pre-call cost estimate.  Uses ~4 chars/token heuristic.
        Adds max_tokens as upper bound for completion tokens.
        """
        prompt_chars = sum(len(m.get("content", "")) for m in messages)
        prompt_tokens = Decimal(str(prompt_chars // 4))
        completion_tokens = Decimal(str(config.max_tokens))
        total_tokens = prompt_tokens + completion_tokens
        return total_tokens * Decimal(str(config.cost_per_1k_tokens)) / Decimal("1000")

    async def _record_response_cost(
        self,
        response: Any,
        config: ModelConfig,
        task: TaskType,
        model: str,
    ) -> None:
        """Extract token usage from response and record actual spend."""
        try:
            usage = response.usage
            total_tokens = Decimal(str(usage.total_tokens))
            cost = total_tokens * Decimal(str(config.cost_per_1k_tokens)) / Decimal("1000")
            await self.daily_budget.record_spend(cost)

            logger.info(
                "llm_complete",
                task=task.value,
                model=model,
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
                cost_usd=float(cost),
                budget_remaining=float(self.daily_budget.budget_remaining),
            )
        except Exception:
            # Usage extraction is best-effort; don't fail the caller
            logger.debug("llm_usage_unavailable", model=model)
