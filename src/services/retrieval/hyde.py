# src/services/retrieval/hyde.py

from __future__ import annotations

import logging
from typing import Optional

from src.services.llm.router import ModelRouter, TaskType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

HYDE_PROMPT = """You are a private equity analyst writing evidence notes for an investment committee.

A company called {company_id} has been scored on the "{dimension}" dimension of AI-readiness.

Write a SHORT paragraph (3-5 sentences) describing what REAL evidence would look like 
for a company that scores well on "{dimension}". 

Be specific — mention:
- Concrete things the company would be doing (hiring, tools, processes)
- Specific keywords that would appear in SEC filings, job postings, or patents
- Numbers or metrics where relevant

Do NOT mention scores or ratings. Just describe the evidence itself.
Write as if you found this evidence in a real document."""


HYDE_PROMPT_WITH_SCORE = """You are a private equity analyst writing evidence notes for an investment committee.

A company called {company_id} scored {score}/100 on the "{dimension}" dimension of AI-readiness.
This is a Level {level} score which means: {level_name}.

Write a SHORT paragraph (3-5 sentences) describing what evidence would justify this score.

Be specific — mention:
- Concrete things the company is doing (hiring, tools, processes)  
- Specific keywords that would appear in SEC filings, job postings, or patents
- Numbers or metrics where relevant

Do NOT mention scores or ratings. Just describe the evidence itself.
Write as if you found this evidence in a real document."""


# ---------------------------------------------------------------------------
# HyDEQueryEnhancer
# ---------------------------------------------------------------------------

class HyDEQueryEnhancer:
    """
    Hypothetical Document Embeddings (HyDE) query enhancer.

    Instead of searching with a question (which gives mediocre results),
    HyDE asks the LLM to write a fake-but-realistic answer first,
    then uses THAT as the search query.

    This works because:
    - Questions and answers have different embeddings
    - A hypothetical answer is semantically closer to real evidence
    - Vector search finds much better results when searching answer-to-answer

    Usage:
        enhancer = HyDEQueryEnhancer()

        # Basic enhancement
        better_query = await enhancer.enhance(
            query="Why did NVDA score high on Talent?",
            dimension="talent",
            company_id="NVDA",
        )

        # With score context (even better results)
        better_query = await enhancer.enhance_with_score(
            query="Why did NVDA score high on Talent?",
            dimension="talent",
            company_id="NVDA",
            score=87.0,
            level=4,
            level_name="Good",
        )

        # Then pass to HybridRetriever
        results = retriever.search(better_query, company_id="NVDA")
    """

    def __init__(self, router: Optional[ModelRouter] = None) -> None:
        self.router = router or ModelRouter()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def enhance(
        self,
        query: str,
        dimension: str,
        company_id: str,
    ) -> str:
        """
        Enhance a search query using HyDE.

        Asks the LLM to write a hypothetical evidence paragraph,
        then returns that paragraph as the new search query.

        Args:
            query:      Original question e.g. "Why did NVDA score high on Talent?"
            dimension:  The CS3 dimension being searched e.g. "talent"
            company_id: Company ticker e.g. "NVDA"

        Returns:
            Hypothetical document text to use as the new search query.
            Falls back to original query if LLM call fails.
        """
        prompt = HYDE_PROMPT.format(
            company_id=company_id,
            dimension=dimension,
        )

        try:
            response = await self.router.complete(
                task=TaskType.EVIDENCE_EXTRACTION,
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )
            hypothetical = response.choices[0].message.content.strip()
            logger.info(
                f"hyde_enhanced: company={company_id} dimension={dimension} "
                f"original='{query[:50]}' "
                f"hypothetical='{hypothetical[:80]}...'"
            )
            return hypothetical

        except Exception as e:
            logger.warning(f"hyde_enhance_failed: {e} — falling back to original query")
            return query  # fallback to original query

    async def enhance_with_score(
        self,
        query: str,
        dimension: str,
        company_id: str,
        score: float,
        level: int,
        level_name: str,
    ) -> str:
        """
        Enhanced version that also uses the score context.

        When you know the score (e.g. NVDA scored 87 on Talent),
        this generates a more targeted hypothetical document.

        Args:
            query:      Original question
            dimension:  CS3 dimension name
            company_id: Company ticker
            score:      Numeric score 0-100
            level:      Score level 1-5
            level_name: Human label e.g. "Good", "Excellent"

        Returns:
            Hypothetical document text, falls back to original query on error.
        """
        prompt = HYDE_PROMPT_WITH_SCORE.format(
            company_id=company_id,
            dimension=dimension,
            score=score,
            level=level,
            level_name=level_name,
        )

        try:
            response = await self.router.complete(
                task=TaskType.JUSTIFICATION_GENERATION,
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )
            hypothetical = response.choices[0].message.content.strip()
            logger.info(
                f"hyde_enhanced_with_score: company={company_id} "
                f"dimension={dimension} score={score} "
                f"hypothetical='{hypothetical[:80]}...'"
            )
            return hypothetical

        except Exception as e:
            logger.warning(
                f"hyde_enhance_with_score_failed: {e} — falling back to original query"
            )
            return query  # fallback to original query

    async def enhance_batch(
        self,
        queries: list[tuple[str, str, str]],  # (query, dimension, company_id)
    ) -> list[str]:
        """
        Enhance multiple queries at once.

        Args:
            queries: List of (query, dimension, company_id) tuples

        Returns:
            List of enhanced queries in the same order.
        """
        import asyncio
        results = await asyncio.gather(
            *[
                self.enhance(query=q, dimension=d, company_id=c)
                for q, d, c in queries
            ],
            return_exceptions=True,
        )

        # Replace any exceptions with original queries
        enhanced = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"hyde_batch_item_failed index={i}: {result}")
                enhanced.append(queries[i][0])  # fallback to original
            else:
                enhanced.append(result)

        return enhanced