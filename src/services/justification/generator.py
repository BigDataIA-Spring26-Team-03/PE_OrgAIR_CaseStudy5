# src/services/justification/generator.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from src.services.integration.cs3_client import (
    CS3Client,
    Dimension,
    DimensionScore,
    RubricCriteria,
    ScoreLevel,
)
from src.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from src.services.retrieval.hyde import HyDEQueryEnhancer
from src.services.llm.router import ModelRouter, TaskType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CitedEvidence:
    """A single piece of evidence with citation details for IC presentation."""
    evidence_id: str
    content: str                  # truncated to 500 chars
    source_type: str
    source_url: Optional[str]
    confidence: float
    matched_keywords: List[str]
    relevance_score: float


@dataclass
class ScoreJustification:
    """
    Complete score justification for an IC presentation.

    Produced by JustificationGenerator.generate_justification() and
    consumed by ICPrepWorkflow and the Justification API endpoint.
    """
    company_id: str
    dimension: Dimension
    score: float
    level: int
    level_name: str
    confidence_interval: tuple

    rubric_criteria: str
    rubric_keywords: List[str]

    supporting_evidence: List[CitedEvidence]
    gaps_identified: List[str]

    generated_summary: str
    evidence_strength: str        # "strong", "moderate", or "weak"


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

JUSTIFICATION_PROMPT = """You are a PE analyst preparing score justification for an investment committee.

COMPANY: {company_id}
DIMENSION: {dimension}
SCORE: {score}/100 (Level {level} - {level_name})

RUBRIC CRITERIA FOR THIS LEVEL:
{rubric_criteria}

RUBRIC KEYWORDS TO MATCH:
{rubric_keywords}

SUPPORTING EVIDENCE FOUND:
{evidence_text}

Generate a concise IC-ready justification (150-200 words) that:
1. States why the score matches this rubric level
2. Cites specific evidence with source references [Source Type, FY]
3. Identifies gaps preventing a higher score
4. Assesses evidence strength (strong/moderate/weak)

Write in professional PE memo style."""


# ---------------------------------------------------------------------------
# JustificationGenerator
# ---------------------------------------------------------------------------

class JustificationGenerator:
    """
    Generate score justifications with cited evidence for IC presentations.

    Flow:
        1. Fetch dimension score from CS3
        2. Fetch rubric criteria for that level
        3. Enhance search query using HyDE (Hypothetical Document Embeddings)
        4. Search evidence via HybridRetriever
        5. Match evidence to rubric keywords → CitedEvidence
        6. Identify gaps from next-level rubric
        7. Generate LLM summary in PE memo style

    Usage:
        generator = JustificationGenerator()
        justification = await generator.generate_justification("NVDA", Dimension.DATA_INFRASTRUCTURE)
    """

    def __init__(
        self,
        cs3_client: Optional[CS3Client] = None,
        retriever: Optional[HybridRetriever] = None,
        router: Optional[ModelRouter] = None,
    ) -> None:
        self.cs3 = cs3_client or CS3Client()
        self.retriever = retriever or HybridRetriever()
        self.router = router or ModelRouter()
        self.hyde = HyDEQueryEnhancer(router=self.router)

    async def generate_justification(
        self,
        company_id: str,
        dimension: Dimension,
    ) -> ScoreJustification:
        """
        Generate a full score justification for one dimension.

        Args:
            company_id: Company ticker e.g. "NVDA"
            dimension:  The CS3 dimension to justify e.g. Dimension.DATA_INFRASTRUCTURE

        Returns:
            ScoreJustification ready for IC presentation.
        """
        # 1. Fetch score from CS3
        async with CS3Client() as cs3:
            score = await cs3.get_dimension_score(company_id, dimension)

            # 2. Get rubric for that level
            rubrics = await cs3.get_rubric(dimension, score.level)

        rubric = rubrics[0] if rubrics else None

        # 3. Build base query from rubric keywords + dimension name
        if rubric and rubric.keywords:
            base_query = " ".join(rubric.keywords[:5]) + " " + dimension.value.replace("_", " ")
        else:
            base_query = dimension.value.replace("_", " ")

        # 3b. Enhance query using HyDE — generates a hypothetical evidence
        #     paragraph which is semantically closer to real evidence than
        #     a keyword query. Falls back to base_query if LLM call fails.
        query = await self.hyde.enhance_with_score(
            query=base_query,
            dimension=dimension.value,
            company_id=company_id,
            score=score.score,
            level=score.level.value,
            level_name=score.level.name_label,
        )

        logger.info(
            f"hyde_query: company={company_id} dimension={dimension.value} "
            f"base='{base_query[:60]}' enhanced='{query[:80]}'"
        )

        # 4. Search for evidence using hybrid retrieval
        # No dimension filter — evidence is tagged by primary signal category,
        # not by the dimension being justified. Rely on semantic similarity.
        results = self.retriever.search(
            query=query,
            top_k=15,
            company_id=company_id,
        )

        # 5. Match evidence to rubric keywords → CitedEvidence
        cited = self._match_to_rubric(results, rubric)

        # 6. Identify gaps from next-level rubric
        gaps = await self._identify_gaps(company_id, dimension, score.level, results)

        # 7. Generate LLM summary
        evidence_text = "\n".join([
            f"[{e.source_type}, conf={e.confidence:.2f}] {e.content[:300]}..."
            for e in cited[:5]
        ]) or "No direct keyword matches found — score based on semantic similarity to rubric criteria."

        response = await self.router.complete(
            task=TaskType.JUSTIFICATION_GENERATION,
            messages=[{
                "role": "user",
                "content": JUSTIFICATION_PROMPT.format(
                    company_id=company_id,
                    dimension=dimension.value.replace("_", " ").title(),
                    score=score.score,
                    level=score.level.value,
                    level_name=score.level.name_label,
                    rubric_criteria=rubric.criteria_text if rubric else "N/A",
                    rubric_keywords=", ".join(rubric.keywords) if rubric else "N/A",
                    evidence_text=evidence_text,
                ),
            }],
        )

        summary = response.choices[0].message.content
        strength = self._assess_strength(cited)

        return ScoreJustification(
            company_id=company_id,
            dimension=dimension,
            score=score.score,
            level=score.level.value,
            level_name=score.level.name_label,
            confidence_interval=score.confidence_interval,
            rubric_criteria=rubric.criteria_text if rubric else "",
            rubric_keywords=rubric.keywords if rubric else [],
            supporting_evidence=cited,
            gaps_identified=gaps,
            generated_summary=summary,
            evidence_strength=strength,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _match_to_rubric(
        self,
        results: List[RetrievedDocument],
        rubric: Optional[RubricCriteria],
    ) -> List[CitedEvidence]:
        """
        Match retrieved documents to rubric keywords.

        A document is cited if:
        - Any individual word from a rubric keyword phrase appears in content, OR
        - Its RRF relevance score is > 0.003 (accounts for RRF score scale ~0.009)
        """
        if not rubric:
            return []

        cited = []
        for r in results:
            # Split multi-word keywords so "data science team" matches on "data", "science", or "team"
            matched = [
                kw for kw in rubric.keywords
                if any(word in r.content.lower() for word in kw.lower().split())
            ]
            # Lower score threshold — RRF scores are always small (~0.003–0.01)
            if matched or r.score > 0.5:
                cited.append(CitedEvidence(
                    evidence_id=r.doc_id,
                    content=r.content[:500],
                    source_type=r.metadata.get("source_type", "unknown"),
                    source_url=r.metadata.get("source_url"),
                    confidence=float(r.metadata.get("confidence", 0.5)),
                    matched_keywords=matched,
                    relevance_score=r.score,
                ))

        # Sort by number of keyword matches descending, return top 5
        return sorted(cited, key=lambda x: len(x.matched_keywords), reverse=True)[:5]

    async def _identify_gaps(
        self,
        company_id: str,
        dimension: Dimension,
        current_level: ScoreLevel,
        evidence: List[RetrievedDocument],
    ) -> List[str]:
        """
        Find criteria from the next rubric level not matched in current evidence.

        Returns up to 5 gap strings like:
            "No evidence of 'data mesh' (Level 5 criterion)"
        """
        if current_level == ScoreLevel.LEVEL_5:
            return []   # already at top level

        next_level = ScoreLevel(current_level.value + 1)

        async with CS3Client() as cs3:
            next_rubrics = await cs3.get_rubric(dimension, next_level)

        if not next_rubrics:
            return []

        next_rubric = next_rubrics[0]
        evidence_text = " ".join(e.content.lower() for e in evidence)

        gaps = []
        for kw in next_rubric.keywords:
            if kw.lower() not in evidence_text:
                gaps.append(
                    f"No evidence of '{kw}' (Level {next_level.value} criterion)"
                )

        return gaps[:5]

    @staticmethod
    def _assess_strength(evidence: List[CitedEvidence]) -> str:
        """
        Assess overall evidence strength based on confidence and keyword matches.

        Returns "strong", "moderate", or "weak".
        """
        if not evidence:
            return "weak"

        avg_conf = sum(e.confidence for e in evidence) / len(evidence)
        avg_matches = sum(len(e.matched_keywords) for e in evidence) / len(evidence)

        if avg_conf >= 0.8 and avg_matches >= 2:
            return "strong"
        elif avg_conf >= 0.6 or avg_matches >= 1:
            return "moderate"
        return "weak"