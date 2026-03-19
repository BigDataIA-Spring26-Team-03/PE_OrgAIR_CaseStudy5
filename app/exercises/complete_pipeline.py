"""
Complete Pipeline Exercise: Why did NVIDIA score on Data Infrastructure?

Prerequisites: FastAPI running on port 8000 with NVDA scored
Run with: python exercises/complete_pipeline.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import httpx
from src.services.integration.cs1_client import CS1Client
from src.services.retrieval.hybrid import HybridRetriever
from src.services.retrieval.dimension_mapper import DimensionMapper
from src.services.llm.router import ModelRouter, TaskType
from src.services.integration.cs3_client import (
    Dimension, ScoreLevel, DimensionScore, RubricCriteria
)
from src.services.justification.generator import (
    CitedEvidence, ScoreJustification, JUSTIFICATION_PROMPT
)

BASE_URL  = "http://localhost:8000"
TICKER    = "NVDA"
DIM_KEY   = "data_infrastructure"
DIMENSION = Dimension.DATA_INFRASTRUCTURE

# ---------------------------------------------------------------------------
# Hardcoded rubric 
# ---------------------------------------------------------------------------
RUBRIC_BY_LEVEL = {
    4: RubricCriteria(
        dimension=DIMENSION,
        level=ScoreLevel.LEVEL_4,
        criteria_text=(
            "Hybrid cloud infrastructure with 70-90% data quality metrics. "
            "Centralized data platform, real-time pipelines, and structured ML feature store. "
            "Data governance policies in place with measurable SLAs."
        ),
        keywords=[
            "cloud", "data pipeline", "data quality", "data lake",
            "feature store", "real-time", "governance", "ETL", "warehouse",
        ],
    ),
    3: RubricCriteria(
        dimension=DIMENSION,
        level=ScoreLevel.LEVEL_3,
        criteria_text=(
            "Basic data infrastructure with some cloud adoption. "
            "Ad-hoc pipelines, limited data quality controls."
        ),
        keywords=["cloud", "database", "pipeline", "analytics"],
    ),
    5: RubricCriteria(
        dimension=DIMENSION,
        level=ScoreLevel.LEVEL_5,
        criteria_text=(
            "Best-in-class data mesh architecture, >90% data quality, "
            "automated data contracts, real-time streaming at scale."
        ),
        keywords=[
            "data mesh", "streaming", "data contract", "MLOps",
            "lakehouse", "automated", "observability",
        ],
    ),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def fetch_scoring_results(ticker: str) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{BASE_URL}/api/v1/scoring/results/{ticker}")
        r.raise_for_status()
        return r.json()


def build_mock_evidence(company_id: str) -> list:
    """
    Return a small set of realistic mock evidence items since
    /api/v1/evidence is not implemented in this app.
    """
    from src.services.integration.cs2_client import (
        CS2Evidence, SourceType, SignalCategory
    )
    from datetime import datetime
    return [
        CS2Evidence(
            evidence_id="mock-001",
            company_id=company_id,
            source_type=SourceType.SEC_10K_ITEM_1,
            signal_category=SignalCategory.DIGITAL_PRESENCE,
            content=(
                "NVIDIA operates a hybrid cloud data infrastructure spanning AWS and "
                "Azure. Our data pipeline processes over 10TB daily with automated "
                "quality checks achieving 87% data quality scores. We maintain a "
                "centralized data lake for ML feature store management."
            ),
            extracted_at=datetime(2024, 3, 1),
            confidence=0.9,
            fiscal_year=2024,
        ),
        CS2Evidence(
            evidence_id="mock-002",
            company_id=company_id,
            source_type=SourceType.JOB_POSTING_LINKEDIN,
            signal_category=SignalCategory.TECHNOLOGY_HIRING,
            content=(
                "Senior Data Engineer – Build and maintain real-time ETL pipelines. "
                "Experience with data warehouse (Snowflake/BigQuery), cloud platforms, "
                "and governance tooling required."
            ),
            extracted_at=datetime(2024, 4, 1),
            confidence=0.85,
            fiscal_year=2024,
        ),
        CS2Evidence(
            evidence_id="mock-003",
            company_id=company_id,
            source_type=SourceType.SEC_10K_ITEM_7,
            signal_category=SignalCategory.LEADERSHIP_SIGNALS,
            content=(
                "Management has prioritized investment in data infrastructure, "
                "allocating $500M toward cloud migration and governance in FY2024. "
                "Data quality SLA targets were met in 3 of 4 quarters."
            ),
            extracted_at=datetime(2024, 3, 1),
            confidence=0.88,
            fiscal_year=2024,
        ),
    ]


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_pipeline():
    print("=" * 60)
    print(f"EXERCISE: {TICKER} Data Infrastructure Score Justification")
    print("=" * 60)

    # Step 1 — Company lookup
    print("\n[Step 1] Fetching company from CS1...")
    async with CS1Client() as cs1:
        try:
            company = await cs1.get_company(TICKER)
            print(f"  Company : {company.name}")
            print(f"  Ticker  : {company.ticker}")
            print(f"  ID      : {company.company_id}")
        except Exception as e:
            print(f"  ERROR: {e}")
            return

    # Step 2 — Scoring results
    print(f"\n[Step 2] Fetching scoring results...")
    try:
        results = await fetch_scoring_results(TICKER)
        print(f"  Org-AI-R : {results.get('final_score', 0):.1f}")
        print(f"  V^R      : {results.get('vr_score', 0):.1f}")
        print(f"  H^R      : {results.get('hr_score', 0):.1f}")
        print(f"  Synergy  : {results.get('synergy_score', 0):.1f}")
        conf = results.get("confidence", {})
        print(f"  CI 95%   : [{conf.get('ci_lower', 0):.1f}, {conf.get('ci_upper', 0):.1f}]")
        print(f"  Reliability: {conf.get('reliability', 0):.2f}")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"  ERROR: {e}")
        return

    # Step 3 — Extract dimension score
    print(f"\n[Step 3] Extracting {DIM_KEY} score...")
    dim_data = results.get("dimension_scores", {}).get(DIM_KEY)
    if not dim_data:
        print(f"  ERROR: '{DIM_KEY}' not in results. Available: {list(results.get('dimension_scores',{}).keys())}")
        return

    dim_val = float(dim_data.get("score", 0))
    level   = ScoreLevel.from_score(dim_val)
    print(f"  Score : {dim_val:.1f} / 100")
    print(f"  Level : {level.value} ({level.name_label})")

    conf = results.get("confidence", {})
    score_obj = DimensionScore(
        dimension=DIMENSION,
        score=dim_val,
        level=level,
        confidence=float(conf.get("reliability", 0.8)),
        confidence_interval=(conf.get("ci_lower", dim_val - 6), conf.get("ci_upper", dim_val + 6)),
        evidence_count=int(conf.get("evidence_count", 0)),
        last_updated=results.get("scored_at", ""),
    )
    # Step 4 — Rubric
    print(f"\n[Step 4] Loading rubric for Level {level.value}...")
    rubric = RUBRIC_BY_LEVEL.get(level.value, RUBRIC_BY_LEVEL[4])
    print(f"  Criteria : {rubric.criteria_text[:100]}...")
    print(f"  Keywords : {rubric.keywords[:5]}")

    # Step 5 — Index evidence 
    print(f"\n[Step 5] Building and indexing evidence...")
    evidence  = build_mock_evidence(company.company_id)
    mapper    = DimensionMapper()
    retriever = HybridRetriever()
    indexed   = retriever.index_evidence(evidence, mapper)
    print(f"  Indexed {indexed} mock evidence documents")

    # Step 6 — Hybrid search
    print(f"\n[Step 6] Running hybrid search for relevant evidence...")
    query   = " ".join(rubric.keywords[:5])
    results_search = retriever.search(
        query=query,
        top_k=10,
        company_id=company.company_id,
        dimension=DIM_KEY,
    )
    print(f"  Retrieved {len(results_search)} documents via hybrid search")

    # Step 7 — Match to rubric keywords → CitedEvidence
    cited = []
    for r in results_search:
        matched = [kw for kw in rubric.keywords if kw.lower() in r.content.lower()]
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
    cited = sorted(cited, key=lambda x: len(x.matched_keywords), reverse=True)[:5]

    # Step 8 — Identify gaps
    next_level_val = level.value + 1
    gaps = []
    if next_level_val <= 5:
        next_rubric = RUBRIC_BY_LEVEL.get(next_level_val)
        if next_rubric:
            evidence_text = " ".join(r.content.lower() for r in results_search)
            gaps = [
                f"No evidence of '{kw}' (Level {next_level_val} criterion)"
                for kw in next_rubric.keywords
                if kw.lower() not in evidence_text
            ][:5]

    # Step 9 — LLM summary
    print(f"\n[Step 7] Generating IC summary via LLM...")
    evidence_text_llm = "\n".join([
        f"[{e.source_type}, conf={e.confidence:.2f}] {e.content[:300]}..."
        for e in cited[:5]
    ]) or "No evidence found."

    router = ModelRouter()
    try:
        response = await router.complete(
            task=TaskType.JUSTIFICATION_GENERATION,
            messages=[{
                "role": "user",
                "content": JUSTIFICATION_PROMPT.format(
                    company_id=company.ticker,
                    dimension=DIM_KEY.replace("_", " ").title(),
                    score=dim_val,
                    level=level.value,
                    level_name=level.name_label,
                    rubric_criteria=rubric.criteria_text,
                    rubric_keywords=", ".join(rubric.keywords),
                    evidence_text=evidence_text_llm,
                ),
            }],
        )
        summary = response.choices[0].message.content
    except Exception as e:
        summary = f"[LLM unavailable: {e}]"

    # Assess strength
    if cited:
        avg_conf    = sum(e.confidence for e in cited) / len(cited)
        avg_matches = sum(len(e.matched_keywords) for e in cited) / len(cited)
        strength = "strong" if avg_conf >= 0.8 and avg_matches >= 2 else \
                   "moderate" if avg_conf >= 0.6 or avg_matches >= 1 else "weak"
    else:
        strength = "weak"

    # ----------------------------------------------------------------
    # Final report
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SCORE JUSTIFICATION REPORT")
    print("=" * 60)
    print(f"\nCompany   : {company.name} ({company.ticker})")
    print(f"Dimension : Data Infrastructure")
    print(f"Score     : {dim_val:.0f}/100  (Level {level.value} — {level.name_label})")
    print(f"CI 95%    : [{score_obj.confidence_interval[0]:.0f}, {score_obj.confidence_interval[1]:.0f}]")

    print(f"\nRubric Criteria:")
    print(f"  {rubric.criteria_text}")

    print(f"\nSupporting Evidence ({len(cited)} items):")
    if cited:
        for i, e in enumerate(cited, 1):
            print(f"  {i}. [{e.source_type}] conf={e.confidence:.2f}  rel={e.relevance_score:.3f}")
            print(f"     {e.content[:100].strip()}...")
            if e.matched_keywords:
                print(f"     Matched keywords: {e.matched_keywords}")
    else:
        print("  No supporting evidence found.")

    print(f"\nGaps to Next Level:")
    if gaps:
        for gap in gaps:
            print(f"  - {gap}")
    else:
        print("  None.")

    print(f"\nEvidence Strength : {strength.upper()}")
    print(f"\nGenerated IC Summary:")
    print("-" * 60)
    print(summary)
    print("-" * 60)
    print("\n✅ Pipeline complete.")


if __name__ == "__main__":
    import platform
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_pipeline())
    finally:
        loop.close()