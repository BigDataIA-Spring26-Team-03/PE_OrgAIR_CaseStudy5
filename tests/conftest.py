from datetime import datetime

import pytest

from src.services.integration.cs2_client import CS2Evidence, SignalCategory, SourceType
from src.services.retrieval.dimension_mapper import DimensionMapper
from src.services.retrieval.hybrid import HybridRetriever

# ---------------------------------------------------------------------------
# Shared seed evidence (realistic NVDA + MSFT data, no CS2 API needed)
# ---------------------------------------------------------------------------

SEED_EVIDENCE = [
    CS2Evidence(
        evidence_id="e1",
        company_id="NVDA",
        source_type=SourceType.SEC_10K_ITEM_1,
        signal_category=SignalCategory.DIGITAL_PRESENCE,
        content=(
            "NVIDIA operates a cloud-native GPU data center with real-time ML pipelines "
            "and Snowflake integration. Data quality exceeds 90% across all datasets."
        ),
        extracted_at=datetime.now(),
        confidence=0.9,
        fiscal_year=2024,
    ),
    CS2Evidence(
        evidence_id="e2",
        company_id="NVDA",
        source_type=SourceType.JOB_POSTING_LINKEDIN,
        signal_category=SignalCategory.TECHNOLOGY_HIRING,
        content=(
            "NVIDIA is hiring 500+ ML engineers and data scientists for its AI platform team. "
            "Roles include feature store engineers, MLOps specialists, and AI researchers."
        ),
        extracted_at=datetime.now(),
        confidence=0.85,
        fiscal_year=2024,
    ),
    CS2Evidence(
        evidence_id="e3",
        company_id="NVDA",
        source_type=SourceType.GLASSDOOR_REVIEW,
        signal_category=SignalCategory.CULTURE_SIGNALS,
        content=(
            "Strong AI-first culture at NVIDIA. Management encourages innovation. "
            "Team is technically excellent and collaborative."
        ),
        extracted_at=datetime.now(),
        confidence=0.75,
        fiscal_year=2024,
    ),
    CS2Evidence(
        evidence_id="e4",
        company_id="MSFT",
        source_type=SourceType.SEC_10K_ITEM_1,
        signal_category=SignalCategory.DIGITAL_PRESENCE,
        content=(
            "Microsoft Azure cloud infrastructure supports petabyte-scale data analytics "
            "with integrated AI services and real-time streaming."
        ),
        extracted_at=datetime.now(),
        confidence=0.88,
        fiscal_year=2024,
    ),
]


@pytest.fixture
def seeded_retriever(tmp_path):
    """
    Real HybridRetriever backed by real ChromaDB in an isolated tmp dir.
    Pre-seeded with NVDA + MSFT evidence. Never touches ./chroma_data.
    """
    retriever = HybridRetriever(persist_dir=str(tmp_path / "chroma_test"))
    mapper = DimensionMapper()
    retriever.index_evidence(SEED_EVIDENCE, mapper)
    return retriever
