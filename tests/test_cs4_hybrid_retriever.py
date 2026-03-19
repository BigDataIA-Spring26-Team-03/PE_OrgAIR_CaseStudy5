# tests/test_cs4_hybrid_retriever.py
# Tests real ChromaDB + real BM25 + RRF fusion.
# No server needed. No LLM calls. Takes ~10 sec (sentence-transformer embedding).

import pytest

from src.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from src.services.retrieval.dimension_mapper import DimensionMapper
from src.services.integration.cs2_client import CS2Evidence, SignalCategory, SourceType
from datetime import datetime

# Re-use the seeded_retriever fixture from conftest.py
# It has: e1(NVDA/DIGITAL_PRESENCE/0.9), e2(NVDA/TECHNOLOGY_HIRING/0.85),
#         e3(NVDA/CULTURE_SIGNALS/0.75), e4(MSFT/DIGITAL_PRESENCE/0.88)


# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------

def test_index_returns_count(tmp_path):
    retriever = HybridRetriever(persist_dir=str(tmp_path / "chroma"))
    mapper = DimensionMapper()
    from tests.conftest import SEED_EVIDENCE
    count = retriever.index_evidence(SEED_EVIDENCE, mapper)
    assert count == len(SEED_EVIDENCE)


def test_index_empty_list(tmp_path):
    retriever = HybridRetriever(persist_dir=str(tmp_path / "chroma"))
    mapper = DimensionMapper()
    count = retriever.index_evidence([], mapper)
    assert count == 0


def test_bm25_built_after_indexing(seeded_retriever):
    assert seeded_retriever._bm25 is not None
    assert len(seeded_retriever._corpus) == 4


# ---------------------------------------------------------------------------
# Basic search
# ---------------------------------------------------------------------------

def test_search_returns_results(seeded_retriever):
    results = seeded_retriever.search("GPU data center machine learning", top_k=3)
    assert len(results) > 0
    assert len(results) <= 3


def test_search_returns_retrieved_documents(seeded_retriever):
    results = seeded_retriever.search("cloud infrastructure", top_k=2)
    for r in results:
        assert isinstance(r, RetrievedDocument)
        assert r.doc_id
        assert r.content
        assert isinstance(r.score, float)


def test_search_scores_non_negative(seeded_retriever):
    results = seeded_retriever.search("data analytics AI", top_k=4)
    for r in results:
        assert r.score >= 0.0


def test_search_scores_sorted_descending(seeded_retriever):
    results = seeded_retriever.search("machine learning engineers", top_k=4)
    scores = [r.score for r in results]
    assert scores == sorted(scores, reverse=True)


def test_search_retrieval_method_is_hybrid(seeded_retriever):
    results = seeded_retriever.search("data infrastructure", top_k=3)
    for r in results:
        assert r.retrieval_method == "hybrid"


# ---------------------------------------------------------------------------
# company_id filter
# ---------------------------------------------------------------------------

def test_filter_company_id_excludes_other(seeded_retriever):
    results = seeded_retriever.search("cloud data analytics", top_k=10, company_id="NVDA")
    doc_ids = [r.doc_id for r in results]
    # e4 is MSFT — must not appear
    assert "e4" not in doc_ids


def test_filter_company_id_msft(seeded_retriever):
    results = seeded_retriever.search("Azure cloud petabyte analytics", top_k=10, company_id="MSFT")
    for r in results:
        assert r.metadata.get("company_id") == "MSFT"


# ---------------------------------------------------------------------------
# min_confidence filter
# ---------------------------------------------------------------------------

def test_filter_min_confidence_excludes_low(seeded_retriever):
    # e3 has confidence=0.75, should be excluded when min=0.85
    results = seeded_retriever.search("culture innovation team", top_k=10, min_confidence=0.85)
    doc_ids = [r.doc_id for r in results]
    assert "e3" not in doc_ids


def test_filter_min_confidence_zero_returns_all(seeded_retriever):
    results = seeded_retriever.search("NVIDIA", top_k=10, min_confidence=0.0)
    assert len(results) >= 1  # at least some docs returned


# ---------------------------------------------------------------------------
# BM25 keyword matching (sparse signal)
# ---------------------------------------------------------------------------

def test_bm25_keyword_hit_surfaces_correct_doc(seeded_retriever):
    # "ML engineers data scientists hiring" is exact content of e2
    results = seeded_retriever.search("ML engineers data scientists hiring", top_k=4)
    doc_ids = [r.doc_id for r in results]
    # e2 should appear in top results
    assert "e2" in doc_ids
    # and it should be in top 2
    assert doc_ids.index("e2") < 2


# ---------------------------------------------------------------------------
# index_documents (analyst notes path)
# ---------------------------------------------------------------------------

def test_index_documents_generic(tmp_path):
    retriever = HybridRetriever(persist_dir=str(tmp_path / "chroma_notes"))
    docs = [
        {
            "doc_id": "note1",
            "content": "Management confirmed GPU data center expansion plans for 2025.",
            "metadata": {"company_id": "NVDA", "confidence": 1.0},
        },
        {
            "doc_id": "note2",
            "content": "CEO emphasized talent retention as top priority.",
            "metadata": {"company_id": "NVDA", "confidence": 1.0},
        },
    ]
    count = retriever.index_documents(docs)
    assert count == len(docs)
    results = retriever.search("data center expansion", top_k=2)
    assert len(results) > 0


# ---------------------------------------------------------------------------
# RRF formula sanity check
# ---------------------------------------------------------------------------

def test_rrf_fusion_doc_in_both_lists_scores_higher(tmp_path):
    """A doc appearing in BOTH dense and sparse should score higher than one in only one."""
    retriever = HybridRetriever(persist_dir=str(tmp_path / "chroma_rrf"))
    mapper = DimensionMapper()
    from tests.conftest import SEED_EVIDENCE
    retriever.index_evidence(SEED_EVIDENCE, mapper)

    # e1 content: "GPU data center ML pipelines Snowflake"
    # Searching with these exact terms should make e1 appear in both dense + sparse
    results = retriever.search("GPU data center ML pipelines Snowflake", top_k=4)
    assert len(results) > 0
    # e1 should be ranked first (appears in both lists)
    assert results[0].doc_id == "e1"
