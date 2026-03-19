# src/services/retrieval/hybrid.py

from __future__ import annotations

import logging
import re
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from rank_bm25 import BM25Okapi

from src.services.search.vector_store import VectorStore, SearchResult
from src.services.retrieval.dimension_mapper import DimensionMapper
from src.services.integration.cs2_client import CS2Evidence

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class RetrievedDocument:
    """A single document returned by hybrid retrieval."""
    doc_id: str
    content: str
    metadata: Dict[str, Any]
    score: float                      # final RRF score
    retrieval_method: str             # "dense", "sparse", or "hybrid"


# ---------------------------------------------------------------------------
# HybridRetriever
# ---------------------------------------------------------------------------

class HybridRetriever:
    """
    Combines dense (vector) search and sparse (BM25) search using
    Reciprocal Rank Fusion (RRF) to produce better results than either alone.

    Usage:
        retriever = HybridRetriever()
        retriever.index_evidence(evidence_list, dimension_mapper)
        results = retriever.search("AI talent hiring", company_id="NVDA", top_k=10)
    """

    def __init__(
        self,
        persist_dir: str = os.getenv("CHROMA_PERSIST_DIR", "./chroma_data"),
        dense_weight: float = 0.6,    # how much to trust vector search
        sparse_weight: float = 0.4,   # how much to trust BM25
        rrf_k: int = 60,              # RRF constant (standard value)
    ) -> None:
        self.vector_store = VectorStore(persist_dir=persist_dir)
        self.dense_weight = dense_weight
        self.sparse_weight = sparse_weight
        self.rrf_k = rrf_k

        # BM25 state — built when evidence is indexed
        self._bm25: Optional[BM25Okapi] = None
        self._corpus: List[str] = []         # raw text for each doc
        self._doc_ids: List[str] = []        # doc_id matching each corpus entry
        self._metadata: List[Dict] = []      # metadata matching each corpus entry

        # Re-hydrate BM25 from any documents already persisted in ChromaDB
        self._reload_from_chroma()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _reload_from_chroma(self) -> None:
        """Rebuild BM25 from any documents already persisted in ChromaDB."""
        try:
            all_docs = self.vector_store.collection.get(include=["documents", "metadatas"])
            if not all_docs["ids"]:
                return
            self._corpus   = all_docs["documents"]
            self._doc_ids  = all_docs["ids"]
            self._metadata = all_docs["metadatas"]
            tokenized = [re.sub(r'[^\w\s]', ' ', doc.lower()).split() for doc in self._corpus]
            self._bm25 = BM25Okapi(tokenized)
            logger.info("bm25_reloaded_from_chroma", extra={"count": len(self._doc_ids)})
        except Exception as exc:
            logger.warning("bm25_reload_skipped", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------

    def index_evidence(
        self,
        evidence_list: List[CS2Evidence],
        dimension_mapper: DimensionMapper,
    ) -> int:
        """
        Index CS2 and CS3 evidence into both ChromaDB (dense) and BM25 (sparse).

        Returns number of documents indexed.
        """
        if not evidence_list:
            logger.warning("index_evidence called with empty list")
            return 0

        # 1. Index into ChromaDB for dense search
        count = self.vector_store.index_cs2_evidence(evidence_list, dimension_mapper)

        # 2. Build BM25 index from the same evidence
        self._corpus = [e.content for e in evidence_list]
        self._doc_ids = [e.evidence_id for e in evidence_list]
        self._metadata = [
            {
                "company_id": e.company_id,
                "source_type": e.source_type.value,
                "signal_category": e.signal_category.value,
                "dimension": dimension_mapper.get_primary_dimension(e.signal_category).value,
                "confidence": e.confidence,
                "source_url": e.source_url or "",
            }
            for e in evidence_list
        ]

        # Tokenize: lowercase, normalize punctuation, split on whitespace
        tokenized = [re.sub(r'[^\w\s]', ' ', doc.lower()).split() for doc in self._corpus]
        self._bm25 = BM25Okapi(tokenized)

        logger.info(f"hybrid_indexed: {count} documents")
        return count

    def index_documents(self, documents: List[Dict[str, Any]]) -> int:
        """
        Generic indexing for analyst notes and other non-CS2 documents.

        Each document must have: doc_id, content, metadata (optional).
        """
        if not documents:
            return 0

        # Index into ChromaDB
        count = self.vector_store.index_documents(documents)

        # Add to BM25 corpus
        for doc in documents:
            self._corpus.append(doc["content"])
            self._doc_ids.append(doc["doc_id"])
            self._metadata.append(doc.get("metadata", {}))

        # Rebuild BM25 index
        if self._corpus:
            tokenized = [re.sub(r'[^\w\s]', ' ', doc.lower()).split() for doc in self._corpus]
            self._bm25 = BM25Okapi(tokenized)

        return count

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        top_k: int = 10,
        company_id: Optional[str] = None,
        dimension: Optional[str] = None,
        source_types: Optional[List[str]] = None,
        min_confidence: float = 0.0,
    ) -> List[RetrievedDocument]:
        """
        Hybrid search: runs dense + sparse search then merges with RRF.

        Args:
            query:          Natural language search query.
            top_k:          Number of final results to return.
            company_id:     Filter to specific company e.g. "NVDA".
            dimension:      Filter to specific dimension e.g. "talent".
            source_types:   Filter to specific source types.
            min_confidence: Minimum evidence confidence (0.0 - 1.0).

        Returns:
            List of RetrievedDocument sorted by RRF score descending.
        """
        # --- Dense search (ChromaDB vector search) ---
        dense_results = self._dense_search(
            query=query,
            n=top_k * 2,  # fetch more than needed for better fusion
            company_id=company_id,
            dimension=dimension,
            source_types=source_types,
            min_confidence=min_confidence,
        )

        # --- Sparse search (BM25 keyword search) ---
        sparse_results = self._sparse_search(
            query=query,
            n=top_k * 2,
            company_id=company_id,
            dimension=dimension,
            min_confidence=min_confidence,
        )

        # --- RRF Fusion ---
        fused = self._rrf_fusion(dense_results, sparse_results, top_k)

        logger.info(
            f"hybrid_search: query='{query[:50]}' "
            f"dense={len(dense_results)} sparse={len(sparse_results)} "
            f"fused={len(fused)}"
        )
        return fused

    # ------------------------------------------------------------------
    # Private: Dense search
    # ------------------------------------------------------------------

    def _dense_search(
        self,
        query: str,
        n: int,
        company_id: Optional[str],
        dimension: Optional[str],
        source_types: Optional[List[str]],
        min_confidence: float,
    ) -> List[RetrievedDocument]:
        """Run vector search via ChromaDB."""
        results: List[SearchResult] = self.vector_store.search(
            query=query,
            top_k=n,
            company_id=company_id,
            dimension=dimension,
            source_types=source_types,
            min_confidence=min_confidence,
        )
        return [
            RetrievedDocument(
                doc_id=r.doc_id,
                content=r.content,
                metadata=r.metadata,
                score=r.score,
                retrieval_method="dense",
            )
            for r in results
        ]

    # ------------------------------------------------------------------
    # Private: Sparse search (BM25)
    # ------------------------------------------------------------------

    def _sparse_search(
        self,
        query: str,
        n: int,
        company_id: Optional[str],
        dimension: Optional[str],
        min_confidence: float,
    ) -> List[RetrievedDocument]:
        """Run BM25 keyword search over the indexed corpus."""
        if self._bm25 is None or not self._corpus:
            logger.warning("BM25 index not built yet — skipping sparse search")
            return []

        # Score all documents
        tokenized_query = re.sub(r'[^\w\s]', ' ', query.lower()).split()
        scores = self._bm25.get_scores(tokenized_query)

        # Get top-n indices sorted by score descending
        top_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True,
        )[:n]

        results = []
        for idx in top_indices:
            if scores[idx] == 0:
                continue  # skip zero-score docs

            meta = self._metadata[idx]

            # Apply filters manually (BM25 doesn't support metadata filtering)
            if company_id and meta.get("company_id") != company_id:
                continue
            if dimension and meta.get("dimension") != dimension:
                continue
            if min_confidence > 0 and meta.get("confidence", 0) < min_confidence:
                continue

            results.append(RetrievedDocument(
                doc_id=self._doc_ids[idx],
                content=self._corpus[idx],
                metadata=meta,
                score=float(scores[idx]),
                retrieval_method="sparse",
            ))

        return results

    # ------------------------------------------------------------------
    # Private: RRF Fusion
    # ------------------------------------------------------------------

    def _rrf_fusion(
        self,
        dense: List[RetrievedDocument],
        sparse: List[RetrievedDocument],
        top_k: int,
    ) -> List[RetrievedDocument]:
        """
        Merge dense and sparse results using Reciprocal Rank Fusion.

        Formula:
            score(doc) = dense_weight  * 1/(rank_in_dense  + rrf_k)
                       + sparse_weight * 1/(rank_in_sparse + rrf_k)

        Documents appearing in both lists get a higher combined score.
        """
        rrf_scores: Dict[str, float] = defaultdict(float)
        doc_map: Dict[str, RetrievedDocument] = {}

        # Score from dense results
        for rank, doc in enumerate(dense):
            rrf_scores[doc.doc_id] += self.dense_weight / (rank + 1 + self.rrf_k)
            doc_map[doc.doc_id] = doc

        # Score from sparse results
        for rank, doc in enumerate(sparse):
            rrf_scores[doc.doc_id] += self.sparse_weight / (rank + 1 + self.rrf_k)
            if doc.doc_id not in doc_map:
                doc_map[doc.doc_id] = doc

        # Sort by combined RRF score descending
        sorted_ids = sorted(
            rrf_scores.keys(),
            key=lambda doc_id: rrf_scores[doc_id],
            reverse=True,
        )[:top_k]

        return [
            RetrievedDocument(
                doc_id=did,
                content=doc_map[did].content,
                metadata=doc_map[did].metadata,
                score=rrf_scores[did],
                retrieval_method="hybrid",
            )
            for did in sorted_ids
        ]