"""
Airflow DAG: evidence_indexing_pipeline

"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

for path in ["/opt/airflow", "/opt/airflow/src", "/opt/airflow/app"]:
    if path not in sys.path:
        sys.path.insert(0, path)

from airflow.decorators import dag, task

CS2_BASE_URL = os.environ.get("CS2_BASE_URL", "http://localhost:8000")
CHROMA_PERSIST_DIR = os.environ.get("CHROMA_PERSIST_DIR", "/data/chroma")

# Deprecated: tickers now loaded dynamically from Snowflake inside fetch_evidence task
_FALLBACK_COMPANIES = ["NVDA", "JPM", "WMT", "GE", "DG"]


@dag(
    dag_id="evidence_indexing_pipeline",
    description="Nightly: fetch unindexed CS2 evidence → index into ChromaDB + BM25",
    schedule="0 2 * * *",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["cs4", "rag", "evidence-indexing"],
    default_args={
        "owner": "pe-orgair",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
)
def evidence_indexing_pipeline():

    @task
    def fetch_evidence() -> list:
        """
        Fetch all unindexed evidence from CS2 API across all target companies.
        Returns serialized evidence dicts for XCom.
        """
        import logging
        from src.services.integration.cs2_client import CS2Client

        log = logging.getLogger(__name__)

        # Load tickers dynamically from Snowflake
        try:
            from app.services.snowflake import db
            rows = db.execute_query(
                "SELECT ticker FROM companies WHERE is_deleted = FALSE AND ticker IS NOT NULL"
            )
            tickers = [r["ticker"] for r in rows]
            log.info("Loaded %d tickers from Snowflake", len(tickers))
        except Exception as exc:
            log.error(
                "Snowflake ticker load FAILED (%s). "
                "Evidence indexing will cover only the hardcoded fallback of %d tickers: %s. "
                "Fix Snowflake connection to index evidence for all companies.",
                exc, len(_FALLBACK_COMPANIES), _FALLBACK_COMPANIES
            )
            tickers = _FALLBACK_COMPANIES

        async def _fetch():
            all_evidence = []
            async with CS2Client(base_url=CS2_BASE_URL) as cs2:
                for ticker in tickers:
                    try:
                        items = await cs2.get_evidence(
                            company_id=ticker,
                            indexed=False,
                        )
                        all_evidence.extend(items)
                        log.info("Fetched %d unindexed items for %s", len(items), ticker)
                    except Exception as exc:
                        log.error("Failed to fetch evidence for %s: %s", ticker, exc, exc_info=True)
                        raise
            return all_evidence

        evidence_list = asyncio.run(_fetch())

        # Serialize CS2Evidence → plain dicts for XCom (must be JSON-serializable)
        return [
            {
                "evidence_id": e.evidence_id,
                "company_id": e.company_id,
                "source_type": e.source_type.value,
                "signal_category": e.signal_category.value,
                "content": e.content,
                "confidence": e.confidence,
                "fiscal_year": e.fiscal_year,
                "source_url": e.source_url,
            }
            for e in evidence_list
        ]

    @task
    def index_evidence(evidence_dicts: list) -> dict:
        """
        Index evidence into HybridRetriever (ChromaDB + BM25),
        then mark items as indexed in CS2.
        Returns stats dict: {indexed: int, marked: int}.
        """
        import logging
        from datetime import datetime as dt
        from src.services.integration.cs2_client import (
            CS2Client, CS2Evidence, SourceType, SignalCategory,
        )
        from src.services.retrieval.hybrid import HybridRetriever
        from src.services.retrieval.dimension_mapper import DimensionMapper

        log = logging.getLogger(__name__)

        if not evidence_dicts:
            log.info("No unindexed evidence found — nothing to index.")
            return {"indexed": 0, "marked": 0}

        # Reconstruct CS2Evidence objects from XCom dicts
        evidence_list = [
            CS2Evidence(
                evidence_id=d["evidence_id"],
                company_id=d["company_id"],
                source_type=SourceType(d["source_type"]),
                signal_category=SignalCategory(d["signal_category"]),
                content=d["content"],
                extracted_at=dt.now(),
                confidence=d["confidence"],
                fiscal_year=d.get("fiscal_year"),
                source_url=d.get("source_url"),
            )
            for d in evidence_dicts
        ]

        # Index into HybridRetriever
        retriever = HybridRetriever(persist_dir=CHROMA_PERSIST_DIR)
        mapper = DimensionMapper()
        indexed_count = retriever.index_evidence(evidence_list, mapper)
        log.info("Indexed %d evidence items into ChromaDB + BM25", indexed_count)

        # Mark as indexed in CS2
        async def _mark():
            async with CS2Client(base_url=CS2_BASE_URL) as cs2:
                return await cs2.mark_indexed(
                    [e.evidence_id for e in evidence_list]
                )

        marked_count = asyncio.run(_mark())
        log.info("Marked %d evidence items as indexed in CS2", marked_count)

        return {"indexed": indexed_count, "marked": marked_count}

    # DAG wiring: fetch → index (sequential, index depends on fetch output)
    evidence = fetch_evidence()
    index_evidence(evidence)


evidence_indexing_pipeline()
