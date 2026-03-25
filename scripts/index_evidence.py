"""
One-time script to index all evidence from Snowflake into ChromaDB.

Run with:
    poetry run python scripts/index_evidence.py

What it does:
1. Calls /api/v1/evidence for each company
2. Converts to CS2Evidence objects
3. Indexes into ChromaDB + BM25 via HybridRetriever
4. Marks as indexed in Redis
"""

import sys
import os
import httpx
import asyncio
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.retrieval.hybrid import HybridRetriever
from src.services.retrieval.dimension_mapper import DimensionMapper
from src.services.integration.cs2_client import (
    CS2Evidence, SourceType, SignalCategory
)

BASE_URL = "http://localhost:8000"
_FALLBACK_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]

def raw_to_cs2evidence(raw: dict) -> CS2Evidence | None:
    try:
        return CS2Evidence(
            evidence_id=str(raw["evidence_id"]),
            company_id=str(raw["company_id"]),
            source_type=SourceType(raw["source_type"]),
            signal_category=SignalCategory(raw["signal_category"]),
            content=raw.get("content") or "",
            extracted_at=datetime.now(),
            confidence=float(raw.get("confidence") or 0.5),
            fiscal_year=raw.get("fiscal_year"),
            source_url=raw.get("source_url"),
        )
    except Exception as e:
        print(f"  ⚠️  Skipping evidence {raw.get('evidence_id')}: {e}")
        return None

async def fetch_evidence(company_id: str) -> list:
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            f"{BASE_URL}/api/v1/evidence",
            params={"company_id": company_id}
        )
        response.raise_for_status()
        return response.json()

async def mark_indexed(evidence_ids: list[str]) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{BASE_URL}/api/v1/evidence/mark-indexed",
            json={"evidence_ids": evidence_ids}
        )
        response.raise_for_status()

async def fetch_all_tickers() -> list[str]:
    """Fetch all company tickers from CS1 API with fallbacks."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{BASE_URL}/api/v1/companies",
                params={"limit": 500, "offset": 0},
            )
            response.raise_for_status()
            data = response.json()
            companies = data if isinstance(data, list) else data.get("items", data.get("results", []))
            tickers = [
                str(c.get("ticker", "")).strip().upper()
                for c in (companies or [])
                if c.get("ticker")
            ]
            if tickers:
                print(f"📋 Loaded {len(tickers)} tickers from CS1 API: {tickers}")
                return tickers
    except Exception as exc:
        print(f"⚠️  CS1 API unavailable: {exc}")

    env_tickers = os.getenv("PORTFOLIO_TICKERS", "").strip()
    if env_tickers:
        tickers = [t.strip().upper() for t in env_tickers.split(",") if t.strip()]
        if tickers:
            print(f"📋 Loaded {len(tickers)} tickers from PORTFOLIO_TICKERS env var")
            return tickers

    print(f"⚠️  Using hardcoded fallback: {_FALLBACK_TICKERS}")
    return _FALLBACK_TICKERS


async def main():
    print("🚀 Starting evidence indexing pipeline...\n")

    retriever = HybridRetriever()
    mapper = DimensionMapper()
    total_indexed = 0

    companies = await fetch_all_tickers()
    for ticker in companies:
        print(f"📊 Processing {ticker}...")

        try:
            raw_evidence = await fetch_evidence(ticker)
            print(f"   Found {len(raw_evidence)} evidence items in Snowflake")
        except Exception as e:
            print(f"   ❌ Failed to fetch evidence for {ticker}: {e}")
            continue

        if not raw_evidence:
            print(f"   ⚠️  No evidence found for {ticker}, skipping")
            continue

        evidence_list = []
        for raw in raw_evidence:
            ev = raw_to_cs2evidence(raw)
            if ev and ev.content.strip():
                evidence_list.append(ev)

        print(f"   Converted {len(evidence_list)} valid evidence items")

        if not evidence_list:
            print(f"   ⚠️  No valid evidence for {ticker}, skipping")
            continue

        try:
            count = retriever.index_evidence(evidence_list, mapper)
            print(f"   ✅ Indexed {count} items into ChromaDB + BM25")
            total_indexed += count
        except Exception as e:
            print(f"   ❌ Failed to index {ticker}: {e}")
            continue

        try:
            ids = [e.evidence_id for e in evidence_list]
            await mark_indexed(ids)
            print(f"   ✅ Marked {len(ids)} items as indexed in Redis")
        except Exception as e:
            print(f"   ⚠️  Failed to mark indexed in Redis: {e}")

        print()

    print(f"✅ Done! Total evidence indexed: {total_indexed}")
    chroma_path = os.getenv("CHROMA_PERSIST_DIR", "./chroma_data")
    print(f"📁 ChromaDB stored at: {chroma_path}")
    print(f"\nNow test search:")
    print(f'  curl "http://localhost:8000/api/v1/search?query=AI+talent&company_id=NVDA"')

if __name__ == "__main__":
    asyncio.run(main())
