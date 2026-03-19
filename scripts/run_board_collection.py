"""Standalone CLI script to collect board governance data for all 5 companies.

Uses the 6-stage LLM-powered pipeline:
  1. COLLECT: EDGAR API -> fetch DEF 14A HTML -> strip to clean text
  2. CHUNK: Clean text -> overlapping chunks, filter garbage
  3. LLM EXTRACT: LangChain + GPT-4o-mini -> structured JSON per chunk
  4. MERGE + DEDUPLICATE: Combine chunks -> normalize names -> dedupe
  5. SCORE: BoardCompositionAnalyzer.analyze_board() (unchanged)
  6. PERSIST + QUALITY GATES: Validate -> Snowflake

Usage:
    python scripts/run_board_collection.py                  # all 5 tickers, cached
    python scripts/run_board_collection.py --ticker NVDA    # single ticker
    python scripts/run_board_collection.py --no-cache       # force fresh SEC EDGAR scrape + LLM extraction
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
import uuid as uuid_lib
from datetime import datetime
from pathlib import Path

# Ensure project root and src/ are importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

from app.services.snowflake import db
from app.pipelines.board_collector import BoardCompositionCollector
from app.pipelines.board_chunker import chunk_proxy_text
from app.pipelines.board_llm_extractor import (
    extract_from_chunks,
    merge_extractions,
    quality_check,
)
from scoring.board_analyzer import BoardCompositionAnalyzer, BoardMember

ALL_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]

# Deterministic industry IDs (must match existing seed data / schema)
INDUSTRY_IDS = {
    "Manufacturing": "00000000-0000-0000-0000-000000000001",
    "Technology": "00000000-0000-0000-0000-000000000002",
    "Business Services": "00000000-0000-0000-0000-000000000003",
    "Retail": "00000000-0000-0000-0000-000000000004",
    "Financial Services": "00000000-0000-0000-0000-000000000005",
}

# Ticker -> (company name, industry name, deterministic company UUID)
COMPANY_SEED = {
    "NVDA": ("NVIDIA Corporation", "Business Services", "10000000-0000-0000-0000-000000000001"),
    "JPM": ("JPMorgan Chase & Co.", "Financial Services", "10000000-0000-0000-0000-000000000002"),
    "WMT": ("Walmart Inc.", "Retail", "10000000-0000-0000-0000-000000000003"),
    "GE": ("General Electric Company", "Manufacturing", "10000000-0000-0000-0000-000000000004"),
    "DG": ("Dollar General Corporation", "Retail", "10000000-0000-0000-0000-000000000005"),
}


def seed_companies() -> None:
    """Ensure the 5 portfolio companies exist in the companies table."""
    for ticker, (name, industry, company_id) in COMPANY_SEED.items():
        result = db.execute_query(
            "SELECT id FROM companies WHERE ticker = %(ticker)s",
            {"ticker": ticker},
        )
        if result:
            print(f"  {ticker} already exists.")
            continue

        industry_id = INDUSTRY_IDS[industry]
        db.execute_update(
            """
            INSERT INTO companies (id, name, ticker, industry_id, created_at)
            VALUES (%(id)s, %(name)s, %(ticker)s, %(industry_id)s, %(created_at)s)
            """,
            {
                "id": company_id,
                "name": name,
                "ticker": ticker,
                "industry_id": industry_id,
                "created_at": datetime.utcnow(),
            },
        )
        print(f"  Seeded {ticker} ({name}).")


def create_tables() -> None:
    """Run schema_board_composition.sql DDL to create/replace tables."""
    schema_path = PROJECT_ROOT / "app" / "database" / "schema_board_composition.sql"
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}", file=sys.stderr)
        sys.exit(1)

    with open(schema_path, "r") as f:
        schema_sql = f.read()

    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]

    print("Creating board governance tables...")
    for i, statement in enumerate(statements, 1):
        preview = statement[:60].replace("\n", " ")
        print(f"  [{i}/{len(statements)}] {preview}...")
        db.execute_update(statement)
    print("  Tables ready.\n")


def resolve_company_id(ticker: str) -> str:
    """Look up company_id from the companies table."""
    result = db.execute_query(
        "SELECT id FROM companies WHERE ticker = %(ticker)s",
        {"ticker": ticker.upper()},
    )
    if not result:
        raise ValueError(f"Company {ticker} not found in companies table.")
    return str(result[0].get("ID") or result[0].get("id"))


def collect_ticker(ticker: str, use_cache: bool) -> dict:
    """Collect board data for a single ticker using the LLM pipeline.

    Pipeline stages:
    1. Collect raw text from EDGAR
    2. Chunk the text
    3. LLM extraction per chunk
    4. Merge + deduplicate
    5. Score with BoardCompositionAnalyzer
    6. Persist to Snowflake
    """
    ticker = ticker.upper()
    company_id = resolve_company_id(ticker)

    collector = BoardCompositionCollector()

    # Stage 1: Collect raw text (or use cached)
    print(f"  Stage 1: Collecting raw text...")
    raw = collector.collect_board_data(ticker, use_cache=use_cache)

    # If cache had members already, skip LLM stages
    if raw.get("members"):
        print(f"  Using cached members ({len(raw['members'])} directors)")
    else:
        # Need raw text for LLM extraction
        raw_text_data = collector.collect_raw_text(ticker)
        raw_text = raw_text_data.get("raw_text", "")

        if not raw_text:
            raise RuntimeError(f"No proxy text available for {ticker}")

        # Stage 2: Chunk
        print(f"  Stage 2: Chunking ({len(raw_text):,} chars)...")
        chunks = chunk_proxy_text(raw_text)
        print(f"  -> {len(chunks)} chunks after filtering")

        if not chunks:
            raise RuntimeError(f"No usable chunks produced for {ticker}")

        # Stage 3: LLM extraction
        print(f"  Stage 3: LLM extraction...")
        extractions = extract_from_chunks(chunks, ticker, use_cache=use_cache)
        total_directors = sum(len(e.directors) for e in extractions)
        print(f"  -> {total_directors} director mentions from {len(extractions)} chunks")

        if not extractions:
            raise RuntimeError(f"LLM extraction produced no results for {ticker}")

        # Stage 4: Merge + deduplicate
        print(f"  Stage 4: Merge + deduplicate...")
        members = merge_extractions(extractions)
        print(f"  -> {len(members)} unique directors")

        raw["members"] = members

        # Cache final result with members
        collector.cache_with_members(ticker, raw)

    # Quality gate
    ok, reason = quality_check(raw.get("members", []))
    if not ok:
        print(f"  WARNING: Quality gate: {reason} (proceeding anyway)")

    # Stage 5: Score
    print(f"  Stage 5: Scoring...")
    members = [
        BoardMember(
            name=m["name"],
            title=m.get("title", "Director"),
            committees=m.get("committees", []),
            bio=m.get("bio", ""),
            is_independent=m.get("is_independent", False),
            tenure_years=m.get("tenure_years", 0.0),
        )
        for m in raw.get("members", [])
    ]

    analyzer = BoardCompositionAnalyzer()
    signal = analyzer.analyze_board(
        company_id=company_id,
        ticker=ticker,
        members=members,
        committees=raw.get("committees", []),
        strategy_text=raw.get("strategy_text", ""),
    )

    # Stage 6: Persist
    print(f"  Stage 6: Persisting to Snowflake...")
    signal_id = str(uuid_lib.uuid4())

    insert_signal = """
        INSERT INTO board_governance_signals (
            id, company_id, ticker,
            governance_score, has_tech_committee, has_ai_expertise,
            has_data_officer, has_independent_majority,
            has_risk_tech_oversight, has_ai_strategy,
            ai_experts, evidence, confidence, created_at
        ) VALUES (
            %(id)s, %(company_id)s, %(ticker)s,
            %(governance_score)s, %(has_tech_committee)s, %(has_ai_expertise)s,
            %(has_data_officer)s, %(has_independent_majority)s,
            %(has_risk_tech_oversight)s, %(has_ai_strategy)s,
            %(ai_experts)s, %(evidence)s, %(confidence)s, %(created_at)s
        )
    """

    db.execute_update(
        insert_signal,
        {
            "id": signal_id,
            "company_id": company_id,
            "ticker": ticker,
            "governance_score": float(signal.governance_score),
            "has_tech_committee": signal.has_tech_committee,
            "has_ai_expertise": signal.has_ai_expertise,
            "has_data_officer": signal.has_data_officer,
            "has_independent_majority": signal.has_independent_majority,
            "has_risk_tech_oversight": signal.has_risk_tech_oversight,
            "has_ai_strategy": signal.has_ai_strategy,
            "ai_experts": json.dumps(signal.ai_experts),
            "evidence": json.dumps(signal.evidence),
            "confidence": float(signal.confidence),
            "created_at": datetime.utcnow(),
        },
    )

    insert_member = """
        INSERT INTO board_members (
            id, company_id, governance_signal_id,
            name, title, committees, bio,
            is_independent, tenure_years, created_at
        ) VALUES (
            %(id)s, %(company_id)s, %(governance_signal_id)s,
            %(name)s, %(title)s, %(committees)s, %(bio)s,
            %(is_independent)s, %(tenure_years)s, %(created_at)s
        )
    """

    for m in members:
        db.execute_update(
            insert_member,
            {
                "id": str(uuid_lib.uuid4()),
                "company_id": company_id,
                "governance_signal_id": signal_id,
                "name": m.name,
                "title": m.title,
                "committees": json.dumps(m.committees),
                "bio": m.bio[:2000] if m.bio else "",
                "is_independent": m.is_independent,
                "tenure_years": m.tenure_years,
                "created_at": datetime.utcnow(),
            },
        )

    return {
        "ticker": ticker,
        "signal_id": signal_id,
        "governance_score": float(signal.governance_score),
        "member_count": len(members),
        "confidence": float(signal.confidence),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect board governance data using LLM pipeline."
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Run for a single ticker (e.g. NVDA). Default: all 5 companies.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Force fresh SEC EDGAR scraping + LLM extraction (skip cache).",
    )
    args = parser.parse_args()

    use_cache = not args.no_cache
    tickers = [args.ticker.upper()] if args.ticker else ALL_TICKERS

    print("=" * 60)
    print("Board Governance Collection (LLM Pipeline)")
    print(f"Tickers: {', '.join(tickers)}")
    print(f"Cache: {'enabled' if use_cache else 'disabled (fresh scrape + extraction)'}")
    print("=" * 60)

    # Step 1: Create tables and seed companies
    try:
        db.connect()
        create_tables()
        print("Seeding companies...")
        seed_companies()
        print()
    except Exception as e:
        print(f"Failed to initialize: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    # Step 2: Collect for each ticker
    collected = []
    failed = []

    for ticker in tickers:
        print(f"\nCollecting {ticker}...")
        try:
            result = collect_ticker(ticker, use_cache=use_cache)
            collected.append(result)
            print(f"  Score: {result['governance_score']:.1f}  "
                  f"Members: {result['member_count']}  "
                  f"Confidence: {result['confidence']:.2f}")
        except Exception as e:
            failed.append({"ticker": ticker, "error": str(e)})
            print(f"  FAILED: {e}", file=sys.stderr)
            traceback.print_exc()

    # Step 3: Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Collected: {len(collected)} / {len(tickers)}")

    if collected:
        print(f"\n{'Ticker':<8} {'Score':>8} {'Members':>8} {'Confidence':>12}")
        print("-" * 40)
        for r in collected:
            print(f"{r['ticker']:<8} {r['governance_score']:>8.1f} "
                  f"{r['member_count']:>8} {r['confidence']:>12.2f}")

    if failed:
        print(f"\nFailed ({len(failed)}):")
        for f in failed:
            print(f"  {f['ticker']}: {f['error']}")

    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
