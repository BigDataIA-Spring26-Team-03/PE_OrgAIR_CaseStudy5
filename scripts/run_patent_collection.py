"""
Runner: Innovation Activity (Patent) signal collection for all 5 portfolio companies.

Usage (from project root):
    poetry run python scripts/run_patent_collection.py

What it does:
    1. Calls PatentSignalPipeline.run_for_all_companies() — already fully built
    2. Hits USPTO API for each company using COMPANY_USPTO_NAMES mapping
    3. Scores patents on count, recency, and category diversity
    4. Inserts innovation_activity signals into Snowflake
    5. Prints ranked summary

Requirements:
    - USPTO API key must be set in environment or .env as USPTO_API_KEY
    - Companies must exist in Snowflake companies table
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# --- Path setup ---
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.pipelines.patent_signals import PatentSignalPipeline, COMPANY_USPTO_NAMES


async def main():
    print("\n" + "="*60)
    print("  Innovation Activity (Patent) Collection — OrgAIR Pipeline")
    print("="*60)

    # Show which companies will be processed
    tickers = ["NVDA", "JPM", "WMT", "GE", "DG"]
    print("\n  USPTO name mappings:")
    for t in tickers:
        uspto = COMPANY_USPTO_NAMES.get(t, "❌ NOT MAPPED")
        print(f"    {t:<8} → {uspto}")

    print("\n  Starting USPTO API collection (last 5 years)...")
    print("  Note: ~2s delay between companies to respect API rate limits\n")

    # Clean old innovation signals first
    from app.services.snowflake import db
    for ticker in tickers:
        rows = db.execute_query(
            "SELECT id FROM companies WHERE ticker = %(t)s", {"t": ticker}
        )
        if rows:
            company_id = rows[0].get("id")
            deleted = db.execute_update(
                """
                DELETE FROM external_signals
                WHERE company_id = %(company_id)s
                  AND category = 'innovation_activity'
                """,
                {"company_id": company_id}
            )
            if deleted:
                print(f"  Deleted {deleted} old innovation_activity signals for {ticker}")

    # Run the real pipeline
    pipeline = PatentSignalPipeline(years=5)
    results  = await pipeline.run_for_all_companies()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n\n" + "="*60)
    print("  COLLECTION SUMMARY")
    print("="*60)
    print(f"  Total:      {results['total']}")
    print(f"  Successful: {results['successful']}")
    print(f"  Failed:     {results['failed']}")

    successful = results["results"]["successful"]
    failed     = results["results"]["failed"]

    if successful:
        print(f"\n  {'Ticker':<8} {'Score':<10} Company")
        print(f"  {'-'*7:<8} {'-'*7:<10} {'-'*20}")
        for r in sorted(successful, key=lambda x: x.get("score", 0), reverse=True):
            print(f"  {r['ticker']:<8} {r.get('score', 0):<10} {r['company_name']}")

    if failed:
        print(f"\n  FAILED:")
        for r in failed:
            print(f"    • {r['ticker']}: {r.get('error', 'unknown')}")

    # ── Verify SQL ─────────────────────────────────────────────────────────────
    print("\n  Verify in Snowflake:")
    print("""
    SELECT c.ticker,
           COUNT(*) as signal_count,
           ROUND(AVG(es.normalized_score), 2) as avg_score
    FROM external_signals es
    JOIN companies c ON es.company_id = c.id
    WHERE c.ticker IN ('NVDA','JPM','WMT','GE','DG')
      AND es.category = 'innovation_activity'
    GROUP BY c.ticker
    ORDER BY avg_score DESC;
    """)

    print("\n  Expected ranking:")
    print("    NVDA: ~85-95  (GPU/AI chip patents, CUDA, transformer accelerators)")
    print("    WMT:  ~60-70  (supply chain AI, demand forecasting)")
    print("    JPM:  ~50-60  (fintech AI, fraud detection — should match current)")
    print("    GE:   ~35-45  (industrial IoT, predictive maintenance)")
    print("    DG:   ~05-15  (minimal AI patent activity)")


if __name__ == "__main__":
    asyncio.run(main())