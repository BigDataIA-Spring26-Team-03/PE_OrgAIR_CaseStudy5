#!/usr/bin/env python3
"""Quick test of patent collection (innovation activity) - uses dynamic _contains search."""

import argparse
import asyncio
from app.services.snowflake import SnowflakeService
from app.pipelines.patent_signals import collect_patent_signals_real


async def main(ticker: str = "TSLA"):
    db = SnowflakeService()
    try:
        companies = db.execute_query(
            "SELECT id, name, ticker FROM companies WHERE ticker = %(ticker)s AND is_deleted = FALSE",
            {"ticker": ticker.upper()},
        )
        if not companies:
            print(f"❌ Company '{ticker}' not found in DB")
            return

        company = companies[0]
        company_id = company["id"]
        company_name = company["name"]
        print(f"📋 Testing patent collection for {ticker}: {company_name}")
        print("-" * 50)

        signals = await collect_patent_signals_real(
            company_id=company_id,
            company_name=company_name,
            years=5,
            ticker=ticker,
        )

        if signals:
            s = signals[0]
            print(f"✅ Collected {len(signals)} patent signal(s)")
            print(f"   Score: {s.score}")
            print(f"   Title: {s.title[:80]}..." if len(s.title or "") > 80 else f"   Title: {s.title}")
        else:
            print("⚠️ No patent signals returned (may be 0 AI patents for this company)")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ticker", nargs="?", default="TSLA", help="Ticker symbol (e.g. MSFT, NVDA)")
    args = parser.parse_args()
    asyncio.run(main(ticker=args.ticker.upper()))
