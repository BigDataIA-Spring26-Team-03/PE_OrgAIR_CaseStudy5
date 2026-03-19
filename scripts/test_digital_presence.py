#!/usr/bin/env python3
"""Quick test of Digital Presence (tech signals) collection for a single ticker."""

import argparse
from app.services.snowflake import SnowflakeService
from app.pipelines.tech_signals import scrape_tech_signal_inputs, tech_inputs_to_signals


def main(ticker: str = "TSLA"):
    db = SnowflakeService()
    try:
        rows = db.execute_query(
            """
            SELECT id, ticker, name
            FROM companies
            WHERE ticker = %(ticker)s AND is_deleted = FALSE
            """,
            {"ticker": ticker.upper()},
        )
        if not rows:
            print(f"❌ Company '{ticker}' not found in DB")
            return

        company = rows[0]
        company_id = company["id"]
        domain = db.get_domain_for_company(company_id=company_id, ticker=ticker.upper())
        if not domain:
            print(f"❌ No domain for {ticker} (company_domains or yfinance)")
            return

        print(f"📋 Testing Digital Presence for {ticker}: {domain}")
        print("-" * 50)

        inputs = scrape_tech_signal_inputs(company=ticker.upper(), company_domain_or_url=domain)
        signals = tech_inputs_to_signals(company_id=company_id, items=inputs)

        if signals:
            scores = [s.score for s in signals]
            print(f"✅ Collected {len(signals)} tech signals")
            print(f"   Avg score: {sum(scores) / len(scores):.1f}")
            print(f"   Pages: {[inp.url for inp in inputs[:5]]}...")
        else:
            print("⚠️ No tech signals found")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ticker", nargs="?", default="TSLA", help="Ticker (e.g. MSFT, NVDA)")
    args = parser.parse_args()
    main(ticker=args.ticker.upper())
