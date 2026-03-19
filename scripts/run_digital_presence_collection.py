"""
Runner script: Digital Presence Collection for all companies in DB.

Usage (from project root):
    poetry run python scripts/run_digital_presence_collection.py

What it does:
    1. Fetches all companies from Snowflake; resolves domain via company_domains or yfinance
    2. Deletes old digital_presence signals per company
    3. Scrapes each company's website (multi-page) for tech stack evidence
    4. Inserts ExternalSignal rows into Snowflake
    5. Prints a summary table of signal counts and scores
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# --- Path setup so app/ and src/ imports work ---
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.pipelines.tech_signals import scrape_tech_signal_inputs, tech_inputs_to_signals
from app.services.snowflake import db

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_all_companies_with_domains() -> list[dict]:
    """Fetch all companies from Snowflake; resolve domain via company_domains or yfinance."""
    rows = db.execute_query(
        """
        SELECT id, ticker, name
        FROM companies
        WHERE is_deleted = FALSE
        ORDER BY ticker
        """
    )
    out = []
    for r in rows:
        company_id = r.get("id")
        ticker = (r.get("ticker") or "").strip().upper()
        if not company_id or not ticker:
            continue
        domain = db.get_domain_for_company(company_id=company_id, ticker=ticker)
        if domain:
            out.append({"company_id": company_id, "ticker": ticker, "domain": domain})
        else:
            print(f"  [SKIP] {ticker}: no domain (company_domains or yfinance)")
    return out


def delete_old_signals(company_id: str, ticker: str) -> int:
    """Delete existing digital_presence signals for a clean re-run."""
    result = db.execute_update(
        """
        DELETE FROM external_signals
        WHERE company_id = %(company_id)s
          AND category = 'digital_presence'
        """,
        {"company_id": company_id}
    )
    return result


def collect_and_store(company_info: dict) -> dict:
    """Run scraper and insert signals. Returns summary dict."""
    ticker     = company_info["ticker"]
    company_id = company_info["company_id"]
    domain     = company_info["domain"]

    print(f"\n{'='*60}")
    print(f"  {ticker} — {domain}")
    print(f"{'='*60}")

    # 1. Delete old signals
    deleted = delete_old_signals(company_id, ticker)
    print(f"  Deleted {deleted} old digital_presence signals")

    # 2. Scrape
    print(f"  Scraping pages...")
    inputs = scrape_tech_signal_inputs(company=ticker, company_domain_or_url=domain)
    print(f"  Pages scanned: {len(inputs)}")
    for inp in inputs:
        print(f"    • {inp.url}")

    # 3. Convert to ExternalSignal objects
    signals = tech_inputs_to_signals(company_id=company_id, items=inputs)

    # 4. Insert into Snowflake
    inserted = db.insert_external_signals(signals)
    print(f"  Inserted {inserted} signals into Snowflake")

    # 5. Summary stats
    scores = [s.score for s in signals]
    avg_score  = round(sum(scores) / len(scores), 2) if scores else 0
    top3_avg   = round(sum(sorted(scores, reverse=True)[:3]) / min(3, len(scores)), 2) if scores else 0

    print(f"  Scores: {scores}")
    print(f"  Avg score: {avg_score}  |  Top-3 avg: {top3_avg}")

    return {
        "ticker":       ticker,
        "domain":       domain,
        "pages_scanned": len(inputs),
        "signals_inserted": inserted,
        "avg_score":    avg_score,
        "top3_avg":     top3_avg,
        "scores":       scores,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  Digital Presence Collection — OrgAIR Pipeline")
    print("="*60)

    companies = get_all_companies_with_domains()
    print(f"  Found {len(companies)} companies with resolvable domains")

    summaries = []
    errors    = []

    for info in companies:
        ticker = info["ticker"]
        try:
            summary = collect_and_store(info)
            summaries.append(summary)
        except Exception as e:
            print(f"\n  [ERROR] {ticker}: {e}")
            import traceback; traceback.print_exc()
            errors.append({"ticker": ticker, "error": str(e)})

    # ── Final Summary Table ──────────────────────────────────────────────────
    print("\n\n" + "="*60)
    print("  COLLECTION SUMMARY")
    print("="*60)
    print(f"  {'Ticker':<8} {'Domain':<28} {'Pages':<8} {'Signals':<10} {'Avg Score':<12} {'Top3 Avg'}")
    print(f"  {'-'*7:<8} {'-'*27:<28} {'-'*5:<8} {'-'*7:<10} {'-'*9:<12} {'-'*8}")

    for s in sorted(summaries, key=lambda x: x["top3_avg"], reverse=True):
        print(
            f"  {s['ticker']:<8} {s['domain']:<28} "
            f"{s['pages_scanned']:<8} {s['signals_inserted']:<10} "
            f"{s['avg_score']:<12} {s['top3_avg']}"
        )

    if errors:
        print(f"\n  FAILED ({len(errors)}):")
        for e in errors:
            print(f"    • {e['ticker']}: {e['error']}")

    print(f"\n  ✓ Done. {len(summaries)} companies collected, {len(errors)} failed.")

    # ── Verify in Snowflake ──────────────────────────────────────────────────
    print("\n  Run this SQL to verify:")
    print("""
    SELECT c.ticker,
           COUNT(*) as signal_count,
           ROUND(AVG(es.normalized_score), 2) as avg_score,
           ROUND(MAX(es.normalized_score), 2) as max_score
    FROM external_signals es
    JOIN companies c ON es.company_id = c.id
    WHERE es.category = 'digital_presence'
    GROUP BY c.ticker
    ORDER BY avg_score DESC;
    """)


if __name__ == "__main__":
    main() 