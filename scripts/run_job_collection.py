"""
Runner: Technology Hiring signal collection for all 5 portfolio companies.

Usage (from project root):
    poetry run python scripts/run_jobs_collection.py

What it does:
    1. Fetches company_id from Snowflake for each ticker
    2. Scrapes job postings via JobSpy (Indeed + Google) using AI-specific queries
    3. Scores each posting for AI relevance
    4. Inserts technology_hiring signals into Snowflake
    5. Prints ranked summary table

Notes:
    - Uses Indeed + Google (LinkedIn rate-limits aggressively)
    - Searches last 30 days of postings
    - Filters to company-specific postings only via alias matching
    - Cleans old technology_hiring signals before inserting fresh ones
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

# --- Path setup ---
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.pipelines.job_signals import scrape_job_postings, job_postings_to_signals
from app.services.snowflake import db

# ── Company Config ─────────────────────────────────────────────────────────────
# Each entry: ticker, company_name, search_query, aliases
# Search query is AI/ML specific to surface relevant hiring signals
# Aliases ensure filtering only returns this company's postings

COMPANY_CONFIGS = [
    {
        "ticker":  "NVDA",
        "name":    "NVIDIA",                        # JobSpy sees "NVIDIA" not "NVIDIA Corporation"
        "query":   "machine learning engineer NVIDIA",
        "aliases": ["NVIDIA", "NVIDIA Corporation", "NVIDIA Corp", "nVidia"],
    },
    {
        "ticker":  "JPM",
        "name":    "JPMorgan Chase",
        "query":   "machine learning engineer AI data science JPMorgan",
        "aliases": ["JPMorgan", "JP Morgan", "JPMorgan Chase", "JPMC", "Chase"],
    },
    {
        "ticker":  "WMT",
        "name":    "Walmart",
        "query":   "machine learning engineer AI data science Walmart",
        "aliases": ["Walmart", "Walmart Inc", "Walmart Global Tech"],
    },
    {
        "ticker":  "GE",
        "name":    "GE Aerospace",
        "query":   "engineer software technology GE Aerospace", 
        "aliases": ["GE Aerospace", "GE Vernova", "GE HealthCare",
                    "General Electric", "GE Research", "GE"],
    },
    {
        "ticker":  "DG",
        "name":    "Dollar General",
        "query":   "software engineer data Dollar General",
        "aliases": ["Dollar General", "Dollar General Corporation"],
    },
]

SOURCES           = ["indeed", "google"]
LOCATION          = "United States"
MAX_PER_SOURCE    = 15   # 15 per source × 2 sources = up to 30 postings per company
HOURS_OLD         = 24 * 30  # last 30 days

# Set to True to skip alias filtering and print raw company names from JobSpy
# Use this to debug when a company returns 0 results
DEBUG_NO_FILTER   = False


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_company_id(ticker: str) -> Optional[str]:
    rows = db.execute_query(
        "SELECT id FROM companies WHERE ticker = %(ticker)s",
        {"ticker": ticker}
    )
    return rows[0].get("id") if rows else None


def delete_old_hiring_signals(company_id: str, ticker: str) -> int:
    deleted = db.execute_update(
        """
        DELETE FROM external_signals
        WHERE company_id = %(company_id)s
          AND category = 'technology_hiring'
        """,
        {"company_id": company_id}
    )
    print(f"  Deleted {deleted} old technology_hiring signals")
    return deleted


def collect_and_store(config: dict) -> dict:
    ticker     = config["ticker"]
    name       = config["name"]
    query      = config["query"]
    aliases    = config["aliases"]

    print(f"\n{'='*60}")
    print(f"  {ticker} — {name}")
    print(f"  Query: \"{query}\"")
    print(f"{'='*60}")

    # 1. Get company_id
    company_id = get_company_id(ticker)
    if not company_id:
        print(f"  [ERROR] {ticker} not found in companies table — skipping")
        return {"ticker": ticker, "error": "company not found"}

    # 2. Delete old signals
    delete_old_hiring_signals(company_id, ticker)

    # 3. Scrape
    print(f"  Scraping job postings (sources: {SOURCES}, max: {MAX_PER_SOURCE}/source)...")
    try:
        if DEBUG_NO_FILTER:
            # Skip alias filtering — shows raw company names JobSpy returns
            jobs = scrape_job_postings(
                search_query=query,
                sources=SOURCES,
                location=LOCATION,
                max_results_per_source=MAX_PER_SOURCE,
                hours_old=HOURS_OLD,
                target_company_name=None,
                target_company_aliases=None,
            )
            if jobs:
                from collections import Counter
                raw_names = Counter(j.company for j in jobs).most_common(10)
                print(f"  [DEBUG] Raw company names returned by JobSpy:")
                for cname, cnt in raw_names:
                    print(f"    {cnt:>3}x  '{cname}'")
        else:
            jobs = scrape_job_postings(
                search_query=query,
                sources=SOURCES,
                location=LOCATION,
                max_results_per_source=MAX_PER_SOURCE,
                hours_old=HOURS_OLD,
                target_company_name=name,
                target_company_aliases=aliases,
            )
    except Exception as e:
        print(f"  [ERROR] Scraping failed: {e}")
        return {"ticker": ticker, "error": str(e)}

    print(f"  Job postings found: {len(jobs)}")

    if not jobs:
        print(f"  [WARN] No postings found — check aliases or try broader query")
        return {
            "ticker":    ticker,
            "jobs_found": 0,
            "inserted":  0,
            "avg_score": 0,
            "top_jobs":  [],
        }

    # 4. Convert to signals
    signals = job_postings_to_signals(company_id=company_id, jobs=jobs)
    scores  = [s.score for s in signals]
    avg_score = round(sum(scores) / len(scores), 2) if scores else 0

    # Show top 5 most relevant postings
    top = sorted(zip(scores, [j.title for j in jobs]), reverse=True)[:5]
    print(f"  Top postings by AI relevance:")
    for score, title in top:
        print(f"    [{score:>3}] {title}")

    # 5. Insert into Snowflake
    inserted = db.insert_external_signals(signals)
    print(f"  Inserted {inserted} signals | Avg score: {avg_score}")

    return {
        "ticker":     ticker,
        "jobs_found": len(jobs),
        "inserted":   inserted,
        "avg_score":  avg_score,
        "top_jobs":   [t for _, t in top],
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  Technology Hiring Collection — OrgAIR Pipeline")
    print("="*60)

    summaries = []
    errors    = []

    for config in COMPANY_CONFIGS:
        try:
            result = collect_and_store(config)
            if "error" in result:
                errors.append(result)
            else:
                summaries.append(result)
        except Exception as e:
            import traceback; traceback.print_exc()
            errors.append({"ticker": config["ticker"], "error": str(e)})

    # ── Summary Table ──────────────────────────────────────────────────────────
    print("\n\n" + "="*60)
    print("  COLLECTION SUMMARY")
    print("="*60)
    print(f"  {'Ticker':<8} {'Jobs Found':<14} {'Inserted':<12} {'Avg Score'}")
    print(f"  {'-'*7:<8} {'-'*11:<14} {'-'*8:<12} {'-'*9}")

    for s in sorted(summaries, key=lambda x: x.get("avg_score", 0), reverse=True):
        print(
            f"  {s['ticker']:<8} {s.get('jobs_found', 0):<14} "
            f"{s.get('inserted', 0):<12} {s.get('avg_score', 0)}"
        )

    if errors:
        print(f"\n  FAILED ({len(errors)}):")
        for e in errors:
            print(f"    • {e['ticker']}: {e.get('error', 'unknown')}")

    print(f"\n  ✓ Done. {len(summaries)} companies collected, {len(errors)} failed.")

    # ── Verify SQL ─────────────────────────────────────────────────────────────
    print("\n  Verify in Snowflake:")
    print("""
    SELECT c.ticker,
           COUNT(*) as signal_count,
           ROUND(AVG(es.normalized_score), 2) as avg_score,
           ROUND(MAX(es.normalized_score), 2) as max_score
    FROM external_signals es
    JOIN companies c ON es.company_id = c.id
    WHERE c.ticker IN ('NVDA','JPM','WMT','GE','DG')
      AND es.category = 'technology_hiring'
    GROUP BY c.ticker
    ORDER BY avg_score DESC;
    """)


if __name__ == "__main__":
    main()