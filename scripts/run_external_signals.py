from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from app.services.snowflake import SnowflakeService
from app.pipelines.external_signals_orchestrator import run_external_signals_pipeline

# Digital Presence (REAL)
from app.pipelines.tech_signals import tech_inputs_to_signals, scrape_tech_signal_inputs

# Patents + Leadership (still mock for now)
from app.pipelines.patent_signals import patent_inputs_to_signals, scrape_patent_signal_inputs_mock
from app.pipelines.leadership_signals import scrape_leadership_profiles_mock


def _build_job_aliases(company_name: str, ticker: Optional[str]) -> List[str]:
    job_aliases: List[str] = [company_name]
    if ticker:
        job_aliases.append(ticker)

    # Add common “brand/subsidiary” aliases (minimal, compliance-safe)
    SPECIAL_ALIASES = {
        "UNH": ["UnitedHealth", "United Health", "UnitedHealthcare", "UHG", "Optum"],
        "JPM": ["JPMorgan", "JP Morgan", "Chase", "JPMC"],
        "GS": ["Goldman Sachs", "Goldman"],
        "WMT": ["Walmart", "Walmart Global Tech", "Walmart Inc", "Walmart Inc."],
        "TGT": ["Target", "Target Corporation"],
        "ADP": ["ADP", "Automatic Data Processing"],
        "PAYX": ["Paychex", "Paychex Inc", "Paychex Inc."],
        "HCA": ["HCA", "HCA Healthcare", "HCA Healthcare Inc", "HCA Healthcare Inc."],
        "CAT": ["Caterpillar", "CAT", "Caterpillar Inc", "Caterpillar Inc."],
        "DE": ["Deere", "John Deere", "Deere & Company"],
    }

    if ticker:
        job_aliases += SPECIAL_ALIASES.get(ticker, [])

    # de-dupe while preserving order (case-insensitive)
    seen = set()
    cleaned: List[str] = []
    for a in job_aliases:
        if not a:
            continue
        key = a.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(a.strip())

    return cleaned


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run External Signals pipeline (Jobs(company-specific) + Digital Presence(real) + Patents(mock) + Leadership(mock))."
    )
    parser.add_argument("--company-id", required=True, help="Must be an existing companies.id in Snowflake")
    parser.add_argument("--query", required=True, help="Job search query (e.g., 'machine learning engineer')")
    parser.add_argument("--location", default="United States")
    parser.add_argument("--sources", default="indeed,google", help="Comma-separated (indeed,google)")
    parser.add_argument("--max-per-source", type=int, default=3)

    args = parser.parse_args()
    sources: List[str] = [s.strip() for s in args.sources.split(",") if s.strip()]

    # ✅ IMPORTANT: Create ONE SnowflakeService and reuse it
    svc = SnowflakeService()

    # -------------------
    # A) Fetch company name + ticker + domain from Snowflake (real source)
    # -------------------
    company = svc.get_company(args.company_id)
    if not company:
        print(f"❌ Company not found for company_id={args.company_id}. Check companies table.", file=sys.stderr)
        sys.exit(1)

    company_name: str = company.get("name") or ""
    if not company_name:
        print(f"❌ Company name is missing for company_id={args.company_id}.", file=sys.stderr)
        sys.exit(1)

    company_ticker: Optional[str] = company.get("ticker") or None

    domain_url: Optional[str] = svc.get_primary_domain_by_company_id(args.company_id)
    if not domain_url:
        print(
            f"❌ No primary domain found in company_domains for company_id={args.company_id}. "
            f"Insert a row into company_domains first.",
            file=sys.stderr,
        )
        sys.exit(1)

    # -------------------
    # A2) Build hiring aliases (name + ticker + known brands/subsidiaries)
    # -------------------
    job_aliases: List[str] = _build_job_aliases(company_name, company_ticker)

    # -------------------
    # B) Digital Presence (REAL) using company_name + domain_url
    # -------------------
    tech_items = scrape_tech_signal_inputs(
        company=company_name,
        company_domain_or_url=domain_url,
    )
    tech_signals = tech_inputs_to_signals(company_id=args.company_id, items=tech_items)

    # -------------------
    # C) Patents (MOCK for now)
    # -------------------
    patent_items = scrape_patent_signal_inputs_mock(company=company_name)
    patent_signals = patent_inputs_to_signals(company_id=args.company_id, items=patent_items)

    # -------------------
    # D) Leadership (MOCK for now)
    # -------------------
    leadership_profiles = scrape_leadership_profiles_mock(company=company_name)

    # -------------------
    # E) Orchestrator runs jobs scraping (real via JobSpy) + aggregates everything
    # ✅ company-specific hiring uses company_name + aliases
    # -------------------
    result = run_external_signals_pipeline(
        company_id=args.company_id,
        jobs_search_query=args.query,
        jobs_sources=sources,
        jobs_location=args.location,
        jobs_max_results_per_source=args.max_per_source,
        jobs_target_company_name=company_name,
        jobs_target_company_ticker=company_ticker,
        jobs_target_company_aliases=job_aliases,  # ✅ THIS is what you were missing
        tech_items=tech_items,
        patent_items=patent_items,
        leadership_profiles=leadership_profiles,
    )

    # -------------------
    # F) Write to Snowflake (same svc)
    # -------------------
    all_signals = result.jobs_signals + result.tech_signals + result.patent_signals + result.leadership_signals
    n = svc.insert_external_signals(all_signals)
    svc.upsert_company_signal_summary(result.summary, signal_count=n)

    print(f"\n✅ Inserted {n} external_signals rows into Snowflake")

    print("\n=== External Signals Run ===")
    print("company_id:", result.company_id)
    print("company_name:", company_name)
    print("ticker:", company_ticker)
    print("domain_url:", domain_url)
    print("job_aliases:", job_aliases)
    print("jobs_signals:", len(result.jobs_signals))
    print("digital_presence_signals:", len(result.tech_signals))
    print("patent_signals:", len(result.patent_signals))
    print("leadership_signals:", len(result.leadership_signals))
    print("SUMMARY:", result.summary)

    # Debug
    print("\n[debug] digital_presence_items(real):", len(tech_items))
    print("[debug] digital_presence_signals(real):", len(tech_signals))
    print("[debug] patent_signals(mock):", len(patent_signals))
    print("[debug] leadership_profiles(mock):", len(leadership_profiles))


if __name__ == "__main__":
    main()