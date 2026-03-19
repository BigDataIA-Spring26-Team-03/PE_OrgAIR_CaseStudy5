# test_job_scores_one.py
from app.services.snowflake import SnowflakeService
from app.pipelines.job_signals import scrape_job_postings, job_postings_to_signals, aggregate_job_signals

def get_company(db: SnowflakeService, ticker: str):
    rows = db.execute_query(
        "SELECT id, name, ticker FROM companies WHERE ticker=%(t)s LIMIT 1",
        {"t": ticker},
    )
    return rows[0] if rows else None

def main():
    ticker = "WMT"  # change to DE/CAT/JPM etc.
    db = SnowflakeService()

    row = get_company(db, ticker)
    if not row:
        print("Company not found:", ticker)
        return

    company_id = row.get("id") or row.get("ID")
    name = row.get("name") or row.get("NAME") or ""

    jobs = scrape_job_postings(
        search_query="data engineer",
        sources=["indeed", "google"],
        location="United States",
        max_results_per_source=25,
        target_company_name=name,
        target_company_aliases=[name, ticker],
    )

    print("Jobs:", len(jobs))
    for j in jobs[:5]:
        print(" -", j.company, "|", j.title)

    signals = job_postings_to_signals(company_id, jobs)
    print("\nSignals:", len(signals))
    if signals:
        print("Sample signal score(s):", [s.score for s in signals[:10]])

    summary = aggregate_job_signals(company_id, signals)
    print("\nSummary:")
    print(" jobs_score =", summary.jobs_score)
    print(" composite_score =", summary.composite_score)

if __name__ == "__main__":
    main()