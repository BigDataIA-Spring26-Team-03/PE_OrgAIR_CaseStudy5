# test_job_signals_debug.py
from app.services.snowflake import SnowflakeService
from app.pipelines.job_signals import scrape_job_postings

TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]


def get_company_row_by_ticker(db: SnowflakeService, ticker: str):
    rows = db.execute_query(
        """
        SELECT id, name, ticker
        FROM companies
        WHERE ticker = %(t)s
        LIMIT 1
        """,
        {"t": ticker},
    )
    return rows[0] if rows else None


def main():
    db = SnowflakeService()

    for t in TICKERS:
        row = get_company_row_by_ticker(db, t)
        if not row:
            print("\n====================")
            print("Ticker:", t, "-> company not found in companies table")
            continue

        company_id = row.get("id") or row.get("ID")
        name = row.get("name") or row.get("NAME") or ""
        aliases = [name, t]

        print("\n====================")
        print("Ticker:", t, "| Name:", name, "| CompanyID:", company_id)

        # ----------------------------
        # NORMAL RUN (with filtering)
        # ----------------------------
        jobs = scrape_job_postings(
            search_query="data engineer",
            sources=["indeed", "google"],
            location="United States",
            max_results_per_source=25,
            target_company_name=name,
            target_company_aliases=aliases,
        )

        print("Jobs returned (WITH filter):", len(jobs))
        for j in jobs[:5]:
            print(" -", j.company, "|", j.title)

        # ----------------------------
        # If ZERO results → Deep Debug
        # ----------------------------
        if len(jobs) == 0:
            print("\n--- DEBUG A: Running WITHOUT company filter ---")

            jobs_no_filter = scrape_job_postings(
                search_query="data engineer",
                sources=["indeed", "google"],
                location="United States",
                max_results_per_source=25,
                target_company_name=None,
                target_company_aliases=None,
            )

            print("Jobs returned (NO filter):", len(jobs_no_filter))
            for j in jobs_no_filter[:10]:
                print(" -", j.company, "|", j.title)

            print("\n--- DEBUG B: Forcing company name into query ---")

            forced_query = f'{name} "data engineer"'

            jobs_forced = scrape_job_postings(
                search_query=forced_query,
                sources=["indeed", "google"],
                location="United States",
                max_results_per_source=50,
                target_company_name=None,
                target_company_aliases=None,
            )

            print("Jobs returned (forced query, NO filter):", len(jobs_forced))
            for j in jobs_forced[:10]:
                print(" -", j.company, "|", j.title)


if __name__ == "__main__":
    main()