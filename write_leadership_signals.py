from app.services.snowflake import db
from app.pipelines.leadership_signals import (
    scrape_leadership_profiles, leadership_profiles_to_signals,
    leadership_profiles_to_aggregated_signal, calculate_leadership_score_0_1
)

targets = [
    ("MSFT", "Microsoft",        "https://microsoft.com"),
    ("AMZN", "Amazon",           "https://ir.aboutamazon.com/officers-and-directors/"),
    ("META", "Meta",             "https://investor.atmeta.com/leadership-and-governance/"),
    ("NVDA", "NVIDIA",           "https://nvidianews.nvidia.com/bios"),
    ("GS",   "Goldman Sachs",    "https://www.goldmansachs.com/our-firm/leadership"),
    ("CAT",  "Caterpillar Inc.", "https://www.caterpillar.com/en/company/governance/officers.html"),
    ("DE",   "Deere & Company",  "https://www.deere.com/en/our-company/leadership/"),
]

for ticker, company_name, url in targets:
    print(f"--- {ticker} ---")
    try:
        rows = db.execute_query(
            "SELECT id FROM companies WHERE ticker = %(t)s AND is_deleted = FALSE",
            {"t": ticker}
        )
        if not rows:
            print(f"  SKIP — not found in DB")
            continue
        company_id = str(rows[0].get("ID") or rows[0].get("id"))

        profiles = scrape_leadership_profiles(company=company_name, base_url=url, ticker=ticker)
        profiles = [p for p in profiles if 2 <= len(p.name.split()) <= 5 and len(p.name) < 50]
        score = int(calculate_leadership_score_0_1(profiles) * 100)
        print(f"  profiles={len(profiles)} score={score}")

        if not profiles:
            print(f"  SKIP — 0 profiles after filter")
            continue

        signals = leadership_profiles_to_signals(company_id, profiles)
        agg = leadership_profiles_to_aggregated_signal(company_id, profiles)

        deleted = db.execute_update(
            "DELETE FROM external_signals WHERE company_id = %(cid)s AND category = %(cat)s",
            {"cid": company_id, "cat": "leadership_signals"}
        )
        print(f"  deleted {deleted} old signals")

        inserted = db.insert_external_signals(signals + [agg])
        print(f"  inserted {inserted} signals (agg_score={agg.score})")

    except Exception as e:
        import traceback
        print(f"  FAILED: {e}")
        traceback.print_exc()
        continue

db.close()
print("Done.")
