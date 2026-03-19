# test_check_external_signals.py
from app.services.snowflake import SnowflakeService

def main():
    db = SnowflakeService()
    ticker = "WMT"

    rows = db.execute_query("""
        SELECT category, COUNT(*) as n, AVG(normalized_score) as avg_norm, AVG(confidence) as avg_conf
        FROM external_signals es
        JOIN companies c ON es.company_id = c.id
        WHERE c.ticker = %(t)s
        GROUP BY category
        ORDER BY n DESC
    """, {"t": ticker})

    print(rows)

if __name__ == "__main__":
    main()