#!/usr/bin/env python3
"""
Quick test to verify Snowflake credentials work.
Run: poetry run python scripts/test_snowflake_connection.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from app.config import settings
import snowflake.connector

def main():
    print("Testing Snowflake connection...")
    print(f"  Account: {settings.SNOWFLAKE_ACCOUNT}")
    print(f"  User: {settings.SNOWFLAKE_USER}")
    print(f"  Database: {settings.SNOWFLAKE_DATABASE}")
    print()

    try:
        conn = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            role="ACCOUNTADMIN",  # Same as app
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        print("SUCCESS: Connected to Snowflake!")
        print(f"  SELECT 1 => {result}")
        return 0
    except Exception as e:
        print(f"FAILED: {e}")
        print()
        print("Things to check:")
        print("  1. Log in at https://app.snowflake.com with the same credentials")
        print("  2. If password expired, reset it in Snowflake: Admin > Users > Reset Password")
        print("  3. Account format: try with region, e.g. AULIZOV-DXC76868.us-east-1")
        print("  4. Ensure .env has correct SNOWFLAKE_* values")
        return 1

if __name__ == "__main__":
    sys.exit(main())
