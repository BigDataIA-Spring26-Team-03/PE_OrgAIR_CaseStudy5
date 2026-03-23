"""
scripts/backfill_history.py
----------------------------
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

# Ensure project root is on the path regardless of how the script is run
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from src.services.integration.cs1_client import CS1Client
from src.services.integration.cs3_client import CS3Client
from src.services.tracking.assessment_history import create_history_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fallback company list — used only when CS1 API returns nothing
# ---------------------------------------------------------------------------
_FALLBACK_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]

_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _load_tickers(cs1: CS1Client) -> list[str]:
    """
    Load tickers using the same 3-fallback logic as portfolio_data_service:
      1. CS1 API  — GET /api/v1/companies
      2. Env var  — PORTFOLIO_TICKERS=NVDA,JPM,WMT
      3. Hardcoded fallback — the original 5 CS3 companies
    """
    # Attempt 1 — CS1 API
    try:
        companies = await cs1.list_companies(limit=200)
        tickers = [
            c.ticker for c in companies
            if c.ticker
        ]
        if tickers:
            logger.info(
                "Loaded %d tickers from CS1 API: %s",
                len(tickers), tickers,
            )
            return tickers
    except Exception as exc:
        logger.warning("CS1 API unavailable: %s", exc)

    # Attempt 2 — environment variable
    env_tickers = os.getenv("PORTFOLIO_TICKERS", "").strip()
    if env_tickers:
        tickers = [t.strip().upper() for t in env_tickers.split(",") if t.strip()]
        if tickers:
            logger.info(
                "Loaded %d tickers from PORTFOLIO_TICKERS env var: %s",
                len(tickers), tickers,
            )
            return tickers

    # Attempt 3 — hardcoded fallback
    logger.warning(
        "CS1 API returned nothing and PORTFOLIO_TICKERS is not set. "
        "Using hardcoded fallback: %s",
        _FALLBACK_TICKERS,
    )
    return _FALLBACK_TICKERS


async def _backfill_one(
    ticker: str,
    history_service,
) -> dict:
    """
    Record one history snapshot for a single ticker.
    Returns a result dict describing success or failure.
    """
    try:
        snapshot = await history_service.record_assessment(
            company_id=ticker,
            assessor_id="backfill-script",
            assessment_type="full",
        )
        return {
            "ticker":    ticker,
            "status":    "success",
            "org_air":   float(snapshot.org_air),
            "timestamp": snapshot.timestamp.isoformat(),
        }
    except Exception as exc:
        logger.error("Failed to backfill %s: %s", ticker, exc)
        return {
            "ticker":  ticker,
            "status":  "failed",
            "error":   str(exc),
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print()
    print("=" * 60)
    print("  PE Org-AI-R — Assessment History Backfill")
    print("=" * 60)
    print()

    async with CS1Client() as cs1:
        async with CS3Client() as cs3:
            history_service = create_history_service(cs1, cs3)

            # Step 1 — load tickers
            tickers = await _load_tickers(cs1)
            print(f"Companies to backfill: {tickers}")
            print()

            # Step 2 — backfill all tickers concurrently
            tasks = [
                _backfill_one(ticker, history_service)
                for ticker in tickers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=False)

    # Step 3 — print summary
    print()
    print("=" * 60)
    print("  Backfill Summary")
    print("=" * 60)
    print()

    succeeded = []
    failed    = []

    for r in results:
        if r["status"] == "success":
            succeeded.append(r)
            print(
                f"  ✅  {r['ticker']:<6}  "
                f"Org-AI-R: {r['org_air']:.2f}  "
                f"recorded at {r['timestamp']}"
            )
        else:
            failed.append(r)
            print(
                f"  ❌  {r['ticker']:<6}  "
                f"FAILED — {r.get('error', 'unknown error')}"
            )

    print()
    print(f"  Succeeded : {len(succeeded)}")
    print(f"  Failed    : {len(failed)}")
    print()

    if failed:
        print("  ⚠️  Some companies failed. Common causes:")
        print("     - CS3 API not running (start FastAPI first)")
        print("     - Company not yet scored in CS3")
        print("     - Snowflake env vars not set")
        print()
        print("  Re-run this script after fixing the issues above.")
        print("  It is safe to re-run — no data is deleted.")
    else:
        print("  🎉 All companies backfilled successfully.")
        print()
        print("  Next steps:")
        print("  1. Verify rows in Snowflake:")
        print("     SELECT * FROM assessment_history ORDER BY assessed_at;")
        print("  2. From now on, every score call records history")
        print("     automatically via the connected AssessmentHistoryService.")

    print()
    print("=" * 60)
    print()


if __name__ == "__main__":
    asyncio.run(main())
