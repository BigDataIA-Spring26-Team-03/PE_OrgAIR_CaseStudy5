from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from src.scoring.integration_service import ScoringIntegrationService
from src.services.integration.cs1_client import CS1Client
from src.services.integration.cs3_client import CS3Client
from src.services.tracking.assessment_history import (
    create_history_service,
)

logger = logging.getLogger(__name__)

# API base for fetching company list 
_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")

# Cosmetic sector hints used only when score_company() requires a sector argument
# and the API response doesn't include one. This does NOT restrict which companies
# No ticker is rejected or blocked by this dict.
_SECTOR_HINTS: Dict[str, str] = {
    "NVDA": "technology", "JPM": "financial_services", "WMT": "retail",
    "GE": "manufacturing", "DG": "retail", "MSFT": "technology", "AAPL": "technology",
}


@dataclass(frozen=True)
class PortfolioCompanyView:
    company_id: str
    ticker: str
    name: str
    sector: str
    org_air: float
    vr_score: float
    hr_score: float
    synergy_score: float
    dimension_scores: Dict[str, float]
    confidence_interval: tuple
    entry_org_air: float
    delta_since_entry: float
    evidence_count: int


class PortfolioDataService:
    async def _get_tickers(self, fund_id: str) -> List[str]:
        """
        Resolve portfolio tickers dynamically.
        1. Try GET /api/v1/companies (limit=200) — use all tickers from Snowflake.
        2. Fallback: PORTFOLIO_TICKERS env var (comma-separated, e.g. NVDA,JPM,WMT).
        3. Last resort: empty list.
        """
        _ = fund_id  # reserved for fund-specific composition
        try:
            resp = requests.get(
                f"{_API_BASE}/api/v1/companies",
                params={"limit": 500, "offset": 0},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            companies = data if isinstance(data, list) else data.get("items", data.get("results", []))
            tickers = [
                str(c.get("ticker", "")).strip().upper()
                for c in (companies or [])
                if c.get("ticker")
            ]
            if tickers:
                logger.info("portfolio_tickers_from_api count=%d tickers=%s", len(tickers), tickers[:10])
                return tickers
        except Exception as exc:
            logger.warning("portfolio_tickers_api_failed: %s", exc)

        fallback = os.getenv("PORTFOLIO_TICKERS", "").strip()
        if fallback:
            tickers = [t.strip().upper() for t in fallback.split(",") if t.strip()]
            if tickers:
                logger.info("portfolio_tickers_from_env count=%d", len(tickers))
                return tickers

        logger.error(
            "portfolio_tickers_empty: CS1 API returned no companies "
            "and PORTFOLIO_TICKERS env var is not set. "
            "Returning empty list — add companies via the CS1 API "
            "or set the PORTFOLIO_TICKERS environment variable."
        )
        return []

    async def get_portfolio_view(self, fund_id: str) -> List[PortfolioCompanyView]:
        """
        Build a portfolio-level view for CS5.
        Tickers are resolved dynamically from the companies API or PORTFOLIO_TICKERS env.
        """
        tickers = await self._get_tickers(fund_id)
        if not tickers:
            return []

        tasks = [self._score_one_ticker(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful: List[PortfolioCompanyView] = []
        for ticker, view in zip(tickers, results):
            if isinstance(view, BaseException):
                logger.warning("portfolio_score_exception", extra={"ticker": ticker, "error": str(view)})
                continue
            if view is None:
                logger.warning("portfolio_score_failed", extra={"ticker": ticker})
                continue
            successful.append(view)
        return successful

    async def _score_one_ticker(self, ticker: str) -> Optional[PortfolioCompanyView]:
        try:
            service = ScoringIntegrationService()

            def _run() -> dict:
                try:
                    return service.score_company(ticker)
                except TypeError:
                    sector = _SECTOR_HINTS.get(ticker.upper(), "technology")
                    return service.score_company(ticker, sector)

            result = await asyncio.to_thread(_run)

            org_air = float(result["final_score"])
            ci = (
                float(result["confidence"]["ci_lower"]),
                float(result["confidence"]["ci_upper"]),
            )

            raw_dims = result.get("dimension_scores", {}) or {}
            dimension_scores: Dict[str, float] = {}
            for dim, payload in raw_dims.items():
                if isinstance(payload, dict) and "score" in payload:
                    dimension_scores[str(dim)] = float(payload["score"])
                elif isinstance(payload, (int, float)):
                    dimension_scores[str(dim)] = float(payload)

            entry_org_air = 45.0

            # Record history snapshot — runs in same thread as _score_one_ticker
            # Uses asyncio since we are already inside an async method
            try:
                async with CS1Client() as _cs1:
                    async with CS3Client() as _cs3:
                        _hist = create_history_service(_cs1, _cs3)
                        await _hist.record_assessment(
                            company_id=ticker,
                            assessor_id="portfolio-data-service",
                            assessment_type="full",
                        )
            except Exception as hist_exc:
                logger.warning(
                    "portfolio_history_record_failed: ticker=%s error=%s",
                    ticker, hist_exc,
                )

            # Fetch real company name from CS1
            company_name = ticker  # safe default if CS1 call fails
            try:
                from src.services.integration.cs1_client import CS1Client
                async with CS1Client() as cs1:
                    company = await cs1.get_company(ticker)
                    if company and company.name:
                        company_name = company.name
            except Exception as name_exc:
                logger.warning(
                    "cs1_name_lookup_failed: ticker=%s error=%s — "
                    "falling back to ticker as name",
                    ticker, name_exc,
                )

            return PortfolioCompanyView(
                company_id=str(
                    result.get("company_id") or result.get("ticker") or ticker
                ),
                ticker=str(result.get("ticker") or ticker),
                name=company_name,
                sector=str(result.get("sector") or ""),
                org_air=org_air,
                vr_score=float(result["vr_score"]),
                hr_score=float(result["hr_score"]),
                synergy_score=float(result["synergy_score"]),
                dimension_scores=dimension_scores,
                confidence_interval=ci,
                entry_org_air=entry_org_air,
                delta_since_entry=org_air - entry_org_air,
                evidence_count=int(result.get("evidence_count") or 0),
            )
        except Exception as exc:
            logger.warning(
                "score_one_ticker_exception",
                extra={"ticker": ticker, "error": str(exc)},
            )
            return None


portfolio_data_service = PortfolioDataService()

