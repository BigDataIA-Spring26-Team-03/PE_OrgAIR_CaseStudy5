from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from src.scoring.integration_service import ScoringIntegrationService

logger = logging.getLogger(__name__)


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
    DEFAULT_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]

    async def get_portfolio_view(self, fund_id: str) -> List[PortfolioCompanyView]:
        """
        Build a portfolio-level view for CS5.

        Notes:
        - fund_id is reserved for later use (portfolio composition, weights).
        - Scores all DEFAULT_TICKERS concurrently and returns only successes.
        """
        _ = fund_id  # reserved for later use

        tasks = [self._score_one_ticker(t) for t in self.DEFAULT_TICKERS]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        successful: List[PortfolioCompanyView] = []
        for ticker, view in zip(self.DEFAULT_TICKERS, results):
            if view is None:
                logger.warning("portfolio_score_failed", extra={"ticker": ticker})
                continue
            successful.append(view)
        return successful

    async def _score_one_ticker(self, ticker: str) -> Optional[PortfolioCompanyView]:
        try:
            service = ScoringIntegrationService()

            # The CS5 spec expects score_company(ticker) to be synchronous.
            # In this repo, score_company currently requires a 'sector' argument.
            # We first call it as-specified; if needed, retry with a best-effort sector map.
            def _run() -> dict:
                try:
                    return service.score_company(ticker)  # type: ignore[arg-type]
                except TypeError:
                    sector_map = {
                        "NVDA": "technology",
                        "JPM": "financial_services",
                        "WMT": "retail",
                        "GE": "manufacturing",
                        "DG": "retail",
                    }
                    return service.score_company(ticker, sector_map.get(ticker, "technology"))

            result = await asyncio.to_thread(_run)

            org_air = float(result["final_score"])
            ci = (float(result["confidence"]["ci_lower"]), float(result["confidence"]["ci_upper"]))

            # Flatten dimension scores to Dict[str, float] (score only).
            raw_dims = result.get("dimension_scores", {}) or {}
            dimension_scores: Dict[str, float] = {}
            for dim, payload in raw_dims.items():
                if isinstance(payload, dict) and "score" in payload:
                    dimension_scores[str(dim)] = float(payload["score"])
                elif isinstance(payload, (int, float)):
                    dimension_scores[str(dim)] = float(payload)

            entry_org_air = 45.0

            return PortfolioCompanyView(
                company_id=str(result.get("company_id") or result.get("ticker") or ticker),
                ticker=str(result.get("ticker") or ticker),
                name=ticker,  # placeholder; production would fetch from CS1
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
            logger.warning("score_one_ticker_exception", extra={"ticker": ticker, "error": str(exc)})
            return None


portfolio_data_service = PortfolioDataService()

