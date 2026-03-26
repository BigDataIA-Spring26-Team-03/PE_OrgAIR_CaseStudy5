"""
Unified Portfolio Data Service — Integrates CS1, CS2, CS3, CS4.

This is the ONLY way to get data for agents and dashboards.
ALL data comes from YOUR CS1-CS4 implementations.

Portfolio composition comes from CS1 via fund_id.
Scores come from CS3 (fast path) or full pipeline (slow path).
Evidence counts come from CS2.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from src.services.integration.cs1_client import CS1Client
from src.services.integration.cs2_client import CS2Client
from src.services.integration.cs3_client import CS3Client
from src.services.tracking.assessment_history import (
    create_history_service,
)
from src.scoring.integration_service import ScoringIntegrationService

logger = logging.getLogger(__name__)

_SECTOR_HINTS: Dict[str, str] = {
    "NVDA": "technology",
    "JPM":  "financial_services",
    "WMT":  "retail",
    "GE":   "manufacturing",
    "DG":   "retail",
    "MSFT": "technology",
    "AAPL": "technology",
}


@dataclass(frozen=True)
class PortfolioCompanyView:
    """Complete view of a portfolio company from CS1-CS4."""
    company_id:          str
    ticker:              str
    name:                str
    sector:              str
    org_air:             float
    vr_score:            float
    hr_score:            float
    synergy_score:       float
    dimension_scores:    Dict[str, float]
    confidence_interval: tuple
    entry_org_air:       float
    delta_since_entry:   float
    evidence_count:      int


class PortfolioDataService:
    """
    Unified data service integrating CS1-CS4.
    Matches the professor's reference implementation.
    """

    async def get_portfolio_view(
        self,
        fund_id: str,
    ) -> List[PortfolioCompanyView]:
        """
        Load portfolio from CS1, scores from CS3, evidence from CS2.

        Exactly matches professor's reference spec:
          1. cs1.get_portfolio_companies(fund_id)
          2. cs3.get_assessment(company.ticker)
          3. cs2.get_evidence(company.ticker)
          4. Build PortfolioCompanyView
        """
        # Step 1 — Get portfolio companies from CS1
        async with CS1Client() as cs1:
            companies = await cs1.get_portfolio_companies(fund_id)

        if not companies:
            logger.warning(
                "get_portfolio_view: no companies found for fund_id=%s",
                fund_id,
            )
            return []

        logger.info(
            "get_portfolio_view: scoring %d companies for fund=%s",
            len(companies), fund_id,
        )

        # Step 2 — Score all companies in parallel
        tasks = [self._build_company_view(company) for company in companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful: List[PortfolioCompanyView] = []
        for company, result in zip(companies, results):
            if isinstance(result, BaseException):
                logger.warning(
                    "portfolio_view_failed: ticker=%s error=%s",
                    company.ticker, result,
                )
                continue
            if result is not None:
                successful.append(result)

        return successful

    async def _build_company_view(
        self,
        company,
    ) -> Optional[PortfolioCompanyView]:
        """
        Build one PortfolioCompanyView for a company.
        Uses CS3 fast path, falls back to full pipeline.
        """
        ticker = company.ticker

        try:
            # Step 2a — Get score from CS3 (fast path)
            assessment = None
            try:
                async with CS3Client() as cs3:
                    assessment = await cs3.get_assessment(ticker)
                logger.info(
                    "portfolio_cs3_hit: ticker=%s org_air=%.1f",
                    ticker, assessment.org_air_score,
                )
            except Exception as cs3_exc:
                logger.info(
                    "portfolio_cs3_miss: ticker=%s — running full pipeline. error=%s",
                    ticker, cs3_exc,
                )

            # Step 2b — Full pipeline if CS3 missed
            if assessment is None:
                service = ScoringIntegrationService()

                def _run() -> dict:
                    try:
                        return service.score_company(ticker)
                    except TypeError:
                        sector = _SECTOR_HINTS.get(ticker.upper(), "technology")
                        return service.score_company(ticker, sector)

                raw = await asyncio.to_thread(_run)

                class _FakeAssessment:
                    org_air_score       = float(raw["final_score"])
                    vr_score            = float(raw["vr_score"])
                    hr_score            = float(raw["hr_score"])
                    synergy_score       = float(raw["synergy_score"])
                    confidence_interval = (
                        float(raw["confidence"]["ci_lower"]),
                        float(raw["confidence"]["ci_upper"]),
                    )
                    evidence_count      = int(raw.get("evidence_count") or 0)
                    dimension_scores    = {}

                for dim, payload in (raw.get("dimension_scores", {}) or {}).items():
                    score = (
                        float(payload["score"])
                        if isinstance(payload, dict)
                        else float(payload)
                    )

                    class _DS:
                        pass
                    ds = _DS()
                    ds.score = score

                    class _D:
                        pass
                    d = _D()
                    d.value = str(dim)
                    _FakeAssessment.dimension_scores[d] = ds

                assessment = _FakeAssessment()

            # Step 2c — Get evidence count from CS2
            evidence_count = 0
            try:
                async with CS2Client() as cs2:
                    evidence = await cs2.get_evidence(ticker)
                    evidence_count = len(evidence)
            except Exception as cs2_exc:
                logger.warning(
                    "portfolio_cs2_failed: ticker=%s error=%s",
                    ticker, cs2_exc,
                )

            # Step 2d — Get entry score from history + real company name
            entry_org_air = assessment.org_air_score
            company_name  = company.name or ticker

            try:
                async with CS1Client() as cs1:
                    async with CS3Client() as cs3:
                        hist = create_history_service(cs1, cs3)
                        trend = await hist.calculate_trend(ticker)
                        entry_org_air = float(trend.entry_org_air)
                        await hist.record_assessment(
                            company_id=ticker,
                            assessor_id="portfolio-data-service",
                            assessment_type="full",
                        )
            except Exception as hist_exc:
                logger.warning(
                    "portfolio_history_failed: ticker=%s error=%s",
                    ticker, hist_exc,
                )

            # Step 3 — Build dimension scores dict
            dimension_scores: Dict[str, float] = {}
            for d, s in assessment.dimension_scores.items():
                key = d.value if hasattr(d, "value") else str(d)
                dimension_scores[key] = float(s.score)

            # Step 4 — Build and return PortfolioCompanyView
            return PortfolioCompanyView(
                company_id=str(getattr(company, "company_id", ticker)),
                ticker=ticker,
                name=company_name,
                sector=str(
                    company.sector.value
                    if hasattr(company.sector, "value")
                    else company.sector or ""
                ),
                org_air=assessment.org_air_score,
                vr_score=assessment.vr_score,
                hr_score=assessment.hr_score,
                synergy_score=assessment.synergy_score,
                dimension_scores=dimension_scores,
                confidence_interval=assessment.confidence_interval,
                entry_org_air=entry_org_air,
                delta_since_entry=(assessment.org_air_score - entry_org_air),
                evidence_count=evidence_count,
            )

        except Exception as exc:
            logger.warning(
                "build_company_view_failed: ticker=%s error=%s",
                ticker, exc,
            )
            return None


# Singleton instance
portfolio_data_service = PortfolioDataService()
