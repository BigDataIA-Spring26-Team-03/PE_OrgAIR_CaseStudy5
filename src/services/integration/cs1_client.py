# src/services/integration/cs1_client.py
from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Sector(str, enum.Enum):
    """
    PE-relevant industry sectors.
    """
    TECHNOLOGY          = "Technology"
    HEALTHCARE          = "Healthcare"
    FINANCIAL_SERVICES  = "Financial Services"
    INDUSTRIALS         = "Industrials"
    CONSUMER            = "Consumer"
    ENERGY              = "Energy"
    REAL_ESTATE         = "Real Estate"
    OTHER               = "Other"

    @classmethod
    def from_raw(cls, raw: str) -> Optional["Sector"]:
        if not raw:
            return None
        normalized = raw.strip().lower().replace("  ", " ")
        lookup = {m.value.lower(): m for m in cls}
        if normalized in lookup:
            return lookup[normalized]
        lookup_nospace = {m.value.lower().replace(" ", ""): m for m in cls}
        normalized_nospace = normalized.replace(" ", "")
        if normalized_nospace in lookup_nospace:
            return lookup_nospace[normalized_nospace]
        logger.warning("cs1_unknown_sector", extra={"raw_sector": raw})
        return None


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Company:
    company_id: str
    ticker: str
    name: str
    position_factor: float = 0.0
    industry_id: Optional[str] = None
    sector: Optional[Sector] = None
    sub_sector: Optional[str] = None
    market_cap_percentile: Optional[float] = None
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    fiscal_year_end: Optional[str] = None


@dataclass
class Portfolio:
    portfolio_id: str
    name: str
    companies: List[Company] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CS1Client:
    """
    Async HTTP client for the CS1 Platform API.

    Usage:
        async with CS1Client() as client:
            company = await client.get_company("NVDA")
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "CS1Client":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_company(self, ticker: str) -> Company:
        client = self._get_client()
        try:
            response = await client.get(f"/api/v1/companies/{ticker.upper()}")
            response.raise_for_status()
            return self._map_company(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(f"Company '{ticker}' not found in CS1.")
            raise

    async def list_companies(
        self,
        sector: Optional[Sector] = None,
        min_revenue: Optional[float] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Company]:
        safe_limit = min(limit, 100)  
        companies = await self._fetch_all_companies(limit=safe_limit, offset=offset)
        if sector is not None:
            companies = [c for c in companies if c.sector == sector]
        if min_revenue is not None:
            companies = [
                c for c in companies
                if c.revenue_millions is not None
                and c.revenue_millions >= min_revenue
            ]
        return companies

    async def get_portfolio_companies(self, portfolio_id: str) -> List[Company]:
        """
        Fetch all companies for a portfolio.

        Tries GET /api/v1/portfolios/{portfolio_id}/companies first.
        Falls back to list_companies() if that endpoint doesn't exist yet.
        """
        client = self._get_client()

        try:
            response = await client.get(
                f"/api/v1/portfolios/{portfolio_id}/companies"
            )
            if response.status_code == 200:
                return [self._map_company(item) for item in response.json()]
        except Exception:
            pass  # endpoint doesn't exist yet — use fallback

        logger.warning(
            "get_portfolio_companies: portfolios endpoint not available, "
            "falling back to list_companies(). portfolio_id=%s",
            portfolio_id,
        )
        return await self.list_companies()
    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_all_companies(self, limit: int, offset: int) -> List[Company]:
        """GET /api/v1/companies with limit/offset."""
        client = self._get_client()
        response = await client.get(
            "/api/v1/companies",
            params={"limit": limit, "offset": offset},
        )
        response.raise_for_status()
        return [self._map_company(item) for item in response.json()]

    def _map_company(self, data: dict) -> Company:
        raw_sector  = data.get("sector") or data.get("industry") or ""
        raw_revenue = data.get("revenue_millions") or data.get("revenue_mm")

        return Company(
            company_id=str(data.get("id", "")),
            ticker=str(data.get("ticker") or "").upper(),
            name=data.get("name", ""),
            industry_id=str(data.get("industry_id", "")) or None,
            position_factor=float(data.get("position_factor", 0.0)),
            sector=Sector.from_raw(raw_sector) if raw_sector else None,
            sub_sector=data.get("sub_sector"),
            market_cap_percentile=(
                float(data["market_cap_percentile"])
                if data.get("market_cap_percentile") is not None
                else None
            ),
            revenue_millions=(
                float(raw_revenue) if raw_revenue is not None else None
            ),
            employee_count=(
                int(data["employee_count"])
                if data.get("employee_count") is not None
                else None
            ),
            fiscal_year_end=data.get("fiscal_year_end"),
        )

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                "CS1Client must be used as an async context manager: "
                "`async with CS1Client() as client:`"
            )
        return self._client