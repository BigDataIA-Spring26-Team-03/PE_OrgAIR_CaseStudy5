# src/scoring/position_factor.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Tuple, Optional, Any
import structlog
import yfinance as yf
from datetime import datetime, timedelta

from .utils import to_decimal, clamp

logger = structlog.get_logger()


@dataclass
class PositionFactorResult:
    """Position factor calculation result with audit trail."""
    # Required fields first
    position_factor: Decimal
    vr_score: Decimal
    sector: str
    sector_avg_vr: Decimal
    vr_component: Decimal
    market_cap_percentile: Decimal
    mcap_component: Decimal
    raw_inputs_vr_score: float
    raw_inputs_market_cap_percentile: float
    
    # Optional fields last (with defaults)
    market_cap: Optional[float] = None
    sector_peer_count: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "position_factor": float(self.position_factor),
            "vr_score": float(self.vr_score),
            "sector": self.sector,
            "sector_avg_vr": float(self.sector_avg_vr),
            "vr_component": float(self.vr_component),
            "market_cap_percentile": float(self.market_cap_percentile),
            "mcap_component": float(self.mcap_component),
            "market_cap": self.market_cap,
            "sector_peer_count": self.sector_peer_count,
            "raw_inputs": {
                "vr_score": self.raw_inputs_vr_score,
                "market_cap_percentile": self.raw_inputs_market_cap_percentile
            }
        }


class PositionFactorCalculator:
    """
    Calculate position factor using real-time market cap data.
    
    Production version with Yahoo Finance integration.
    """
    
    # Sector average V^R scores (framework calibration)
    SECTOR_AVG_VR: Dict[str, float] = {
        "technology": 65.0,
        "financial_services": 55.0,
        "healthcare": 52.0,
        "business_services": 50.0,
        "retail": 48.0,
        "manufacturing": 45.0,
        "industrials": 45.0,
        "consumer": 48.0,
        "financial": 55.0,
        "services": 50.0
    }
    
    # Major peers by sector (for percentile calculation)
    SECTOR_PEERS: Dict[str, List[str]] = {
        "technology": [
            "NVDA", "AMD", "INTC", "QCOM", "AVGO", "TXN", "ADI", "MRVL",
            "AMAT", "LRCX", "KLAC", "MCHP", "MU", "NXPI", "SWKS", "ON"
        ],
        "financial_services": [
            "JPM", "BAC", "WFC", "C", "GS", "MS", "BK", "USB",
            "PNC", "TFC", "SCHW", "AXP", "BLK", "SPGI", "CME", "ICE"
        ],
        "retail": [
            "WMT", "COST", "HD", "LOW", "TGT", "DG", "DLTR", "ROST",
            "TJX", "BBY", "ULTA", "DKS", "FL", "AZO", "ORLY", "AAP"
        ],
        "manufacturing": [
            "GE", "CAT", "DE", "HON", "MMM", "EMR", "ITW", "PH",
            "ETN", "CMI", "ROK", "DOV", "IR", "AME", "HUBB", "IEX"
        ],
        "industrials": [
            "GE", "CAT", "DE", "HON", "UNP", "UPS", "RTX", "BA",
            "LMT", "GD", "NOC", "FDX", "CSX", "NSC", "WM", "RSG"
        ]
    }
    
    def __init__(self, cache_duration_hours: int = 24):
        """
        Initialize with caching.
        
        Args:
            cache_duration_hours: How long to cache market cap data
        """
        self.cache: Dict[str, Tuple[float, datetime]] = {}
        self.cache_duration = timedelta(hours=cache_duration_hours)
        
        logger.info(
            "PositionFactorCalculator initialized",
            cache_duration_hours=cache_duration_hours,
            sector_averages=self.SECTOR_AVG_VR
        )
    
    def get_market_cap(self, ticker: str, use_cache: bool = True) -> Optional[float]:
        """
        Fetch market cap from Yahoo Finance with caching.
        
        Args:
            ticker: Stock ticker
            use_cache: Use cached data if available
            
        Returns:
            Market cap in USD, or None if unavailable
        """
        
        # Check cache
        if use_cache and ticker in self.cache:
            cached_mcap, cached_time = self.cache[ticker]
            age = datetime.now() - cached_time
            
            if age < self.cache_duration:
                logger.debug(f"Using cached market cap for {ticker}")
                return cached_mcap
        
        # Fetch from Yahoo Finance
        try:
            logger.debug(f"Fetching market cap for {ticker} from Yahoo Finance")
            stock = yf.Ticker(ticker)
            info = stock.info
            
            market_cap = info.get('marketCap') or info.get('market_cap')
            
            if market_cap and market_cap > 0:
                # Cache it
                self.cache[ticker] = (market_cap, datetime.now())
                logger.debug(f"{ticker} market cap: ${market_cap/1e9:.2f}B")
                return market_cap
            else:
                logger.warning(f"No market cap data for {ticker}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching {ticker} market cap: {e}")
            return None
    
    def calculate_percentile(
        self,
        ticker: str,
        sector: str,
        use_cache: bool = True
    ) -> float:
        """
        Calculate market cap percentile within sector.
        
        Args:
            ticker: Company ticker
            sector: Company sector
            use_cache: Use cached market cap data
            
        Returns:
            Percentile (0-1)
        """
        
        logger.info(f"Calculating market cap percentile for {ticker} in {sector}")
        
        # Get peer tickers
        sector_key = sector.lower().replace(" ", "_")
        peer_tickers = self.SECTOR_PEERS.get(sector_key, [])
        
        if not peer_tickers:
            logger.warning(f"No peers defined for sector {sector}, using default 0.5")
            return 0.5
        
        # Ensure target ticker is included
        if ticker not in peer_tickers:
            peer_tickers = peer_tickers + [ticker]
        
        # Fetch market caps for all peers
        market_caps: List[Tuple[str, float]] = []
        
        for t in peer_tickers:
            mcap = self.get_market_cap(t, use_cache=use_cache)
            if mcap:
                market_caps.append((t, mcap))
        
        if not market_caps:
            logger.error("Could not fetch any market cap data")
            return 0.5
        
        # Sort by market cap
        market_caps.sort(key=lambda x: x[1])
        
        # Find ticker's rank
        tickers_sorted = [t for t, _ in market_caps]
        
        if ticker not in tickers_sorted:
            logger.warning(f"{ticker} market cap not available, using default")
            return 0.5
        
        rank = tickers_sorted.index(ticker) + 1
        total = len(tickers_sorted)
        percentile = rank / total
        
        logger.info(
            "Market cap percentile calculated",
            ticker=ticker,
            rank=rank,
            total=total,
            percentile=round(percentile, 3),
            market_cap=f"${market_caps[rank-1][1]/1e9:.2f}B"
        )
        
        return percentile
    
    def calculate(
        self,
        vr_score: float,
        sector: str,
        market_cap_percentile: float
    ) -> PositionFactorResult:
        """Calculate with provided percentile (existing method)."""
        
        # Input validation
        if not 0 <= vr_score <= 100:
            raise ValueError(f"vr_score must be 0-100, got {vr_score}")
        
        if not 0 <= market_cap_percentile <= 1:
            raise ValueError(f"market_cap_percentile must be 0-1, got {market_cap_percentile}")
        
        raw_vr = vr_score
        raw_mcap = market_cap_percentile
        
        # Convert to Decimal
        vr = to_decimal(vr_score)
        mcap_pct = to_decimal(market_cap_percentile)
        
        # Get sector average
        sector_lower = sector.lower().replace(" ", "_")
        sector_avg = self.SECTOR_AVG_VR.get(sector_lower, 50.0)
        sector_avg_decimal = to_decimal(sector_avg)
        
        logger.debug(
            "Calculating position factor",
            vr_score=float(vr),
            sector=sector,
            sector_avg_vr=float(sector_avg_decimal),
            market_cap_percentile=float(mcap_pct)
        )
        
        # VR component
        vr_diff = vr - sector_avg_decimal
        vr_component = vr_diff / Decimal("50")
        vr_component = clamp(vr_component, Decimal("-1"), Decimal("1"))
        
        # Market cap component
        mcap_component = (mcap_pct - Decimal("0.5")) * Decimal("2")
        mcap_component = clamp(mcap_component, Decimal("-1"), Decimal("1"))
        
        # Weighted combination (60% VR, 40% market cap)
        pf = Decimal("0.6") * vr_component + Decimal("0.4") * mcap_component
        pf = clamp(pf, Decimal("-1"), Decimal("1"))
        
        result = PositionFactorResult(
            position_factor=pf,
            vr_score=vr,
            sector=sector,
            sector_avg_vr=sector_avg_decimal,
            vr_component=vr_component,
            market_cap_percentile=mcap_pct,
            mcap_component=mcap_component,
            raw_inputs_vr_score=raw_vr,
            raw_inputs_market_cap_percentile=raw_mcap
        )
        
        logger.info(
            "Position factor calculated",
            position_factor=float(result.position_factor),
            vr_component=float(result.vr_component),
            mcap_component=float(result.mcap_component)
        )
        
        return result
    
    def calculate_with_realtime(
        self,
        vr_score: float,
        ticker: str,
        sector: str,
        use_cache: bool = True
    ) -> PositionFactorResult:
        """
        Calculate PF using real-time Yahoo Finance data.
        
        This is the PRODUCTION method - fetches live market caps.
        """
        
        # Get real-time percentile
        mcap_percentile = self.calculate_percentile(ticker, sector, use_cache=use_cache)
        
        # Get actual market cap for audit trail
        market_cap = self.get_market_cap(ticker, use_cache=use_cache)
        
        # Calculate PF
        result = self.calculate(vr_score, sector, mcap_percentile)
        
        # Add market cap to result
        result.market_cap = market_cap
        result.sector_peer_count = len(self.SECTOR_PEERS.get(sector.lower().replace(" ", "_"), []))
        
        return result


# Convenience functions
def calculate_position_factor(
    vr_score: float,
    sector: str,
    market_cap_percentile: float
) -> PositionFactorResult:
    """Calculate with provided percentile."""
    calc = PositionFactorCalculator()
    return calc.calculate(vr_score, sector, market_cap_percentile)


def calculate_position_factor_realtime(
    vr_score: float,
    ticker: str,
    sector: str
) -> PositionFactorResult:
    """Calculate with real-time Yahoo Finance data."""
    calc = PositionFactorCalculator()
    return calc.calculate_with_realtime(vr_score, ticker, sector)