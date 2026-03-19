# tests/test_position_factor.py

import pytest
from decimal import Decimal
from src.scoring.position_factor import (
    PositionFactorCalculator,
    calculate_position_factor_realtime
)


def test_realtime_nvda():
    """Test NVDA with real Yahoo Finance data."""
    print("\n" + "="*70)
    print("Testing NVDA with Real-Time Market Cap")
    print("="*70)
    
    result = calculate_position_factor_realtime(
        vr_score=85.0,
        ticker="NVDA",
        sector="technology"
    )
    
    print(f"\nNVDA Position Factor: {float(result.position_factor):.3f}")
    print(f"Market Cap: ${result.market_cap/1e9:.2f}B")
    print(f"Market Cap Percentile: {float(result.market_cap_percentile):.2f}")
    print(f"Sector Peers: {result.sector_peer_count}")
    
    # NVDA should be industry leader
    assert result.position_factor > Decimal("0.5")
    assert result.market_cap_percentile > 0.7


def test_all_5_companies_realtime():
    """Test all 5 CS3 companies with real data."""
    print("\n" + "="*70)
    print("All 5 Companies - Real-Time Position Factors")
    print("="*70)
    
    companies = [
        {"ticker": "NVDA", "vr": 85, "sector": "technology"},
        {"ticker": "JPM", "vr": 70, "sector": "financial_services"},
        {"ticker": "WMT", "vr": 60, "sector": "retail"},
        {"ticker": "GE", "vr": 50, "sector": "manufacturing"},
        {"ticker": "DG", "vr": 40, "sector": "retail"},
    ]
    
    print(f"\n{'Ticker':<8} {'VR':<6} {'Market Cap':<15} {'Percentile':<12} {'PF':<8} {'Status'}")
    print("-"*70)
    
    for company in companies:
        result = calculate_position_factor_realtime(
            vr_score=company['vr'],
            ticker=company['ticker'],
            sector=company['sector']
        )
        
        mcap_str = f"${result.market_cap/1e9:.1f}B" if result.market_cap else "N/A"
        pf = float(result.position_factor)
        
        if pf > 0.5:
            status = "🟢 Leader"
        elif pf > 0:
            status = "🟡 Above Avg"
        elif pf > -0.5:
            status = "🟠 Below Avg"
        else:
            status = "🔴 Laggard"
        
        print(
            f"{company['ticker']:<8} "
            f"{company['vr']:<6.0f} "
            f"{mcap_str:<15} "
            f"{float(result.market_cap_percentile):<12.2f} "
            f"{pf:<8.3f} "
            f"{status}"
        )


def test_cache_works():
    """Test caching mechanism."""
    calc = PositionFactorCalculator(cache_duration_hours=1)
    
    # First call - fetches from API
    import time
    start = time.time()
    mcap1 = calc.get_market_cap("NVDA", use_cache=False)
    time1 = time.time() - start
    
    # Second call - from cache
    start = time.time()
    mcap2 = calc.get_market_cap("NVDA", use_cache=True)
    time2 = time.time() - start
    
    print(f"\nAPI call: {time1:.3f}s")
    print(f"Cache hit: {time2:.3f}s")
    print(f"Speedup: {time1/time2:.1f}x")
    
    assert mcap1 == mcap2
    assert time2 < time1  # Cache should be faster


def test_fallback_on_api_failure():
    """Test graceful fallback if Yahoo Finance fails."""
    calc = PositionFactorCalculator()
    
    # Use fake ticker that won't be found
    percentile = calc.calculate_percentile("FAKE123", "technology", use_cache=False)
    
    # Should fallback to 0.5
    assert percentile == 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])