# tests/test_hr_calculator.py

import pytest
from decimal import Decimal
from src.scoring.hr_calculator import (
    HRCalculator,
    calculate_hr
)


def test_hr_calculator_initialization():
    """Test calculator initializes with correct delta."""
    calc = HRCalculator()
    assert calc.delta == Decimal("0.15")


def test_industry_leader_gets_boost():
    """Test that industry leaders get HR boost."""
    calc = HRCalculator(use_database=False)
    
    # NVDA: Leader in tech
    result = calc.calculate(
        sector="technology",
        position_factor=0.64  # From our real test!
    )
    
    # HR should be higher than base (75)
    assert result.hr_score > result.hr_base
    
    print(f"\nNVDA (Leader):")
    print(f"  HR_base: {result.hr_base}")
    print(f"  Position Factor: {result.position_factor}")
    print(f"  HR Score: {result.hr_score:.2f}")
    print(f"  Boost: +{float((result.hr_score - result.hr_base) / result.hr_base * 100):.1f}%")


def test_industry_laggard_gets_penalty():
    """Test that laggards get HR penalty."""
    calc = HRCalculator(use_database=False)
    
    # Dollar General: Lagging in retail
    result = calc.calculate(
        sector="retail",
        position_factor=-0.30
    )
    
    # HR should be lower than base (70)
    assert result.hr_score < result.hr_base
    
    print(f"\nDollar General (Laggard):")
    print(f"  HR_base: {result.hr_base}")
    print(f"  Position Factor: {result.position_factor}")
    print(f"  HR Score: {result.hr_score:.2f}")
    print(f"  Penalty: {float((result.hr_score - result.hr_base) / result.hr_base * 100):.1f}%")


def test_average_company_no_change():
    """Test average company (PF=0) gets base HR."""
    calc = HRCalculator(use_database=False)
    
    result = calc.calculate(
        sector="manufacturing",
        position_factor=0.0
    )
    
    # HR should equal base (no adjustment)
    assert result.hr_score == result.hr_base
    
    print(f"\nAverage Company:")
    print(f"  HR_base: {result.hr_base}")
    print(f"  Position Factor: {result.position_factor}")
    print(f"  HR Score: {result.hr_score:.2f}")


def test_hr_always_bounded():
    """Test HR is always 0-100."""
    calc = HRCalculator(use_database=False)
    
    # Extreme positive
    result_high = calc.calculate("financial_services", 1.0)
    assert Decimal("0") <= result_high.hr_score <= Decimal("100")
    
    # Extreme negative
    result_low = calc.calculate("retail", -1.0)
    assert Decimal("0") <= result_low.hr_score <= Decimal("100")


def test_delta_correction():
    """Test that delta is 0.15 (not old 0.5 value)."""
    calc = HRCalculator()
    
    # With PF=1.0, adjustment should be 1.15 (not 1.5)
    result = calc.calculate("technology", 1.0)
    
    expected = Decimal("75") * Decimal("1.15")
    assert abs(result.hr_score - expected) < Decimal("0.01")


def test_5_company_portfolio():
    """Test all 5 CS3 companies."""
    calc = HRCalculator(use_database=False)
    
    companies = [
        {"ticker": "NVDA", "sector": "technology", "pf": 0.64, "expected_hr": 82},
        {"ticker": "JPM", "sector": "financial_services", "pf": 0.50, "expected_hr": 86},
        {"ticker": "WMT", "sector": "retail", "pf": 0.30, "expected_hr": 73},
        {"ticker": "GE", "sector": "manufacturing", "pf": 0.0, "expected_hr": 72},
        {"ticker": "DG", "sector": "retail", "pf": -0.30, "expected_hr": 67},
    ]
    
    print("\n" + "="*80)
    print("H^R Scores for 5-Company Portfolio")
    print("="*80)
    print(f"\n{'Ticker':<8} {'Sector':<20} {'HR_base':<10} {'PF':<8} {'HR Score':<10} {'Status'}")
    print("-"*80)
    
    for company in companies:
        result = calc.calculate(
            sector=company['sector'],
            position_factor=company['pf']
        )
        
        hr = float(result.hr_score)
        
        if hr >= 80:
            status = "🟢 Excellent"
        elif hr >= 70:
            status = "🟡 Good"
        else:
            status = "🟠 Fair"
        
        print(
            f"{company['ticker']:<8} "
            f"{company['sector']:<20} "
            f"{float(result.hr_base):<10.1f} "
            f"{float(result.position_factor):<8.2f} "
            f"{hr:<10.2f} "
            f"{status}"
        )
        
        # Verify within reasonable range
        assert abs(hr - company['expected_hr']) < 5


def test_convenience_function():
    """Test convenience function."""
    result = calculate_hr("technology", 0.5)
    
    assert Decimal("0") <= result.hr_score <= Decimal("100")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])