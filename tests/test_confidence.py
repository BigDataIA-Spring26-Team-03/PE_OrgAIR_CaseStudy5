# tests/test_confidence.py

import pytest
from decimal import Decimal
from src.scoring.confidence import (
    ConfidenceCalculator,
    calculate_confidence_interval
)


def test_more_evidence_narrower_ci():
    """Test that more evidence produces narrower confidence intervals."""
    calc = ConfidenceCalculator()
    
    # Few evidence
    ci_few = calc.calculate(
        score=75.0,
        score_type="org_air",
        evidence_count=5
    )
    
    # Lots of evidence
    ci_many = calc.calculate(
        score=75.0,
        score_type="org_air",
        evidence_count=100
    )
    
    print(f"\n5 evidence:   CI width = {ci_few.ci_width:.2f}")
    print(f"100 evidence: CI width = {ci_many.ci_width:.2f}")
    
    # More evidence → narrower CI
    assert ci_many.ci_width < ci_few.ci_width


def test_higher_reliability_lower_sem():
    """Test reliability affects SEM."""
    calc = ConfidenceCalculator()
    
    # Low evidence → low reliability → high SEM
    ci_low = calc.calculate(
        score=75.0,
        score_type="org_air",
        evidence_count=5
    )
    
    # High evidence → high reliability → low SEM
    ci_high = calc.calculate(
        score=75.0,
        score_type="org_air",
        evidence_count=50
    )
    
    print(f"\n5 evidence:  ρ={ci_low.reliability:.3f}, SEM={ci_low.sem:.2f}")
    print(f"50 evidence: ρ={ci_high.reliability:.3f}, SEM={ci_high.sem:.2f}")
    
    assert ci_high.reliability > ci_low.reliability
    assert ci_high.sem < ci_low.sem


def test_ci_always_bounded():
    """Test CI is always within [0, 100]."""
    calc = ConfidenceCalculator()
    
    # Test extreme scores
    ci_low = calc.calculate(5.0, "org_air", 10)
    ci_high = calc.calculate(95.0, "org_air", 10)
    
    assert Decimal("0") <= ci_low.ci_lower <= Decimal("100")
    assert Decimal("0") <= ci_high.ci_upper <= Decimal("100")


def test_different_score_types():
    """Test different score types use different sigmas."""
    calc = ConfidenceCalculator()
    
    # VR (σ=15) should have wider CI than HR (σ=12)
    ci_vr = calc.calculate(75.0, "vr", 20)
    ci_hr = calc.calculate(75.0, "hr", 20)
    
    print(f"\nVR CI width:  {ci_vr.ci_width:.2f}")
    print(f"HR CI width:  {ci_hr.ci_width:.2f}")
    
    assert ci_vr.ci_width > ci_hr.ci_width


def test_confidence_levels():
    """Test different confidence levels."""
    calc = ConfidenceCalculator()
    
    ci_90 = calc.calculate(75.0, "org_air", 20, confidence_level=0.90)
    ci_95 = calc.calculate(75.0, "org_air", 20, confidence_level=0.95)
    ci_99 = calc.calculate(75.0, "org_air", 20, confidence_level=0.99)
    
    print(f"\n90% CI: [{ci_90.ci_lower:.2f}, {ci_90.ci_upper:.2f}] width={ci_90.ci_width:.2f}")
    print(f"95% CI: [{ci_95.ci_lower:.2f}, {ci_95.ci_upper:.2f}] width={ci_95.ci_width:.2f}")
    print(f"99% CI: [{ci_99.ci_lower:.2f}, {ci_99.ci_upper:.2f}] width={ci_99.ci_width:.2f}")
    
    # Higher confidence → wider interval
    assert ci_90.ci_width < ci_95.ci_width < ci_99.ci_width


def test_5_company_portfolio():
    """Test confidence intervals for all 5 companies."""
    calc = ConfidenceCalculator()
    
    # Simulated evidence counts (would come from integration service)
    companies = [
        {"ticker": "NVDA", "score": 85.0, "evidence": 79},
        {"ticker": "JPM", "score": 70.0, "evidence": 65},
        {"ticker": "WMT", "score": 60.0, "evidence": 52},
        {"ticker": "GE", "score": 50.0, "evidence": 48},
        {"ticker": "DG", "score": 40.0, "evidence": 35},
    ]
    
    print("\n" + "="*90)
    print("Confidence Intervals for 5-Company Portfolio")
    print("="*90)
    print(f"\n{'Ticker':<8} {'Score':<8} {'Evidence':<10} {'Reliability':<12} {'SEM':<8} {'95% CI':<20}")
    print("-"*90)
    
    for company in companies:
        ci = calc.calculate(
            score=company['score'],
            score_type="org_air",
            evidence_count=company['evidence']
        )
        
        ci_str = f"[{ci.ci_lower:.1f}, {ci.ci_upper:.1f}]"
        
        print(
            f"{company['ticker']:<8} "
            f"{float(ci.point_estimate):<8.1f} "
            f"{ci.evidence_count:<10} "
            f"{float(ci.reliability):<12.3f} "
            f"{float(ci.sem):<8.2f} "
            f"{ci_str:<20}"
        )
        
        # More evidence → narrower CI
        assert ci.ci_width < Decimal("20")  # Should be reasonably narrow


def test_spearman_brown_formula():
    """Test Spearman-Brown calculation is correct."""
    calc = ConfidenceCalculator()
    
    # Manual calculation
    n = 20
    r = 0.30
    
    expected_rho = (n * r) / (1 + (n-1) * r)
    # expected_rho = 6.0 / 6.7 = 0.896
    
    ci = calc.calculate(75.0, "org_air", n)
    
    print(f"\nExpected ρ: {expected_rho:.3f}")
    print(f"Calculated ρ: {ci.reliability:.3f}")
    
    assert abs(float(ci.reliability) - expected_rho) < 0.01


def test_convenience_function():
    """Test convenience function."""
    ci = calculate_confidence_interval(
        score=75.0,
        score_type="org_air",
        evidence_count=50
    )
    
    assert Decimal("0") <= ci.ci_lower <= Decimal("100")
    assert Decimal("0") <= ci.ci_upper <= Decimal("100")
    assert ci.ci_lower < ci.point_estimate < ci.ci_upper


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])