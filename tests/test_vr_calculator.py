import pytest
from decimal import Decimal
from src.scoring.vr_calculator import VRCalculator

def test_vr_basic_calculation():
    """Test basic V^R calculation"""
    calc = VRCalculator()
    
    result = calc.calculate(
        dimension_scores=[75, 75, 75, 75, 75, 75, 75],
        talent_concentration=0.3
    )
    
    # Should have some reasonable score
    assert Decimal(0) <= result.vr_score <= Decimal(100)
    assert result.weighted_mean == Decimal(75)  # All equal = mean is 75

def test_vr_with_zero_talent_concentration():
    """Test with no talent concentration (no penalty)"""
    calc = VRCalculator()
    
    result = calc.calculate(
        dimension_scores=[80] * 7,
        talent_concentration=0.0
    )
    
    # Talent risk adj should be 1.0 (no penalty)
    assert result.talent_risk_adjustment == Decimal(1)

def test_vr_with_high_talent_concentration():
    """Test with high TC (should apply penalty)"""
    calc = VRCalculator()
    
    result_low_tc = calc.calculate([75] * 7, talent_concentration=0.2)
    result_high_tc = calc.calculate([75] * 7, talent_concentration=0.8)
    
    # High TC should lower the score
    assert result_high_tc.vr_score < result_low_tc.vr_score
    assert result_high_tc.talent_risk_adjustment < Decimal(1)

def test_vr_result_to_dict():
    """Test serialization to dict"""
    calc = VRCalculator()
    result = calc.calculate([70] * 7, 0.3)
    
    result_dict = result.to_dict()
    
    assert "vr_score" in result_dict
    assert "dimension_scores" in result_dict
    assert isinstance(result_dict["vr_score"], float)