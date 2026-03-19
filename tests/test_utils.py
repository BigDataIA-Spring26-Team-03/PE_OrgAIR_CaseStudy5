import pytest
from decimal import Decimal
from src.scoring.utils import to_decimal, clamp, weighted_mean, coefficient_of_variation

# ===== to_decimal Tests =====
def test_to_decimal_precision():
    """Test that to_decimal quantizes correctly"""
    assert to_decimal(0.1, 1) + to_decimal(0.2, 1) == Decimal('0.3')
    assert to_decimal(105.789, 2) == Decimal('105.79')

def test_to_decimal_zero():
    """Test with zero"""
    assert to_decimal(0.0, 4) == Decimal('0.0000')

def test_to_decimal_negative():
    """Test with negative numbers"""
    assert to_decimal(-123.456, 2) == Decimal('-123.46')

def test_to_decimal_already_decimal():
    """Test with Decimal input"""
    val = Decimal('12.345')
    assert to_decimal(val, 2) == Decimal('12.35')

# ===== clamp Tests =====
def test_clamp_in_range():
    """Test values within range"""
    assert clamp(Decimal(50)) == Decimal(50)
    assert clamp(Decimal(0)) == Decimal(0)
    assert clamp(Decimal(100)) == Decimal(100)

def test_clamp_above_max():
    """Test values above max"""
    assert clamp(Decimal(105.5)) == Decimal(100)
    assert clamp(Decimal(200)) == Decimal(100)

def test_clamp_below_min():
    """Test values below min"""
    assert clamp(Decimal(-5)) == Decimal(0)
    assert clamp(Decimal(-100)) == Decimal(0)

def test_clamp_custom_range():
    """Test custom min/max"""
    assert clamp(Decimal(5), Decimal(10), Decimal(20)) == Decimal(10)
    assert clamp(Decimal(25), Decimal(10), Decimal(20)) == Decimal(20)

# ===== weighted_mean Tests =====
def test_weighted_mean_basic():
    """Test basic weighted mean"""
    values = [to_decimal(80), to_decimal(60)]
    weights = [Decimal('0.7'), Decimal('0.3')]
    # 80*0.7 + 60*0.3 = 56 + 18 = 74
    assert weighted_mean(values, weights) == Decimal('74.0000')

def test_weighted_mean_single_value():
    """Test with single value"""
    assert weighted_mean([to_decimal(50)], [Decimal('1')]) == Decimal('50.0000')

def test_weighted_mean_empty():
    """Test with empty list"""
    assert weighted_mean([], []) == Decimal(0)

def test_weighted_mean_length_mismatch():
    """Test error handling"""
    with pytest.raises(ValueError):
        weighted_mean([to_decimal(50)], [Decimal('0.5'), Decimal('0.5')])

# ===== coefficient_of_variation Tests =====
def test_cv_zero_mean():
    """Test CV with zero mean (edge case)"""
    assert coefficient_of_variation(Decimal('10'), Decimal('0')) == Decimal(0)

def test_cv_uniform_values():
    """Test CV for uniform data (should be 0)"""
    # If all values are the same, std_dev = 0, so CV = 0
    assert coefficient_of_variation(Decimal('0'), Decimal('50')) == Decimal(0)