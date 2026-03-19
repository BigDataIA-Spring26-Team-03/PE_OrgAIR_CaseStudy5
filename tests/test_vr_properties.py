from decimal import Decimal

from hypothesis import given, settings, strategies as st

from src.scoring.vr_calculator import VRCalculator


# 7 dimension scores between -500 to 500(VRCalculator clamps each to [0,100])
dim_scores = st.lists(
    st.floats(min_value=-500, max_value=500, allow_nan=False, allow_infinity=False),
    min_size=7,
    max_size=7,
)

# Test tc should be between 0 to 1
tc_vals = st.floats(min_value=-5, max_value=5, allow_nan=False, allow_infinity=False)


@settings(max_examples=500)
@given(scores=dim_scores, tc=tc_vals)
def test_vr_always_bounded(scores, tc):
    """0 ≤ VR ≤ 100 always."""
    r = VRCalculator().calculate(scores, tc)
    assert Decimal("0") <= r.vr_score <= Decimal("100")


@settings(max_examples=500)
@given(
    base=st.floats(min_value=0, max_value=100, allow_nan=False, allow_infinity=False),
    delta=st.floats(min_value=0, max_value=50, allow_nan=False, allow_infinity=False),
    tc=st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False),
)
def test_higher_scores_increase_vr_when_uniform(base, delta, tc):
    """
    Monotonicity (safe version):
    Increasing ALL dimensions uniformly should not reduce VR.
    i   f the company gets better across the board, the readiness score shouldn’t decrease
    """
    calc = VRCalculator()
    s1 = [base] * 7
    s2 = [min(100.0, base + delta)] * 7
    r1 = calc.calculate(s1, tc).vr_score
    r2 = calc.calculate(s2, tc).vr_score
    assert r2 >= r1


@settings(max_examples=500)
@given(scores=st.lists(st.floats(min_value=0, max_value=100, allow_nan=False, allow_infinity=False), min_size=7, max_size=7))
def test_higher_tc_lower_vr(scores):
    """Higher TC should not increase VR (holding dimensions fixed)."""
    calc = VRCalculator()
    low = calc.calculate(scores, 0.0).vr_score
    high = calc.calculate(scores, 1.0).vr_score
    assert high <= low


@settings(max_examples=500)
@given(
    x=st.floats(min_value=0, max_value=100, allow_nan=False, allow_infinity=False),
    tc=st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False),
)
def test_uniform_dimensions_no_cv_penalty(x, tc):
    """
    If all dims equal => std_dev=0 => cv=0 => penalty_factor = 1.0
    """
    r = VRCalculator().calculate([x] * 7, tc)
    assert r.penalty_factor == Decimal("1.0000")


@settings(max_examples=500)
@given(scores=dim_scores, tc=tc_vals)
def test_deterministic(scores, tc):
    """Same inputs => identical output.scoring engine is repeatable and audit-safe"""
    calc = VRCalculator()
    r1 = calc.calculate(scores, tc)
    r2 = calc.calculate(scores, tc)
    assert r1.vr_score == r2.vr_score
    assert r1.penalty_factor == r2.penalty_factor
    assert r1.talent_risk_adjustment == r2.talent_risk_adjustment
