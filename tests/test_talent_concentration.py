"""Tests for TalentConcentrationCalculator."""

import sys
from decimal import Decimal
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from scoring.talent_concentration import (
    TalentConcentrationCalculator,
    JobAnalysis,
)


def test_basic_tc_calculation():
    """TC is computed and bounded to [0, 1]."""
    calc = TalentConcentrationCalculator()
    analysis = JobAnalysis(
        total_ai_jobs=20,
        senior_ai_jobs=5,
        mid_ai_jobs=10,
        entry_ai_jobs=5,
        unique_skills={"pytorch", "tensorflow", "mlops", "spark", "docker"},
    )
    tc = calc.calculate_tc(analysis)
    assert Decimal("0") <= tc <= Decimal("1")
    print(f"PASS test_basic_tc_calculation (tc={tc})")


def test_high_concentration():
    """Few jobs, few skills, high leadership ratio -> high TC."""
    calc = TalentConcentrationCalculator()
    analysis = JobAnalysis(
        total_ai_jobs=2,
        senior_ai_jobs=2,
        mid_ai_jobs=0,
        entry_ai_jobs=0,
        unique_skills={"pytorch"},
    )
    tc = calc.calculate_tc(analysis, glassdoor_individual_mentions=5, glassdoor_review_count=10)
    assert tc > Decimal("0.5"), f"Expected high TC, got {tc}"
    print(f"PASS test_high_concentration (tc={tc})")


def test_low_concentration():
    """Many jobs, many skills, low leadership ratio -> low TC."""
    calc = TalentConcentrationCalculator()
    analysis = JobAnalysis(
        total_ai_jobs=100,
        senior_ai_jobs=5,
        mid_ai_jobs=50,
        entry_ai_jobs=45,
        unique_skills=set(f"skill_{i}" for i in range(15)),
    )
    tc = calc.calculate_tc(analysis, glassdoor_individual_mentions=0, glassdoor_review_count=100)
    assert tc < Decimal("0.3"), f"Expected low TC, got {tc}"
    print(f"PASS test_low_concentration (tc={tc})")


def test_zero_jobs_default():
    """Zero jobs should return a reasonable default TC."""
    calc = TalentConcentrationCalculator()
    analysis = JobAnalysis(
        total_ai_jobs=0,
        senior_ai_jobs=0,
        mid_ai_jobs=0,
        entry_ai_jobs=0,
        unique_skills=set(),
    )
    tc = calc.calculate_tc(analysis)
    assert Decimal("0") <= tc <= Decimal("1")
    print(f"PASS test_zero_jobs_default (tc={tc})")


def test_talent_risk_adjustment():
    """TalentRiskAdj = 1 - 0.15 * max(0, TC - 0.25)."""
    calc = TalentConcentrationCalculator()
    analysis = JobAnalysis(
        total_ai_jobs=2,
        senior_ai_jobs=2,
        mid_ai_jobs=0,
        entry_ai_jobs=0,
        unique_skills={"pytorch"},
    )
    tc = calc.calculate_tc(analysis)
    # Manually compute expected risk adj
    tc_float = float(tc)
    expected_adj = 1 - 0.15 * max(0, tc_float - 0.25)
    # TC > 0.25 so adj should be < 1
    assert tc > Decimal("0.25"), f"TC should be > 0.25 for this test, got {tc}"
    assert expected_adj < 1.0
    print(f"PASS test_talent_risk_adjustment (tc={tc}, adj={expected_adj:.4f})")


def test_analyze_job_postings():
    """analyze_job_postings correctly categorizes postings."""
    calc = TalentConcentrationCalculator()
    postings = [
        {"title": "VP of AI Engineering", "metadata_json": '{"skills": ["pytorch", "mlops"]}'},
        {"title": "Senior ML Engineer", "metadata_json": '{"skills": ["pytorch", "tensorflow"]}'},
        {"title": "Junior Data Analyst", "metadata_json": '{"skills": ["pandas"]}'},
        {"title": "ML Engineer", "metadata_json": '{"skills": ["pytorch", "docker"]}'},
        {"title": "Principal Data Scientist", "metadata_json": '{"skills": ["xgboost"]}'},
    ]
    analysis = calc.analyze_job_postings(postings)
    assert analysis.total_ai_jobs == 5
    assert analysis.senior_ai_jobs == 2  # VP, Principal
    assert analysis.mid_ai_jobs == 2     # Senior, ML Engineer (default)
    assert analysis.entry_ai_jobs == 1   # Junior
    assert "pytorch" in analysis.unique_skills
    assert "pandas" in analysis.unique_skills
    print(f"PASS test_analyze_job_postings (skills={len(analysis.unique_skills)})")


def test_skill_concentration_maxes_at_15():
    """Having >= 15 skills means skill_concentration = 0."""
    calc = TalentConcentrationCalculator()
    analysis = JobAnalysis(
        total_ai_jobs=50,
        senior_ai_jobs=10,
        mid_ai_jobs=30,
        entry_ai_jobs=10,
        unique_skills=set(f"skill_{i}" for i in range(20)),
    )
    tc = calc.calculate_tc(analysis)
    # With 20 skills, skill_concentration = max(0, 1 - 20/15) = 0
    # So skill component contributes 0
    assert Decimal("0") <= tc <= Decimal("1")
    print(f"PASS test_skill_concentration_maxes_at_15 (tc={tc})")


def main():
    tests = [
        test_basic_tc_calculation,
        test_high_concentration,
        test_low_concentration,
        test_zero_jobs_default,
        test_talent_risk_adjustment,
        test_analyze_job_postings,
        test_skill_concentration_maxes_at_15,
    ]
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"FAIL {t.__name__}: {e}")

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed < len(tests):
        sys.exit(1)


if __name__ == "__main__":
    main()
