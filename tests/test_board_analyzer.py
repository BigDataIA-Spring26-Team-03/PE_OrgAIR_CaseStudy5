"""
Tests for BoardCompositionAnalyzer — AI governance scoring from board signals.
"""

import sys
from pathlib import Path
from decimal import Decimal

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from scoring.board_analyzer import BoardCompositionAnalyzer, BoardMember


analyzer = BoardCompositionAnalyzer()


def _make_member(name="Jane Doe", title="Director", bio="", is_independent=False, committees=None):
    return BoardMember(
        name=name, title=title, bio=bio,
        is_independent=is_independent, committees=committees or [],
    )


# ── Tests ────────────────────────────────────────────────────────────

def test_base_score_only():
    """No indicators → base score 20."""
    sig = analyzer.analyze_board("C1", "TST", members=[], committees=[])
    assert sig.governance_score == Decimal("20"), f"Expected 20, got {sig.governance_score}"
    print("PASS test_base_score_only")


def test_tech_committee_adds_15():
    sig = analyzer.analyze_board("C1", "TST", members=[], committees=["Technology Committee"])
    assert sig.governance_score == Decimal("35"), f"Expected 35, got {sig.governance_score}"
    assert sig.has_tech_committee
    print("PASS test_tech_committee_adds_15")


def test_ai_expertise_adds_20():
    m = _make_member(name="Dr. Smith", bio="Expert in artificial intelligence and deep learning")
    sig = analyzer.analyze_board("C1", "TST", members=[m], committees=[])
    assert sig.governance_score == Decimal("40"), f"Expected 40, got {sig.governance_score}"
    assert sig.has_ai_expertise
    assert "Dr. Smith" in sig.ai_experts
    print("PASS test_ai_expertise_adds_20")


def test_data_officer_adds_15():
    m = _make_member(name="Alice", title="Chief Data Officer")
    sig = analyzer.analyze_board("C1", "TST", members=[m], committees=[])
    assert sig.governance_score == Decimal("35"), f"Expected 35, got {sig.governance_score}"
    assert sig.has_data_officer
    print("PASS test_data_officer_adds_15")


def test_independence_adds_10():
    members = [
        _make_member(name="A", is_independent=True),
        _make_member(name="B", is_independent=True),
        _make_member(name="C", is_independent=False),
    ]
    sig = analyzer.analyze_board("C1", "TST", members=members, committees=[])
    assert sig.governance_score == Decimal("30"), f"Expected 30, got {sig.governance_score}"
    assert sig.has_independent_majority
    print("PASS test_independence_adds_10")


def test_risk_oversight_adds_10():
    m = _make_member(bio="Expertise in technology risk management")
    sig = analyzer.analyze_board("C1", "TST", members=[m], committees=["Risk Committee"])
    assert sig.has_risk_tech_oversight
    # base 20 + risk 10 = 30
    assert sig.governance_score == Decimal("30"), f"Expected 30, got {sig.governance_score}"
    print("PASS test_risk_oversight_adds_10")


def test_ai_strategy_adds_10():
    sig = analyzer.analyze_board(
        "C1", "TST", members=[], committees=[],
        strategy_text="We are investing in artificial intelligence to transform operations.",
    )
    assert sig.governance_score == Decimal("30"), f"Expected 30, got {sig.governance_score}"
    assert sig.has_ai_strategy
    print("PASS test_ai_strategy_adds_10")


def test_full_score():
    """All indicators present → capped at 100."""
    members = [
        _make_member(name="AI Expert", title="Chief AI Officer",
                     bio="PhD in artificial intelligence", is_independent=True),
        _make_member(name="Ind 1", is_independent=True),
        _make_member(name="Ind 2", is_independent=True),
        _make_member(name="Non-Ind", bio="technology leader"),
    ]
    sig = analyzer.analyze_board(
        "C1", "TST", members=members,
        committees=["Technology Committee", "Risk Committee"],
        strategy_text="Our artificial intelligence strategy drives growth.",
    )
    # 20 + 15 + 20 + 15 + 10 + 10 + 10 = 100
    assert sig.governance_score == Decimal("100"), f"Expected 100, got {sig.governance_score}"
    print("PASS test_full_score")


def test_cap_at_100():
    """Even with redundant signals, score cannot exceed 100."""
    members = [
        _make_member(name="AI PhD", title="Chief AI Officer",
                     bio="artificial intelligence machine learning deep learning",
                     is_independent=True),
        _make_member(name="CTO", title="Chief Technology Officer",
                     bio="data science neural network", is_independent=True),
        _make_member(name="CDO", title="Chief Data Officer",
                     bio="computer vision ai research", is_independent=True),
    ]
    sig = analyzer.analyze_board(
        "C1", "TST", members=members,
        committees=["Technology Committee", "Innovation Committee", "Risk Committee"],
        strategy_text="artificial intelligence and machine learning are key.",
    )
    assert sig.governance_score <= Decimal("100"), f"Score {sig.governance_score} exceeds 100"
    print("PASS test_cap_at_100")


def test_confidence_scales_with_members():
    # 0 members → 0.5
    sig0 = analyzer.analyze_board("C1", "TST", members=[], committees=[])
    assert sig0.confidence == Decimal("0.5"), f"Expected 0.5, got {sig0.confidence}"

    # 10 members → min(0.5 + 0.5, 0.95) = 0.95
    ten = [_make_member(name=f"M{i}") for i in range(10)]
    sig10 = analyzer.analyze_board("C1", "TST", members=ten, committees=[])
    assert sig10.confidence == Decimal("0.95"), f"Expected 0.95, got {sig10.confidence}"

    # 5 members → 0.5 + 0.25 = 0.75
    five = [_make_member(name=f"M{i}") for i in range(5)]
    sig5 = analyzer.analyze_board("C1", "TST", members=five, committees=[])
    assert sig5.confidence == Decimal("0.75"), f"Expected 0.75, got {sig5.confidence}"
    print("PASS test_confidence_scales_with_members")


def test_case_insensitive():
    """Keywords should match regardless of case."""
    m = _make_member(name="Dr. X", title="CHIEF DATA OFFICER",
                     bio="ARTIFICIAL INTELLIGENCE researcher")
    sig = analyzer.analyze_board(
        "C1", "TST", members=[m], committees=["TECHNOLOGY COMMITTEE"],
        strategy_text="MACHINE LEARNING strategy",
    )
    assert sig.has_tech_committee
    assert sig.has_ai_expertise
    assert sig.has_data_officer
    assert sig.has_ai_strategy
    print("PASS test_case_insensitive")


def main():
    tests = [
        test_base_score_only,
        test_tech_committee_adds_15,
        test_ai_expertise_adds_20,
        test_data_officer_adds_15,
        test_independence_adds_10,
        test_risk_oversight_adds_10,
        test_ai_strategy_adds_10,
        test_full_score,
        test_cap_at_100,
        test_confidence_scales_with_members,
        test_case_insensitive,
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
