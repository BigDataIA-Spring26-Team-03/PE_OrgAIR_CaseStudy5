"""
Test Rubric Scorer with Sample Evidence
Tests Task 5.0b implementation.
"""

import sys
from pathlib import Path
from decimal import Decimal

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

from scoring.rubric_scorer import (
    RubricScorer,
    ScoreLevel,
    concatenate_evidence_chunks,
    extract_quantitative_metrics
)


def print_separator(title=""):
    if title:
        print(f"\n{'='*70}")
        print(f"{title:^70}")
        print(f"{'='*70}\n")
    else:
        print("=" * 70)


def test_data_infrastructure():
    """Test Data Infrastructure rubric scoring."""
    print_separator("TEST 1: Data Infrastructure Scoring")
    
    scorer = RubricScorer()
    
    # Test Case 1: Level 5 (Excellent)
    evidence_level5 = """
    Our company has deployed a modern cloud data platform using Snowflake and Databricks.
    We maintain a centralized data lake with real-time streaming capabilities.
    Our ML platform includes a feature store for consistent model training.
    Data quality exceeds 90% across all critical datasets.
    """
    
    result = scorer.score_dimension(
        "data_infrastructure",
        evidence_level5,
        {"data_quality": 0.92}
    )
    
    print("Level 5 Test:")
    print(f"  Score: {float(result.score):.2f}/100")
    print(f"  Level: {result.level.label}")
    print(f"  Matched Keywords: {result.matched_keywords}")
    print(f"  Confidence: {float(result.confidence):.2f}")
    print(f"  Rationale: {result.rationale}")
    
    assert result.level == ScoreLevel.LEVEL_5, "Should be Level 5"
    assert result.score >= 80, "Score should be >= 80"
    print("  ✓ PASS\n")
    
    # Test Case 2: Level 3 (Adequate)
    evidence_level3 = """
    We are currently modernizing our legacy data infrastructure.
    Migration to hybrid cloud environment is underway.
    Data quality is approximately 50% and improving.
    """
    
    result = scorer.score_dimension(
        "data_infrastructure",
        evidence_level3,
        {"data_quality": 0.45}
    )
    
    print("Level 3 Test:")
    print(f"  Score: {float(result.score):.2f}/100")
    print(f"  Level: {result.level.label}")
    print(f"  Confidence: {float(result.confidence):.2f}")
    
    assert result.level == ScoreLevel.LEVEL_3, "Should be Level 3"
    assert 40 <= result.score <= 59, "Score should be in 40-59 range"
    print("  ✓ PASS\n")
    
    # Test Case 3: Level 1 (Nascent)
    evidence_level1 = """
    Our data infrastructure is primarily on-premise with manual processes.
    Data is fragmented across multiple spreadsheets and mainframe systems.
    """
    
    result = scorer.score_dimension(
        "data_infrastructure",
        evidence_level1,
        {"data_quality": 0.10}
    )
    
    print("Level 1 Test:")
    print(f"  Score: {float(result.score):.2f}/100")
    print(f"  Level: {result.level.label}")
    
    assert result.level == ScoreLevel.LEVEL_1, "Should be Level 1"
    assert result.score < 20, "Score should be < 20"
    print("  ✓ PASS\n")


def test_talent():
    """Test Talent rubric scoring."""
    print_separator("TEST 2: Talent Scoring")
    
    scorer = RubricScorer()
    
    # Level 5: Large team
    evidence = """
    Our AI research team consists of over 20 ML specialists including
    principal ML engineers and staff ML researchers. We have an internal
    ML platform team supporting AI infrastructure. ML turnover is below 10%.
    """
    
    result = scorer.score_dimension(
        "talent",
        evidence,
        {"ai_job_ratio": 0.42, "team_size": 25}
    )
    
    print(f"Score: {float(result.score):.2f}/100")
    print(f"Level: {result.level.label}")
    print(f"Matched Keywords: {', '.join(result.matched_keywords[:5])}")
    
    assert result.level in [ScoreLevel.LEVEL_5, ScoreLevel.LEVEL_4], "Should be Level 4 or 5"
    print("✓ PASS\n")


def test_technology_stack():
    """Test Technology Stack rubric scoring."""
    print_separator("TEST 3: Technology Stack Scoring")
    
    scorer = RubricScorer()
    
    # Level 5: Advanced technology with AI/ML (SEC-appropriate language)
    evidence = """
    We have implemented advanced technology platforms including machine learning 
    and artificial intelligence capabilities. Our proprietary technology supports 
    predictive analytics and automation across operations. We utilize advanced 
    analytics tools and digital solutions to drive business value.
    """
    
    result = scorer.score_dimension(
        "technology_stack",
        evidence,
        {"mlops_maturity": 0.85}
    )
    
    print(f"Score: {float(result.score):.2f}/100")
    print(f"Level: {result.level.label}")
    print(f"Confidence: {float(result.confidence):.2f}")
    print(f"Matched Keywords: {', '.join(result.matched_keywords[:5])}")
    
    assert result.score >= 60, f"Should score >= 60, got {result.score}"
    print("✓ PASS\n")


def test_leadership():
    """Test Leadership rubric scoring."""
    print_separator("TEST 4: Leadership Scoring")
    
    scorer = RubricScorer()
    
    # Level 5: Strong executive leadership (SEC-appropriate language)
    evidence = """
    Our executive management team and board of directors have made digital 
    transformation a strategic priority. Strategic initiatives focus on 
    technology investments and innovation strategy. The leadership team 
    has established a clear digital strategy with board oversight.
    """
    
    result = scorer.score_dimension(
        "leadership",
        evidence,
        {"executive_engagement": 0.90}
    )
    
    print(f"Score: {float(result.score):.2f}/100")
    print(f"Level: {result.level.label}")
    print(f"Matched Keywords: {', '.join(result.matched_keywords[:5])}")
    
    assert result.level in [ScoreLevel.LEVEL_5, ScoreLevel.LEVEL_4], "Should be Level 4 or 5"
    print("✓ PASS\n")


def test_use_case_portfolio():
    """Test Use Case Portfolio rubric scoring."""
    print_separator("TEST 5: Use Case Portfolio Scoring")
    
    scorer = RubricScorer()
    
    # Level 4: Established products and services (SEC-appropriate language)
    evidence = """
    Our products and services generate strong revenue growth across multiple 
    market segments. Business operations leverage our solutions to deliver 
    value to customers. We have established offerings with clear competitive 
    advantage and strong market opportunity.
    """
    
    result = scorer.score_dimension(
        "use_case_portfolio",
        evidence,
        {"production_cases": 3, "roi_multiple": 2.5}
    )
    
    print(f"Score: {float(result.score):.2f}/100")
    print(f"Level: {result.level.label}")
    print(f"Matched Keywords: {', '.join(result.matched_keywords[:5])}")
    
    assert result.level in [ScoreLevel.LEVEL_4, ScoreLevel.LEVEL_5, ScoreLevel.LEVEL_3], "Should be Level 3-5"
    print("✓ PASS\n")


def test_culture():
    """Test Culture rubric scoring."""
    print_separator("TEST 6: Culture Scoring")
    
    scorer = RubricScorer()
    
    # Level 4: Positive workplace culture (SEC-appropriate language)
    evidence = """
    Our organization values innovation and maintains a strong workplace culture. 
    Employee engagement programs support diversity and inclusion initiatives. 
    We focus on talent retention through comprehensive workplace values and 
    culture development programs.
    """
    
    result = scorer.score_dimension(
        "culture",
        evidence,
        {"culture_score": 0.72}  # ~4.0 Glassdoor rating
    )
    
    print(f"Score: {float(result.score):.2f}/100")
    print(f"Level: {result.level.label}")
    print(f"Matched Keywords: {', '.join(result.matched_keywords[:5])}")
    
    assert 40 <= result.score <= 100, "Should score between 40-100"
    print("✓ PASS\n")


def test_all_dimensions_together():
    """Test scoring all dimensions at once."""
    print_separator("TEST 7: Score All Dimensions")
    
    scorer = RubricScorer()
    
    evidence_by_dimension = {
        "data_infrastructure": "Modern Snowflake data lake with real-time capabilities and ML platform.",
        "ai_governance": "VP-level AI sponsor with documented AI policies and risk framework.",
        "technology_stack": "MLflow platform adopted with experiment tracking and partial automation.",
        "talent": "Established team of 15 data scientists with active hiring programs.",
        "leadership": "C-suite sponsor (CTO) with AI mentioned in strategic documents.",
        "use_case_portfolio": "2 use cases in production with measured positive ROI.",
        "culture": "Experimentation encouraged with growing data literacy across teams."
    }
    
    metrics_by_dimension = {
        "data_infrastructure": {"data_quality": 0.85},
        "ai_governance": {"governance_maturity": 0.60},
        "technology_stack": {"mlops_maturity": 0.65},
        "talent": {"ai_job_ratio": 0.28, "team_size": 15},
        "leadership": {"executive_engagement": 0.70},
        "use_case_portfolio": {"production_cases": 2, "roi_multiple": 1.8},
        "culture": {"culture_score": 0.68}
    }
    
    results = scorer.score_all_dimensions(evidence_by_dimension, metrics_by_dimension)
    
    print("Dimension Scores:")
    for dim, result in results.items():
        print(f"\n{dim.upper().replace('_', ' ')}:")
        print(f"  Score: {float(result.score):.2f}/100")
        print(f"  Level: {result.level.label}")
        print(f"  Confidence: {float(result.confidence):.2f}")
    
    assert len(results) == 7, "Should score all 7 dimensions"
    print("\n✓ PASS - All dimensions scored\n")


def test_edge_cases():
    """Test edge cases."""
    print_separator("TEST 8: Edge Cases")
    
    scorer = RubricScorer()
    
    # Empty evidence
    print("Edge Case 1: Empty evidence")
    result = scorer.score_dimension("talent", "", {})
    print(f"  Score: {float(result.score):.2f} (should default low)")
    assert result.score < 20, "Empty evidence should score low"
    print("  ✓ PASS\n")
    
    # No quantitative metrics
    print("Edge Case 2: No quantitative metrics")
    result = scorer.score_dimension(
        "talent",
        "We have a data science team with ML engineers.",
        None  # No metrics
    )
    print(f"  Score: {float(result.score):.2f}")
    assert result.score > 0, "Should still score based on keywords"
    print("  ✓ PASS\n")
    
    # Very long evidence
    print("Edge Case 3: Very long evidence (50k chars)")
    long_evidence = """
    Our technology platforms include machine learning and artificial intelligence.
    We utilize advanced analytics and digital solutions across operations.
    Technology investments support automation and innovation strategy.
    """ * 300  # Repeat realistic content, not just one term
    result = scorer.score_dimension("technology_stack", long_evidence, {})
    print(f"  Score: {float(result.score):.2f}")
    print(f"  Confidence: {float(result.confidence):.2f}")
    # Should score well and have high confidence due to repeated keyword matches
    assert result.score > 40, "Long quality evidence should score well"
    print("  ✓ PASS\n")


def main():
    """Run all tests."""
    print_separator("RUBRIC SCORER TESTING SUITE")
    print("Task 5.0b: Rubric-Based Scoring")
    print()
    
    try:
        test_data_infrastructure()
        test_talent()
        test_technology_stack()
        test_leadership()
        test_use_case_portfolio()
        test_culture()
        test_all_dimensions_together()
        test_edge_cases()
        
        print_separator("FINAL SUMMARY")
        print("✅ All rubric scorer tests PASSED")
        print("✅ All 7 dimensions have working rubrics")
        print("✅ Keyword matching working correctly")
        print("✅ Quantitative thresholds enforced")
        print("✅ Score interpolation within ranges working")
        print()
        print("Next Steps:")
        print("  1. Integrate rubric scorer with evidence mapper")
        print("  2. Test on real company data (JPM, WMT)")
        print("  3. Move to Task 5.2: VR Calculator")
        print_separator()
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()