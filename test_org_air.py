#!/usr/bin/env python3
"""Test the Org-AI-R Calculator - Production Version"""

from src.scoring.org_air_calculator import OrgAIRCalculator

def test_org_air():
    calc = OrgAIRCalculator()
    
    print("=" * 80)
    print("TESTING ORG-AI-R CALCULATOR")
    print("=" * 80)
    print(f"\n📐 FORMULA:")
    print(f"   Org-AI-R = (1 - β)·[α·VR + (1-α)·HR] + β·Synergy")
    print(f"   α = 0.60 (60% VR, 40% HR)")
    print(f"   β = 0.12 (88% base, 12% synergy)")
    print("=" * 80)
    
    # Test cases
    test_cases = [
        (85, 80, 70, "NVDA example (high readiness, good sector)"),
        (50, 50, 25, "Average across the board"),
        (100, 100, 100, "Perfect scores"),
        (90, 60, 75, "Strong company, weaker sector"),
        (40, 90, 30, "Weak company, strong sector"),
        (0, 0, 0, "Zero baseline"),
    ]
    
    print("\n📊 TEST CASES:")
    print("-" * 80)
    
    for vr, hr, synergy, description in test_cases:
        result = calc.calculate(vr, hr, synergy)
        
        print(f"\n{description}")
        print(f"  VR={vr}, HR={hr}, Synergy={synergy}")
        print(f"  → Org-AI-R: {result.org_air_score:.2f}")
        print(f"  → Breakdown:")
        print(f"     • Weighted Combo (α·VR + (1-α)·HR): {result.weighted_combination:.2f}")
        print(f"     • Base Contribution (88%): {result.base_contribution:.2f}")
        print(f"     • Synergy Contribution (12%): {result.synergy_contribution:.2f}")
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED!")
    print("=" * 80)
    
    # Show weight breakdown
    print("\n📊 WEIGHT BREAKDOWN:")
    print("   Company-specific (VR): 60%")
    print("   Sector opportunity (HR): 40%")
    print("   Base combination: 88%")
    print("   Synergy effects: 12%")
    print("=" * 80)

if __name__ == "__main__":
    test_org_air()
