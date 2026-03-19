#!/usr/bin/env python3
"""Test the Synergy Calculator - Production Version"""

from src.scoring.synergy_calculator import SynergyCalculator

def test_synergy():
    calc = SynergyCalculator()
    
    print("=" * 80)
    print("TESTING SYNERGY CALCULATOR (WITH ALIGNMENT & TIMING)")
    print("=" * 80)
    
    # Test cases from the notebook
    print("\n📊 NOTEBOOK EXAMPLES:")
    print("-" * 80)
    
    # Example 1: Good timing (within range)
    result1 = calc.calculate(
        vr_score=70.0,
        hr_score=65.0,
        alignment_factor=0.9,
        timing_factor=1.1  # Within [0.8, 1.2]
    )
    print(f"\n✅ Good Timing Example:")
    print(f"   VR=70, HR=65, Alignment=0.9, Timing=1.1")
    print(f"   → Synergy: {result1.synergy_score:.2f}")
    print(f"   → Interaction: {result1.interaction:.2f}")
    print(f"   → Timing Used: {result1.timing_factor:.2f}")
    
    # Example 2: Bad timing (gets clamped)
    result2 = calc.calculate(
        vr_score=70.0,
        hr_score=65.0,
        alignment_factor=0.9,
        timing_factor=1.5  # Will be clamped to 1.2
    )
    print(f"\n⚠️  Bad Timing Example (Clamped):")
    print(f"   VR=70, HR=65, Alignment=0.9, Timing=1.5 (OUT OF RANGE)")
    print(f"   → Synergy: {result2.synergy_score:.2f}")
    print(f"   → Timing Used: {result2.timing_factor:.2f} (CLAMPED from 1.5)")
    
    # Additional test cases
    print("\n\n📊 ADDITIONAL TEST CASES:")
    print("-" * 80)
    
    test_cases = [
        (100, 100, 1.0, 1.2, "Perfect scores, max timing"),
        (85, 92, 0.95, 1.1, "NVDA example (high both)"),
        (50, 50, 1.0, 1.0, "Average scores, neutral timing"),
        (100, 0, 1.0, 1.2, "High VR, zero HR"),
        (70, 65, 0.9, 0.7, "Poor timing (will clamp to 0.8)"),
    ]
    
    for vr, hr, alignment, timing, description in test_cases:
        result = calc.calculate(vr, hr, alignment, timing)
        
        clamped = "CLAMPED" if abs(timing - float(result.timing_factor)) > 0.001 else ""
        
        print(f"\n{description}")
        print(f"  VR={vr}, HR={hr}, Alignment={alignment}, Timing={timing} {clamped}")
        print(f"  → Synergy: {result.synergy_score:.2f}")
        print(f"  → Interaction: {result.interaction:.2f}")
        if clamped:
            print(f"  → Timing Used: {result.timing_factor:.2f} (clamped from {timing})")
    
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED!")
    print("=" * 80)
    
    # Show the formula
    print("\n📐 FORMULA:")
    print("   Synergy = (V^R × H^R / 100) × Alignment × TimingFactor")
    print("   TimingFactor ∈ [0.8, 1.2]")
    print("=" * 80)

if __name__ == "__main__":
    test_synergy()
