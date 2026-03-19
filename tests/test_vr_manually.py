from src.scoring.vr_calculator import VRCalculator
from decimal import Decimal

# Initialize calculator
calc = VRCalculator()

# Sample inputs (simulating NVDA - high scores, low TC)
sample_scores = [85, 90, 88, 92, 78, 85, 82]
sample_tc = 0.15

print("="*60)
print("V^R CALCULATOR SMOKE TEST")
print("="*60)

print(f"\nInputs:")
print(f"  Dimension scores: {sample_scores}")
print(f"  Talent concentration: {sample_tc}")

# Calculate
result = calc.calculate(sample_scores, sample_tc)

print(f"\nResults:")
print(f"  V^R Score: {float(result.vr_score):.2f}")
print(f"  Weighted Mean: {float(result.weighted_mean):.2f}")
print(f"  CV: {float(result.coefficient_of_variation):.4f}")
print(f"  Penalty Factor: {float(result.penalty_factor):.4f}")
print(f"  Talent Risk Adj: {float(result.talent_risk_adjustment):.4f}")

print(f"\nDimension Contributions:")
for i, contrib in enumerate(result.dimension_contributions):
    from src.scoring.config import DIMENSION_NAMES
    print(f"  {DIMENSION_NAMES[i]:25s}: {float(contrib):.2f}")

print(f"\n{'='*60}")
print(f"✓ V^R Calculator is working!")
print(f"{'='*60}")