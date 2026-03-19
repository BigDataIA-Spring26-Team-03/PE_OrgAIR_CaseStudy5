from decimal import Decimal, ROUND_HALF_UP, getcontext
from typing import List

# Set global precision for financial calculations
getcontext().prec = 10

def to_decimal(value: float, places: int = 4) -> Decimal:
    #Convert float to Decimal with explicit precision. (0.3333333=
    if isinstance(value, Decimal):
        return value.quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP)
    return Decimal(str(value)).quantize(
        Decimal(10) ** -places,
        rounding=ROUND_HALF_UP
    )

def clamp(
    value: Decimal, 
    min_val: Decimal = Decimal(0), 
    max_val: Decimal = Decimal(100)
) -> Decimal:
  #Guarantee scores stay in business bounds (0–100 for VR components, etc.)
    return max(min_val, min(max_val, value))

def weighted_mean(values: List[Decimal], weights: List[Decimal]) -> Decimal:
    #Calculate weighted mean using Decimal precision.
    if len(values) != len(weights):
        raise ValueError("Values and weights must have same length")
    
    if not values:
        return Decimal(0)
    
    # Calculate weighted sum
    total = sum(v * w for v, w in zip(values, weights))
    
    return total.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

#Both of these are used to penalize imbalance across dimensions  EG: Technology=90 Governance=20 

#how spread out the scores are from the average
def weighted_std_dev(
    values: List[Decimal],  #List of scores
    weights: List[Decimal],  #Scores for each dimensions
    mean: Decimal
) -> Decimal:
    if not values:
        return Decimal(0)
    
    # Weighted variance
    variance = sum(w * (v - mean) ** 2 for v, w in zip(values, weights))
    
    # Standard deviation (sqrt)
    std_dev = variance.sqrt()
    
    return std_dev.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

def coefficient_of_variation(std_dev: Decimal, mean: Decimal) -> Decimal:
    if mean == Decimal(0):
        return Decimal(0)  # Avoid division by zero
    
    cv = (std_dev / mean).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
    return cv