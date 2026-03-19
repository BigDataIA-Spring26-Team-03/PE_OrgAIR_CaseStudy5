# src/scoring/confidence.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Any, Optional
import math
import structlog

from .utils import to_decimal, clamp

logger = structlog.get_logger()


@dataclass
class ConfidenceInterval:
    """Confidence interval result with SEM details."""
    point_estimate: Decimal
    ci_lower: Decimal
    ci_upper: Decimal
    sem: Decimal
    reliability: Decimal
    evidence_count: int
    confidence_level: Decimal
    
    # Raw inputs for audit
    raw_inputs_score: float
    raw_inputs_evidence_count: int
    
    @property
    def ci_width(self) -> Decimal:
        """Width of confidence interval."""
        return self.ci_upper - self.ci_lower
    
    @property
    def margin_of_error(self) -> Decimal:
        """Margin of error (half-width)."""
        return self.ci_width / Decimal("2")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "point_estimate": float(self.point_estimate),
            "ci_lower": float(self.ci_lower),
            "ci_upper": float(self.ci_upper),
            "sem": float(self.sem),
            "reliability": float(self.reliability),
            "evidence_count": self.evidence_count,
            "confidence_level": float(self.confidence_level),
            "ci_width": float(self.ci_width),
            "margin_of_error": float(self.margin_of_error),
            "raw_inputs": {
                "score": self.raw_inputs_score,
                "evidence_count": self.raw_inputs_evidence_count
            }
        }


class ConfidenceCalculator:
    """
    Calculate SEM-based confidence intervals using Spearman-Brown formula.
    
    Formulas:
        Reliability: ρ = (n × r) / (1 + (n-1) × r)
        SEM: SEM = σ × √(1 - ρ)
        CI: score ± z × SEM
    
    Where:
        n = number of evidence items
        r = average inter-item correlation
        σ = population standard deviation
        z = critical value (1.96 for 95% CI)
    """
    
    # Population standard deviations by score type
    POPULATION_SD: Dict[str, Decimal] = {
        "vr": Decimal("15.0"),       # V^R scores
        "hr": Decimal("12.0"),       # H^R scores
        "synergy": Decimal("10.0"),  # Synergy scores
        "org_air": Decimal("14.0"),  # Final Org-AI-R scores
        "dimension": Decimal("16.0") # Individual dimensions
    }
    
    # Average inter-item correlation (from psychometric research)
    DEFAULT_ITEM_CORRELATION = Decimal("0.30")
    
    # Correlation by score type (can differ)
    SCORE_TYPE_CORRELATIONS: Dict[str, Decimal] = {
        "vr": Decimal("0.30"),
        "hr": Decimal("0.35"),      # H^R sources more correlated
        "synergy": Decimal("0.40"),  # Synergy components tightly linked
        "org_air": Decimal("0.32"),  # Final score moderate correlation
        "dimension": Decimal("0.25") # Dimensions more independent
    }
    
    # Z-scores for confidence levels
    Z_SCORES: Dict[float, Decimal] = {
        0.90: Decimal("1.645"),
        0.95: Decimal("1.96"),
        0.99: Decimal("2.576")
    }
    
    def __init__(self):
        """Initialize confidence calculator."""
        logger.info(
            "ConfidenceCalculator initialized",
            population_sds={k: float(v) for k, v in self.POPULATION_SD.items()},
            default_correlation=float(self.DEFAULT_ITEM_CORRELATION)
        )
    
    def calculate(
        self,
        score: float,
        score_type: str,
        evidence_count: int,
        item_correlation: Optional[float] = None,
        confidence_level: float = 0.95
    ) -> ConfidenceInterval:
        """
        Calculate SEM-based confidence interval.
        
        Args:
            score: Point estimate for the score (0-100)
            score_type: Type of score ("vr", "hr", "synergy", "org_air", "dimension")
            evidence_count: Number of evidence items/sources
            item_correlation: Inter-item correlation (default: use score_type default)
            confidence_level: Confidence level (0.90, 0.95, or 0.99)
        
        Returns:
            ConfidenceInterval with SEM and CI bounds
        """
        
        # ===== INPUT VALIDATION =====
        if not 0 <= score <= 100:
            raise ValueError(f"score must be 0-100, got {score}")
        
        if evidence_count < 1:
            raise ValueError(f"evidence_count must be >= 1, got {evidence_count}")
        
        if confidence_level not in self.Z_SCORES:
            raise ValueError(f"confidence_level must be 0.90, 0.95, or 0.99, got {confidence_level}")
        
        # Store raw inputs
        raw_score = score
        raw_evidence_count = evidence_count
        
        # ===== STEP 1: CONVERT TO DECIMAL =====
        score_dec = to_decimal(score)
        
        # Get correlation (use provided or default for score type)
        if item_correlation is not None:
            r = to_decimal(item_correlation)
        else:
            r = self.SCORE_TYPE_CORRELATIONS.get(score_type, self.DEFAULT_ITEM_CORRELATION)
        
        # Ensure at least 1 evidence item
        n = Decimal(str(max(evidence_count, 1)))
        
        logger.debug(
            "CI calculation inputs",
            score=float(score_dec),
            score_type=score_type,
            evidence_count=int(n),
            correlation=float(r),
            confidence_level=confidence_level
        )
        
        # ===== STEP 2: CALCULATE RELIABILITY (Spearman-Brown) =====
        # ρ = (n × r) / (1 + (n-1) × r)
        numerator = n * r
        denominator = Decimal("1.0") + (n - Decimal("1.0")) * r
        reliability = numerator / denominator
        
        # Clamp to [0, 0.99] to avoid sqrt domain errors
        reliability = clamp(reliability, Decimal("0.0"), Decimal("0.99"))
        
        logger.debug(
            "Reliability calculated (Spearman-Brown)",
            reliability=float(reliability),
            reliability_percentage=float(reliability * Decimal("100"))
        )
        
        # ===== STEP 3: GET POPULATION SD =====
        sigma = self.POPULATION_SD.get(score_type, Decimal("15.0"))
        
        logger.debug(
            "Population SD selected",
            score_type=score_type,
            sigma=float(sigma)
        )
        
        # ===== STEP 4: CALCULATE SEM =====
        # SEM = σ × √(1 - ρ)
        sqrt_term = Decimal(str(math.sqrt(float(Decimal("1.0") - reliability))))
        sem = sigma * sqrt_term
        
        logger.debug(
            "SEM calculated",
            sem=float(sem),
            sqrt_term=float(sqrt_term)
        )
        
        # ===== STEP 5: GET Z-SCORE =====
        z = self.Z_SCORES[confidence_level]
        
        # ===== STEP 6: CALCULATE MARGIN OF ERROR =====
        margin = z * sem
        
        logger.debug(
            "Margin of error calculated",
            z_score=float(z),
            margin=float(margin)
        )
        
        # ===== STEP 7: CALCULATE CI BOUNDS =====
        ci_lower = score_dec - margin
        ci_upper = score_dec + margin
        
        # Clamp to valid score range [0, 100]
        ci_lower = clamp(ci_lower, Decimal("0"), Decimal("100"))
        ci_upper = clamp(ci_upper, Decimal("0"), Decimal("100"))
        
        # ===== BUILD RESULT =====
        result = ConfidenceInterval(
            point_estimate=score_dec,
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            sem=sem,
            reliability=reliability,
            evidence_count=int(n),
            confidence_level=to_decimal(confidence_level),
            raw_inputs_score=raw_score,
            raw_inputs_evidence_count=raw_evidence_count
        )
        
        logger.info(
            "Confidence interval calculated",
            score=float(score_dec),
            score_type=score_type,
            evidence_count=int(n),
            reliability=float(reliability),
            sem=float(sem),
            ci_lower=float(ci_lower),
            ci_upper=float(ci_upper),
            ci_width=float(result.ci_width)
        )
        
        return result


# ===== HELPER FUNCTION FOR TESTING =====
def calculate_confidence_interval(
    score: float,
    score_type: str,
    evidence_count: int
) -> ConfidenceInterval:
    """Convenience function for calculating confidence interval."""
    calc = ConfidenceCalculator()
    return calc.calculate(score, score_type, evidence_count)