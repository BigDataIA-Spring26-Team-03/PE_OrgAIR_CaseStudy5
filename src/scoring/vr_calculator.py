from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict, Any
import structlog

from .utils import to_decimal, clamp, weighted_mean, weighted_std_dev, coefficient_of_variation
from .config import ScoringConfig, DIMENSION_NAMES

logger = structlog.get_logger()


@dataclass
class VRResult:
    # V^R calculation result with complete audit trail.
    vr_score: Decimal
    weighted_mean: Decimal  
    coefficient_of_variation: Decimal
    penalty_factor: Decimal
    talent_concentration: Decimal
    talent_risk_adjustment: Decimal
    dimension_scores: List[Decimal]
    dimension_contributions: List[Decimal]
    
    # Store raw inputs for audit/debugging
    raw_inputs_dimension_scores: List[float]
    raw_inputs_talent_concentration: float
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to JSON-serializable dictionary.
        Useful for saving results and API responses.
        """
        return {
            "vr_score": float(self.vr_score),
            "weighted_mean": float(self.weighted_mean),
            "cv": float(self.coefficient_of_variation),
            "penalty_factor": float(self.penalty_factor),
            "talent_concentration": float(self.talent_concentration),
            "talent_risk_adj": float(self.talent_risk_adjustment),
            "dimension_scores": {
                DIMENSION_NAMES[i]: float(score) 
                for i, score in enumerate(self.dimension_scores)
            },
            "dimension_contributions": {
                DIMENSION_NAMES[i]: float(contrib)
                for i, contrib in enumerate(self.dimension_contributions)
            },
            "raw_inputs": {
                "dimension_scores": self.raw_inputs_dimension_scores,
                "talent_concentration": self.raw_inputs_talent_concentration
            }
        }


class VRCalculator:
    # Calculates V^R (Idiosyncratic Readiness) score.

    def __init__(self):
        # Initialize with PE-Org business rules
        self.lambda_penalty = ScoringConfig.LAMBDA_PENALTY
        self.weights = ScoringConfig.DIMENSION_WEIGHTS
        self.talent_risk_coeff = ScoringConfig.TALENT_RISK_COEFFICIENT
        self.talent_threshold = ScoringConfig.TALENT_THRESHOLD
        
        logger.info(
            "VRCalculator initialized",
            lambda_penalty=float(self.lambda_penalty),
            dimension_weights=[float(w) for w in self.weights],
            talent_risk_coeff=float(self.talent_risk_coeff),
            talent_threshold=float(self.talent_threshold)
        )
    
    def calculate(
        self,
        dimension_scores: List[float],
        talent_concentration: float
    ) -> VRResult:
        # ===== INPUT VALIDATION =====
        if len(dimension_scores) != 7:
            raise ValueError(
                f"Expected 7 dimension scores, got {len(dimension_scores)}"
            )
        
        # Store raw inputs for audit
        raw_dim_scores = dimension_scores.copy()
        raw_tc = talent_concentration
        
        # ===== STEP 1: CONVERT TO DECIMAL & CLAMP =====
        d_scores = [clamp(to_decimal(s)) for s in dimension_scores]
        tc = clamp(to_decimal(talent_concentration), Decimal(0), Decimal(1))
        
        logger.debug(
            "Inputs normalized",
            dimension_scores=[float(s) for s in d_scores],
            talent_concentration=float(tc)
        )
        
        # ===== STEP 2: WEIGHTED MEAN (Dw) =====
        d_bar_w = weighted_mean(d_scores, self.weights)
        d_bar_w = clamp(d_bar_w)  # Ensure intermediate value is bounded
        
        logger.debug(
            "Weighted mean calculated",
            weighted_mean=float(d_bar_w)
        )
        
        # ===== STEP 3: COEFFICIENT OF VARIATION (cvD) =====
        std_dev = weighted_std_dev(d_scores, self.weights, d_bar_w)
        cv_d = coefficient_of_variation(std_dev, d_bar_w)
        
        logger.debug(
            "CV calculated",
            std_dev=float(std_dev),
            cv=float(cv_d)
        )
        
        # ===== STEP 4: PENALTY FACTOR (1 - λ × cvD) =====
        penalty_factor = Decimal(1) - self.lambda_penalty * cv_d
        penalty_factor = clamp(penalty_factor, Decimal(0), Decimal(1))
        
        logger.debug(
            "Penalty factor calculated",
            penalty_factor=float(penalty_factor),
            reduction=float(Decimal(1) - penalty_factor)
        )
        
        # ===== STEP 5: TALENT RISK ADJUSTMENT =====
        tc_excess = max(Decimal(0), tc - self.talent_threshold)
        talent_risk_adj = Decimal(1) - self.talent_risk_coeff * tc_excess
        talent_risk_adj = clamp(talent_risk_adj, Decimal(0), Decimal(1))
        
        logger.debug(
            "Talent risk adjustment calculated",
            tc_excess=float(tc_excess),
            talent_risk_adj=float(talent_risk_adj),
            reduction=float(Decimal(1) - talent_risk_adj)
        )
        
        # ===== STEP 6:  FINAL V^R SCORE =====
        vr_score = d_bar_w * penalty_factor * talent_risk_adj
        vr_score = clamp(vr_score)  # Final bounds check
        
        # ===== STEP 7: DIMENSION CONTRIBUTIONS (for audit) =====
        contributions = [d * w for d, w in zip(d_scores, self.weights)]
        
        # ===== BUILD RESULT =====
        result = VRResult(
            vr_score=vr_score,
            weighted_mean=d_bar_w,
            coefficient_of_variation=cv_d,
            penalty_factor=penalty_factor,
            talent_concentration=tc,
            talent_risk_adjustment=talent_risk_adj,
            dimension_scores=d_scores,
            dimension_contributions=contributions,
            raw_inputs_dimension_scores=raw_dim_scores,
            raw_inputs_talent_concentration=raw_tc
        )
        
        logger.info(
            "V^R score calculated",
            vr_score=float(result.vr_score),
            weighted_mean=float(result.weighted_mean),
            cv=float(result.coefficient_of_variation),
            penalty_factor=float(result.penalty_factor),
            talent_concentration=float(result.talent_concentration),
            talent_risk_adj=float(result.talent_risk_adjustment)
        )
        
        return result


# ===== HELPER FUNCTION FOR TESTING =====
def calculate_vr(
    dimension_scores: List[float],
    talent_concentration: float
) -> VRResult:
    calc = VRCalculator()
    return calc.calculate(dimension_scores, talent_concentration)