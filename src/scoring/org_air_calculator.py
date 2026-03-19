# src/scoring/org_air_calculator.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Any
import structlog

from .utils import to_decimal, clamp
from .config import ScoringConfig

logger = structlog.get_logger()


@dataclass
class OrgAIRResult:
    """Org-AI-R calculation result with complete audit trail."""
    org_air_score: Decimal
    vr_score: Decimal
    hr_score: Decimal
    synergy_score: Decimal
    
    # Weights used
    alpha: Decimal
    beta: Decimal
    
    # Intermediate calculations
    weighted_combination: Decimal
    synergy_contribution: Decimal
    base_contribution: Decimal
    
    # Raw inputs
    raw_inputs_vr: float
    raw_inputs_hr: float
    raw_inputs_synergy: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "org_air_score": float(self.org_air_score),
            "vr_score": float(self.vr_score),
            "hr_score": float(self.hr_score),
            "synergy_score": float(self.synergy_score),
            "weights": {
                "alpha": float(self.alpha),
                "beta": float(self.beta)
            },
            "breakdown": {
                "weighted_combination": float(self.weighted_combination),
                "base_contribution": float(self.base_contribution),
                "synergy_contribution": float(self.synergy_contribution)
            },
            "raw_inputs": {
                "vr": self.raw_inputs_vr,
                "hr": self.raw_inputs_hr,
                "synergy": self.raw_inputs_synergy
            }
        }


class OrgAIRCalculator:
    """
    Calculate Org-AI-R (Organizational AI Readiness) - Final Score.
    
    Uses parameters from ScoringConfig:
        - ALPHA = 0.60 (idiosyncratic weight)
        - BETA = 0.12 (synergy weight)
    
    Formula:
        Org-AI-R = (1 - β) · [α · V^R + (1 - α) · H^R] + β · Synergy
    """
    
    def __init__(self):
        """Initialize Org-AI-R Calculator using config values."""
        self.alpha = ScoringConfig.ALPHA
        self.beta = ScoringConfig.BETA
        
        logger.info(
            "OrgAIRCalculator initialized",
            alpha=float(self.alpha),
            beta=float(self.beta),
            vr_weight_percent=float(self.alpha * Decimal("100")),
            hr_weight_percent=float((Decimal("1") - self.alpha) * Decimal("100")),
            base_weight_percent=float((Decimal("1") - self.beta) * Decimal("100")),
            synergy_weight_percent=float(self.beta * Decimal("100"))
        )
    
    def calculate(
        self,
        vr_score: float,
        hr_score: float,
        synergy_score: float
    ) -> OrgAIRResult:
        """Calculate Org-AI-R score."""
        
        # Input validation
        if not 0 <= vr_score <= 100:
            raise ValueError(f"vr_score must be [0, 100], got {vr_score}")
        if not 0 <= hr_score <= 100:
            raise ValueError(f"hr_score must be [0, 100], got {hr_score}")
        if not 0 <= synergy_score <= 100:
            raise ValueError(f"synergy_score must be [0, 100], got {synergy_score}")
        
        # Store raw inputs
        raw_vr = vr_score
        raw_hr = hr_score
        raw_synergy = synergy_score
        
        # Convert to Decimal
        vr_dec = clamp(to_decimal(vr_score), Decimal("0"), Decimal("100"))
        hr_dec = clamp(to_decimal(hr_score), Decimal("0"), Decimal("100"))
        synergy_dec = clamp(to_decimal(synergy_score), Decimal("0"), Decimal("100"))
        
        logger.debug(
            "Org-AI-R inputs normalized",
            vr_score=float(vr_dec),
            hr_score=float(hr_dec),
            synergy_score=float(synergy_dec)
        )
        
        # Calculate weighted combination: α·VR + (1-α)·HR
        weighted_combo = (self.alpha * vr_dec) + ((Decimal("1") - self.alpha) * hr_dec)
        
        logger.debug(
            "Weighted combination calculated",
            formula="α·VR + (1-α)·HR",
            alpha=float(self.alpha),
            weighted_combo=float(weighted_combo),
            vr_contribution=float(self.alpha * vr_dec),
            hr_contribution=float((Decimal("1") - self.alpha) * hr_dec)
        )
        
        # Calculate base contribution: (1-β)·weighted_combo
        base_contribution = (Decimal("1") - self.beta) * weighted_combo
        
        logger.debug(
            "Base contribution calculated",
            formula="(1-β)·weighted_combo",
            base_contribution=float(base_contribution)
        )
        
        # Calculate synergy contribution: β·Synergy
        synergy_contribution = self.beta * synergy_dec
        
        logger.debug(
            "Synergy contribution calculated",
            formula="β·Synergy",
            synergy_contribution=float(synergy_contribution)
        )
        
        # Calculate final Org-AI-R
        org_air = base_contribution + synergy_contribution
        org_air = clamp(org_air, Decimal("0"), Decimal("100"))
        
        logger.debug(
            "Org-AI-R calculated",
            org_air=float(org_air),
            formula="base + synergy"
        )
        
        # Build result
        result = OrgAIRResult(
            org_air_score=org_air,
            vr_score=vr_dec,
            hr_score=hr_dec,
            synergy_score=synergy_dec,
            alpha=self.alpha,
            beta=self.beta,
            weighted_combination=weighted_combo,
            base_contribution=base_contribution,
            synergy_contribution=synergy_contribution,
            raw_inputs_vr=raw_vr,
            raw_inputs_hr=raw_hr,
            raw_inputs_synergy=raw_synergy
        )
        
        logger.info(
            "org_air_calculated",
            vr_score=float(vr_dec),
            hr_score=float(hr_dec),
            synergy_score=float(synergy_dec),
            weighted_combo=float(weighted_combo),
            base_contribution=float(base_contribution),
            synergy_contribution=float(synergy_contribution),
            org_air_score=float(org_air),
            alpha=float(self.alpha),
            beta=float(self.beta)
        )
        
        return result


def calculate_org_air(
    vr_score: float,
    hr_score: float,
    synergy_score: float
) -> OrgAIRResult:
    """Convenience function for calculating Org-AI-R."""
    calc = OrgAIRCalculator()
    return calc.calculate(vr_score, hr_score, synergy_score)
