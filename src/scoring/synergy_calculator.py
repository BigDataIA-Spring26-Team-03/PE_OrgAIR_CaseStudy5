# src/scoring/synergy_calculator.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Any
import structlog

from .utils import to_decimal, clamp
from .config import ScoringConfig

logger = structlog.get_logger()


@dataclass
class SynergyResult:
    """Synergy calculation result with audit trail."""
    synergy_score: Decimal
    vr_score: Decimal
    hr_score: Decimal
    alignment_factor: Decimal
    timing_factor: Decimal
    interaction: Decimal
    
    # Raw inputs for audit
    raw_inputs_vr: float
    raw_inputs_hr: float
    raw_inputs_alignment: float
    raw_inputs_timing: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "synergy_score": float(self.synergy_score),
            "vr_score": float(self.vr_score),
            "hr_score": float(self.hr_score),
            "alignment_factor": float(self.alignment_factor),
            "timing_factor": float(self.timing_factor),
            "interaction": float(self.interaction),
            "raw_inputs": {
                "vr": self.raw_inputs_vr,
                "hr": self.raw_inputs_hr,
                "alignment": self.raw_inputs_alignment,
                "timing": self.raw_inputs_timing
            }
        }


class SynergyCalculator:
    """
    Calculate Synergy Score with TimingFactor.
    
    Uses parameters from ScoringConfig:
        - TIMING_MIN = 0.8
        - TIMING_MAX = 1.2
    """
    
    # Default values
    DEFAULT_ALIGNMENT = Decimal("1.0")
    DEFAULT_TIMING = Decimal("1.0")
    
    def __init__(self):
        """Initialize Synergy Calculator using config values."""
        self.timing_min = ScoringConfig.TIMING_MIN
        self.timing_max = ScoringConfig.TIMING_MAX
        
        logger.info(
            "SynergyCalculator initialized",
            timing_range=[float(self.timing_min), float(self.timing_max)],
            default_alignment=float(self.DEFAULT_ALIGNMENT),
            default_timing=float(self.DEFAULT_TIMING)
        )
    
    def calculate(
        self,
        vr_score: float,
        hr_score: float,
        alignment_factor: float = 1.0,
        timing_factor: float = 1.0
    ) -> SynergyResult:
        """Calculate Synergy score."""
        
        # Input validation
        if not 0 <= vr_score <= 100:
            raise ValueError(f"vr_score must be [0, 100], got {vr_score}")
        if not 0 <= hr_score <= 100:
            raise ValueError(f"hr_score must be [0, 100], got {hr_score}")
        
        # Store raw inputs
        raw_vr = vr_score
        raw_hr = hr_score
        raw_alignment = alignment_factor
        raw_timing = timing_factor
        
        # Convert to Decimal
        vr_dec = clamp(to_decimal(vr_score), Decimal("0"), Decimal("100"))
        hr_dec = clamp(to_decimal(hr_score), Decimal("0"), Decimal("100"))
        alignment_dec = to_decimal(alignment_factor)
        
        # CRITICAL: Clamp timing factor using config values
        timing_dec = clamp(
            to_decimal(timing_factor),
            self.timing_min,
            self.timing_max
        )
        
        # Log if timing was clamped
        if abs(timing_factor - float(timing_dec)) > 0.001:
            logger.warning(
                "TimingFactor clamped",
                original_value=timing_factor,
                clamped_value=float(timing_dec),
                allowed_range=[float(self.timing_min), float(self.timing_max)]
            )
        
        logger.debug(
            "Synergy inputs normalized",
            vr_score=float(vr_dec),
            hr_score=float(hr_dec),
            alignment_factor=float(alignment_dec),
            timing_factor=float(timing_dec),
            timing_clamped=(timing_factor != float(timing_dec))
        )
        
        # Calculate interaction term
        interaction = (vr_dec * hr_dec) / Decimal("100.0")
        
        logger.debug(
            "Interaction term calculated",
            interaction=float(interaction),
            formula="(VR × HR) / 100"
        )
        
        # Calculate synergy
        synergy = interaction * alignment_dec * timing_dec
        synergy = clamp(synergy, Decimal("0"), Decimal("100"))
        
        logger.debug(
            "Synergy score calculated",
            synergy=float(synergy),
            formula="Interaction × Alignment × TimingFactor"
        )
        
        # Build result
        result = SynergyResult(
            synergy_score=synergy,
            vr_score=vr_dec,
            hr_score=hr_dec,
            alignment_factor=alignment_dec,
            timing_factor=timing_dec,
            interaction=interaction,
            raw_inputs_vr=raw_vr,
            raw_inputs_hr=raw_hr,
            raw_inputs_alignment=raw_alignment,
            raw_inputs_timing=raw_timing
        )
        
        logger.info(
            "synergy_calculated",
            vr_score=float(vr_dec),
            hr_score=float(hr_dec),
            alignment_factor=float(alignment_dec),
            timing_factor=float(timing_dec),
            interaction=float(interaction),
            synergy_score=float(synergy),
            timing_was_clamped=(raw_timing != float(timing_dec))
        )
        
        return result


def calculate_synergy(
    vr_score: float,
    hr_score: float,
    alignment_factor: float = 1.0,
    timing_factor: float = 1.0
) -> SynergyResult:
    """Convenience function for calculating Synergy."""
    calc = SynergyCalculator()
    return calc.calculate(vr_score, hr_score, alignment_factor, timing_factor)
