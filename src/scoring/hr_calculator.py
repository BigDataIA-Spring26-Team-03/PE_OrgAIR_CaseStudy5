# src/scoring/hr_calculator.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Any, Optional
import structlog

from .utils import to_decimal, clamp

logger = structlog.get_logger()


@dataclass
class HRResult:
    """H^R (Horizon Readiness) calculation result with audit trail."""
    hr_score: Decimal
    hr_base: Decimal
    position_factor: Decimal
    delta_used: Decimal
    sector: str
    
    # Raw inputs for audit
    raw_inputs_hr_base: float
    raw_inputs_position_factor: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "hr_score": float(self.hr_score),
            "hr_base": float(self.hr_base),
            "position_factor": float(self.position_factor),
            "delta": float(self.delta_used),
            "sector": self.sector,
            "raw_inputs": {
                "hr_base": self.raw_inputs_hr_base,
                "position_factor": self.raw_inputs_position_factor
            }
        }


class HRCalculator:
    """
    Calculate H^R (Horizon Readiness) - sector benchmark score.
    
    H^R measures how AI-ready the company's SECTOR is, adjusted by
    the company's position within that sector.
    
    Formula:
        H^R = H^R_base × (1 + δ × PositionFactor)
    
    Where:
        - H^R_base: Sector baseline readiness (from industries table)
        - δ = 0.15: Position adjustment coefficient (CORRECTED in v3.0)
        - PositionFactor: Company's position vs. sector peers [-1, 1]
    """
    
    # Corrected delta (was 0.5 in old versions, now 0.15)
    DELTA_POSITION = Decimal("0.15")
    
    # Sector baseline H^R scores (from industries table)
    # These are fallback values if database query fails
    SECTOR_HR_BASE: Dict[str, float] = {
        "technology": 75.0,
        "financial_services": 80.0,
        "financial": 80.0,
        "healthcare": 78.0,
        "business_services": 75.0,
        "services": 75.0,
        "retail": 70.0,
        "consumer": 70.0,
        "manufacturing": 72.0,
        "industrials": 72.0
    }
    
    def __init__(self, use_database: bool = True):
        """
        Initialize HR Calculator.
        
        Args:
            use_database: Query HR_base from Snowflake (True) or use hardcoded (False)
        """
        self.use_database = use_database
        self.delta = self.DELTA_POSITION
        
        logger.info(
            "HRCalculator initialized",
            delta=float(self.delta),
            use_database=use_database,
            sector_baselines=self.SECTOR_HR_BASE
        )
    
    def get_hr_base(self, sector: str) -> float:
        """
        Get H^R baseline for a sector.
        
        Tries database first, falls back to hardcoded values.
        
        Args:
            sector: Company sector
            
        Returns:
            H^R baseline score (0-100)
        """
        
        if self.use_database:
            try:
                from app.services.snowflake import db
                
                # Try to fetch from industries table
                query = f"""
                    SELECT h_r_base 
                    FROM industries 
                    WHERE LOWER(sector) = '{sector.lower()}'
                    OR LOWER(name) = '{sector.lower()}'
                    LIMIT 1
                """
                
                result = db.execute_query(query)
                
                if result and (result[0].get('H_R_BASE') or result[0].get('h_r_base')):
                    hr_base = float(result[0].get('H_R_BASE') or result[0].get('h_r_base'))
                    logger.debug(f"HR_base from database for {sector}: {hr_base}")
                    return hr_base
                    
            except Exception as e:
                logger.warning(f"Could not fetch HR_base from database: {e}")
        
        # Fallback to hardcoded
        sector_normalized = sector.lower().replace(" ", "_")
        hr_base = self.SECTOR_HR_BASE.get(sector_normalized, 75.0)
        
        logger.debug(f"HR_base from hardcoded for {sector}: {hr_base}")
        return hr_base
    
    def calculate(
        self,
        sector: str,
        position_factor: float
    ) -> HRResult:
        """
        Calculate H^R score.
        
        Args:
            sector: Company sector
            position_factor: Position factor from PositionFactorCalculator [-1, 1]
            
        Returns:
            HRResult with H^R score and audit trail
        """
        
        # ===== INPUT VALIDATION =====
        if not -1 <= position_factor <= 1:
            raise ValueError(f"position_factor must be [-1, 1], got {position_factor}")
        
        # Store raw inputs
        raw_pf = position_factor
        
        # ===== STEP 1: GET HR_BASE =====
        hr_base = self.get_hr_base(sector)
        raw_hr_base = hr_base
        
        # ===== STEP 2: CONVERT TO DECIMAL =====
        hr_base_dec = to_decimal(hr_base)
        pf_dec = to_decimal(position_factor)
        
        logger.debug(
            "HR inputs normalized",
            sector=sector,
            hr_base=float(hr_base_dec),
            position_factor=float(pf_dec),
            delta=float(self.delta)
        )
        
        # ===== STEP 3: CALCULATE HR ADJUSTMENT =====
        # adjustment = 1 + δ × PositionFactor
        adjustment = Decimal("1.0") + self.delta * pf_dec
        
        logger.debug(
            "HR adjustment calculated",
            adjustment=float(adjustment),
            adjustment_percentage=float((adjustment - Decimal("1.0")) * Decimal("100"))
        )
        
        # ===== STEP 4: CALCULATE H^R SCORE =====
        hr_score = hr_base_dec * adjustment
        
        # ===== STEP 5: CLAMP TO 0-100 =====
        hr_score = clamp(hr_score, Decimal("0"), Decimal("100"))
        
        # ===== BUILD RESULT =====
        result = HRResult(
            hr_score=hr_score,
            hr_base=hr_base_dec,
            position_factor=pf_dec,
            delta_used=self.delta,
            sector=sector,
            raw_inputs_hr_base=raw_hr_base,
            raw_inputs_position_factor=raw_pf
        )
        
        logger.info(
            "H^R calculated",
            sector=sector,
            hr_base=float(hr_base_dec),
            position_factor=float(pf_dec),
            delta=float(self.delta),
            adjustment=float(adjustment),
            hr_score=float(hr_score)
        )
        
        return result


# ===== HELPER FUNCTION FOR TESTING =====
def calculate_hr(
    sector: str,
    position_factor: float
) -> HRResult:
    """Convenience function for calculating H^R."""
    calc = HRCalculator()
    return calc.calculate(sector, position_factor)