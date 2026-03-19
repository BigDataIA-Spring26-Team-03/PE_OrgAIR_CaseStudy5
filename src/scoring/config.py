from decimal import Decimal

class ScoringConfig:
    """All scoring parameters - CORRECTED v3.0"""
    
    # ===== V^R CALCULATION PARAMETERS =====
    # Non-compensatory penalty
    LAMBDA_PENALTY = Decimal("0.25") 
    
    # Dimension weights (must sum to 1.0)
    DIMENSION_WEIGHTS = [
        Decimal("0.15"),  # data_infrastructure
        Decimal("0.15"),  # ai_governance
        Decimal("0.15"),  # technology_stack
        Decimal("0.15"),  # talent
        Decimal("0.10"),  # leadership
        Decimal("0.15"),  # use_case_portfolio
        Decimal("0.15"),  # culture
    ]
    
    # Talent risk parameters (CORRECTED v3.0)
    TALENT_RISK_COEFFICIENT = Decimal("0.15") 
    TALENT_THRESHOLD = Decimal("0.25")
    
    # ===== H^R CALCULATION PARAMETERS =====
    # Position adjustment coefficient (CORRECTED v3.0)
    DELTA_POSITION = Decimal("0.15")
    
    # ===== ORG-AI-R PARAMETERS =====
    # Idiosyncratic weight (60% company-specific factors)
    ALPHA = Decimal("0.60")
    
    # Synergy weight (12% alignment effects)
    BETA = Decimal("0.12")
    
    # ===== SYNERGY PARAMETERS =====
    # TimingFactor bounds [0.8, 1.2]
    TIMING_MIN = Decimal("0.8")
    TIMING_MAX = Decimal("1.2")

# Validate configuration on import
assert sum(ScoringConfig.DIMENSION_WEIGHTS) == Decimal("1.0"), \
    "Dimension weights must sum to 1.0"

# Dimension names for reference
DIMENSION_NAMES = [
    "data_infrastructure",
    "ai_governance", 
    "technology_stack",
    "talent",
    "leadership",
    "use_case_portfolio",
    "culture",
]
