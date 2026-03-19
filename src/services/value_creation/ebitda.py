from dataclasses import dataclass


@dataclass
class EBITDAProjection:
    company_id: str
    entry_score: float
    exit_score: float
    delta_air: float
    conservative_pct: float
    base_pct: float
    optimistic_pct: float
    risk_adjusted_pct: float
    requires_approval: bool


class EBITDACalculator:
    """
    Projects EBITDA impact from AI score improvements.

    Parameters (v2.0 model):
      GAMMA_0 = 0.0025
      GAMMA_1 = 0.05

    Formulas:
      delta = exit_score - entry_score
      base_pct = GAMMA_0 * (delta ** 2) + GAMMA_1 * delta
      conservative_pct = base_pct * 0.7
      optimistic_pct = base_pct * 1.4
      risk_adjusted_pct = base_pct * (1 - 0.15 * max(0, 0.25 - h_r_score/100))
      requires_approval = base_pct > 5.0

    Round all pct values to 4 decimal places.
    Round delta_air to 2 decimal places.
    """

    GAMMA_0 = 0.0025
    GAMMA_1 = 0.05

    def project(
        self,
        company_id: str,
        entry_score: float,
        exit_score: float,
        h_r_score: float,
    ) -> EBITDAProjection:
        if not (0 <= float(entry_score) <= 100):
            raise ValueError(f"entry_score must be in [0, 100], got {entry_score}")
        if not (0 <= float(exit_score) <= 100):
            raise ValueError(f"exit_score must be in [0, 100], got {exit_score}")
        if not (0 <= float(h_r_score) <= 100):
            raise ValueError(f"h_r_score must be in [0, 100], got {h_r_score}")

        delta = float(exit_score) - float(entry_score)

        base_pct = (self.GAMMA_0 * (delta**2)) + (self.GAMMA_1 * delta)
        conservative_pct = base_pct * 0.7
        optimistic_pct = base_pct * 1.4

        risk_factor = 1 - 0.15 * max(0.0, 0.25 - float(h_r_score) / 100.0)
        risk_adjusted_pct = base_pct * risk_factor

        requires_approval = base_pct > 5.0

        return EBITDAProjection(
            company_id=company_id,
            entry_score=float(entry_score),
            exit_score=float(exit_score),
            delta_air=round(delta, 2),
            conservative_pct=round(conservative_pct, 4),
            base_pct=round(base_pct, 4),
            optimistic_pct=round(optimistic_pct, 4),
            risk_adjusted_pct=round(risk_adjusted_pct, 4),
            requires_approval=requires_approval,
        )


ebitda_calculator = EBITDACalculator()

