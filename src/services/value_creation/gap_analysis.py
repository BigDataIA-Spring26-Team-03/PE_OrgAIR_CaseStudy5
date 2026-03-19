from typing import Dict, Any, List

DIMENSION_INVESTMENT = {
    "data_infrastructure": 150000,
    "ai_governance": 80000,
    "technology_stack": 200000,
    "talent": 300000,
    "leadership": 50000,
    "use_case_portfolio": 120000,
    "culture": 60000,
}


class GapAnalyzer:
    """
    Analyzes dimension gaps and generates an improvement plan.

    analyze(company_id, current_scores, target_org_air) returns a dict:

    For each dimension where current_score < min(target_org_air + 5, 100):
      - gap = target - current (rounded 1dp)
      - investment_estimate = DIMENSION_INVESTMENT[dimension]
      - priority = "high" if gap > 20, "medium" if gap > 10, else "low"

    Sort gaps list by gap descending.

    Return:
      {
        "company_id": str,
        "target_org_air": float,
        "gaps": List[dict],           # sorted by gap desc
        "priority_ranking": List[str], # dimension names sorted by gap desc
        "total_investment_estimate": int,  # sum of all investment_estimates
        "projected_ebitda_pct": float,     # round(avg_gap * 0.05, 2)
      }
    """

    def analyze(
        self,
        company_id: str,
        current_scores: Dict[str, float],
        target_org_air: float,
    ) -> Dict[str, Any]:
        if not (0 <= float(target_org_air) <= 100):
            raise ValueError(
                f"target_org_air must be in [0, 100], got {target_org_air}"
            )

        target = min(float(target_org_air) + 5.0, 100.0)

        gaps: List[Dict[str, Any]] = []
        for dimension, investment in DIMENSION_INVESTMENT.items():
            current = float(current_scores.get(dimension, 0.0))
            if current < target:
                gap = round(target - current, 1)
                if gap > 20:
                    priority = "high"
                elif gap > 10:
                    priority = "medium"
                else:
                    priority = "low"

                gaps.append(
                    {
                        "dimension": dimension,
                        "current_score": current,
                        "target_score": target,
                        "gap": gap,
                        "investment_estimate": investment,
                        "priority": priority,
                    }
                )

        gaps.sort(key=lambda g: g["gap"], reverse=True)
        priority_ranking = [g["dimension"] for g in gaps]
        total_investment = int(sum(g["investment_estimate"] for g in gaps))
        avg_gap = (sum(g["gap"] for g in gaps) / len(gaps)) if gaps else 0.0
        projected_ebitda_pct = round(avg_gap * 0.05, 2)

        return {
            "company_id": company_id,
            "target_org_air": float(target_org_air),
            "gaps": gaps,
            "priority_ranking": priority_ranking,
            "total_investment_estimate": total_investment,
            "projected_ebitda_pct": projected_ebitda_pct,
        }


gap_analyzer = GapAnalyzer()

