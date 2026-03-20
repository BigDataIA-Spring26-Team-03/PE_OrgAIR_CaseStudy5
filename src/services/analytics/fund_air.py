from dataclasses import dataclass
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

SECTOR_BENCHMARKS = {
    "technology":         {"q1": 75, "q2": 65, "q3": 55, "q4": 45},
    "healthcare":         {"q1": 70, "q2": 58, "q3": 48, "q4": 38},
    "financial_services": {"q1": 72, "q2": 60, "q3": 50, "q4": 40},
    "financial":          {"q1": 72, "q2": 60, "q3": 50, "q4": 40},
    "manufacturing":      {"q1": 68, "q2": 55, "q3": 45, "q4": 35},
    "retail":             {"q1": 65, "q2": 52, "q3": 42, "q4": 32},
    "energy":             {"q1": 60, "q2": 48, "q3": 38, "q4": 28},
}


@dataclass
class FundMetrics:
    fund_id: str
    fund_air: float
    company_count: int
    quartile_distribution: Dict[int, int]
    sector_hhi: float
    avg_delta_since_entry: float
    total_ev_mm: float
    ai_leaders_count: int
    ai_laggards_count: int


class FundAIRCalculator:
    def calculate_fund_metrics(
        self,
        fund_id: str,
        companies: list,
        enterprise_values: Dict[str, float],
    ) -> FundMetrics:
        """
        companies is a list of PortfolioCompanyView objects.
        Each has: company_id, ticker, org_air, sector, delta_since_entry.

        enterprise_values maps company_id -> EV in $MM.
        If a company_id is not in the dict, default EV = 100.0.

        Steps:
        1. Raise ValueError("Cannot calculate Fund-AI-R for empty portfolio")
           if companies list is empty.
        2. Get EV for each company (default 100.0 if missing).
        3. total_ev = sum of all EVs.
        4. fund_air = sum(ev_i * company.org_air) / total_ev — round to 1dp.
        5. quartile_distribution = {1: 0, 2: 0, 3: 0, 4: 0}
           For each company call _get_quartile(company.org_air, company.sector)
           and increment the count.
        6. sector_hhi:
           Group EVs by company.sector, sum EV per sector.
           hhi = sum((sector_ev / total_ev) ** 2) — round to 4dp.
        7. avg_delta_since_entry = mean of all company.delta_since_entry — round to 1dp.
        8. ai_leaders_count = count of companies where org_air >= 70.
        9. ai_laggards_count = count of companies where org_air < 50.
        10. total_ev_mm = round(total_ev, 1).
        Return FundMetrics with all fields.
        """
        if not companies:
            raise ValueError("Cannot calculate Fund-AI-R for empty portfolio")

        # 2–3. Get EV per company, default 100.0; total_ev
        evs: List[float] = []
        for c in companies:
            ev = enterprise_values.get(c.company_id, 100.0)
            evs.append(float(ev))

        total_ev = sum(evs)
        if total_ev <= 0:
            raise ValueError("Total enterprise value must be positive")

        # 4. fund_air = EV-weighted average of org_air
        weighted_sum = sum(evs[i] * float(companies[i].org_air) for i in range(len(companies)))
        fund_air = round(weighted_sum / total_ev, 1)

        # 5. quartile_distribution
        quartile_distribution = {1: 0, 2: 0, 3: 0, 4: 0}
        for c in companies:
            q = self._get_quartile(float(c.org_air), str(c.sector or ""))
            quartile_distribution[q] = quartile_distribution.get(q, 0) + 1

        # 6. sector_hhi
        sector_ev: Dict[str, float] = {}
        for i, c in enumerate(companies):
            sec = str(c.sector or "").strip().lower() or "technology"
            sector_ev[sec] = sector_ev.get(sec, 0.0) + evs[i]
        hhi = sum((sector_ev[s] / total_ev) ** 2 for s in sector_ev)
        sector_hhi = round(hhi, 4)

        # 7. avg_delta_since_entry
        deltas = [float(getattr(c, "delta_since_entry", 0.0)) for c in companies]
        avg_delta_since_entry = round(sum(deltas) / len(deltas), 1)

        # 8–9. ai_leaders, ai_laggards
        ai_leaders_count = sum(1 for c in companies if float(c.org_air) >= 70)
        ai_laggards_count = sum(1 for c in companies if float(c.org_air) < 50)

        # 10. total_ev_mm
        total_ev_mm = round(total_ev, 1)

        return FundMetrics(
            fund_id=fund_id,
            fund_air=fund_air,
            company_count=len(companies),
            quartile_distribution=quartile_distribution,
            sector_hhi=sector_hhi,
            avg_delta_since_entry=avg_delta_since_entry,
            total_ev_mm=total_ev_mm,
            ai_leaders_count=ai_leaders_count,
            ai_laggards_count=ai_laggards_count,
        )

    def _get_quartile(self, score: float, sector: str) -> int:
        """
        Look up SECTOR_BENCHMARKS[sector].
        If sector not in SECTOR_BENCHMARKS, use "technology" as default.
        Return 1 if score >= q1 threshold.
        Return 2 if score >= q2 threshold.
        Return 3 if score >= q3 threshold.
        Return 4 otherwise.
        """
        sec = sector.strip().lower() if sector else "technology"
        benchmarks = SECTOR_BENCHMARKS.get(sec, SECTOR_BENCHMARKS["technology"])
        q1, q2, q3 = benchmarks["q1"], benchmarks["q2"], benchmarks["q3"]
        if score >= q1:
            return 1
        if score >= q2:
            return 2
        if score >= q3:
            return 3
        return 4


fund_air_calculator = FundAIRCalculator()
