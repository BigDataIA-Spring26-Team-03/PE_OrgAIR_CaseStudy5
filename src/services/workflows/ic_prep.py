# src/services/workflows/ic_prep.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from src.services.justification.generator import JustificationGenerator, ScoreJustification
from src.services.integration.cs1_client import CS1Client, Company
from src.services.integration.cs3_client import CS3Client, Dimension, CompanyAssessment


@dataclass
class ICMeetingPackage:
    """Complete IC meeting preparation package."""
    company: Company
    assessment: CompanyAssessment
    dimension_justifications: Dict[Dimension, ScoreJustification]

    executive_summary: str
    key_strengths: List[str]
    key_gaps: List[str]
    risk_factors: List[str]
    recommendation: str

    generated_at: str
    total_evidence_count: int
    avg_evidence_strength: str


class ICPrepWorkflow:
    """Prepare complete IC meeting evidence package."""

    def __init__(self) -> None:
        self.cs1 = CS1Client()
        self.cs3 = CS3Client()
        self.generator = JustificationGenerator()

    async def prepare_meeting(
        self,
        company_id: str,
        focus_dimensions: Optional[List[Dimension]] = None,
    ) -> ICMeetingPackage:
        """Generate complete IC meeting package."""
        if focus_dimensions is None:
            focus_dimensions = list(Dimension)

        # 1. Fetch company from CS1
        async with self.cs1 as cs1:
            company = await cs1.get_company(company_id)

        # 2. Fetch assessment from CS3
        async with self.cs3 as cs3:
            assessment = await cs3.get_assessment(company_id)

        # 3. Generate justifications for each dimension
        tasks = [
            self.generator.generate_justification(
                company_id=company_id, dimension=dim
            )
            for dim in focus_dimensions
        ]
        results = await asyncio.gather(*tasks)
        justifications: Dict[Dimension, ScoreJustification] = dict(
            zip(focus_dimensions, results)
        )

        # 4. Synthesise findings
        strengths = self._identify_strengths(assessment, justifications)
        gaps = self._identify_gaps(justifications)
        risks = self._assess_risks(assessment, justifications)

        # 5. Generate executive summary
        summary = self._generate_summary(company, assessment, justifications)

        # 6. Generate recommendation
        recommendation = self._generate_recommendation(assessment, strengths, gaps)

        # Calculate stats
        total_evidence = sum(len(j.supporting_evidence) for j in justifications.values())
        strength_scores = {"strong": 3, "moderate": 2, "weak": 1}
        avg_strength_num = sum(
            strength_scores[j.evidence_strength] for j in justifications.values()
        ) / len(justifications)
        avg_strength = (
            "strong" if avg_strength_num >= 2.5
            else "moderate" if avg_strength_num >= 1.5
            else "weak"
        )

        return ICMeetingPackage(
            company=company,
            assessment=assessment,
            dimension_justifications=justifications,
            executive_summary=summary,
            key_strengths=strengths,
            key_gaps=gaps,
            risk_factors=risks,
            recommendation=recommendation,
            generated_at=datetime.now().isoformat(),
            total_evidence_count=total_evidence,
            avg_evidence_strength=avg_strength,
        )

    def _identify_strengths(
        self,
        assessment: CompanyAssessment,
        justifications: Dict[Dimension, ScoreJustification],
    ) -> List[str]:
        """Identify top 3 strengths."""
        strengths_with_level = [
            (dim, j)
            for dim, j in justifications.items()
            if j.level >= 4 and j.evidence_strength in ["strong", "moderate"]
        ]
        strengths_with_level.sort(key=lambda x: -x[1].level)
        return [
            f"{dim.value.replace('_', ' ').title()}: Level {j.level} ({j.level_name}) "
            f"- {len(j.supporting_evidence)} evidence items"
            for dim, j in strengths_with_level[:3]
        ]

    def _identify_gaps(
        self,
        justifications: Dict[Dimension, ScoreJustification],
    ) -> List[str]:
        """Identify critical gaps across dimensions."""
        gaps = []
        for dim, j in justifications.items():
            if j.level <= 2:
                gaps.append(
                    f"{dim.value.replace('_', ' ').title()}: Level {j.level} - needs investment"
                )
            gaps.extend(j.gaps_identified[:2])
        return gaps[:5]

    def _assess_risks(
        self,
        assessment: CompanyAssessment,
        justifications: Dict[Dimension, ScoreJustification],
    ) -> List[str]:
        """Assess execution risks."""
        risks = []
        if assessment.talent_concentration > 0.25:
            risks.append(
                f"High talent concentration ({assessment.talent_concentration:.2f}) - key person risk"
            )
        weak_dims = [dim for dim, j in justifications.items() if j.evidence_strength == "weak"]
        if weak_dims:
            risks.append(f"Weak evidence for: {', '.join(d.value for d in weak_dims)}")
        if assessment.position_factor < 0:
            risks.append(
                f"Below-average sector position (factor={assessment.position_factor:.2f})"
            )
        return risks[:5]

    def _generate_summary(
        self,
        company: Company,
        assessment: CompanyAssessment,
        justifications: Dict[Dimension, ScoreJustification],
    ) -> str:
        """Generate executive summary paragraph."""
        strong_dims = [d for d, j in justifications.items() if j.level >= 4]
        weak_dims = [d for d, j in justifications.items() if j.level <= 2]

        return (
            f"{company.name} ({company.ticker}) scores {assessment.org_air_score:.0f} on Org-AI-R "
            f"({assessment.confidence_interval[0]:.0f}-{assessment.confidence_interval[1]:.0f} 95% CI). "
            f"V^R={assessment.vr_score:.0f}, H^R={assessment.hr_score:.0f}. "
            f"Strengths in {', '.join(d.value.replace('_', ' ') for d in strong_dims[:2])}. "
            + (f"Gaps in {', '.join(d.value.replace('_', ' ') for d in weak_dims[:2])}. " if weak_dims else "")
            + f"Position factor: {assessment.position_factor:+.2f} vs sector peers."
        )

    def _generate_recommendation(
        self,
        assessment: CompanyAssessment,
        strengths: List[str],
        gaps: List[str],
    ) -> str:
        """Generate investment recommendation."""
        if assessment.org_air_score >= 70 and len(strengths) >= 2:
            return "PROCEED - Strong AI readiness with solid evidence base"
        elif assessment.org_air_score >= 50:
            return "PROCEED WITH CAUTION - Moderate AI readiness, gaps addressable"
        else:
            return "FURTHER DILIGENCE - Significant AI capability gaps identified"