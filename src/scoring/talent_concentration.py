#This file shows how dependent a company's AI team is on a small number of key people
# High Talent Concentration = Key-person risk
# Low Talent Concentration = Distributed talent, safer organization
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Set


@dataclass
class JobAnalysis:
    """Analysis of job postings for talent concentration."""
    total_ai_jobs: int
    senior_ai_jobs: int          
    mid_ai_jobs: int            
    entry_ai_jobs: int           
    unique_skills: Set[str]   


class TalentConcentrationCalculator:
    """
    Calculate talent concentration (key-person risk).
    Bounded to [0, 1]
    """

    def calculate_tc(
        self,
        job_analysis: JobAnalysis,
        glassdoor_individual_mentions: int = 0,
        glassdoor_review_count: int = 1,
    ) -> Decimal:
        """
        Calculate talent concentration ratio.
        """
        # Calculate leadership ratio
        # Higher ratio → higher talent concentration → more risk
        if job_analysis.total_ai_jobs > 0:
            leadership_ratio = job_analysis.senior_ai_jobs / job_analysis.total_ai_jobs
        else:
            leadership_ratio = 0.5  # Default if no data

        # Calculate team size factor
        #Small team → high TC
        #Large team → low TC
        team_size_factor = min(1.0, 1.0 / (job_analysis.total_ai_jobs ** 0.5 + 0.1))

        # Calculate skill concentration
        # More diverse skills → lower concentration → safer
        skill_concentration = max(0, 1 - (len(job_analysis.unique_skills) / 15))

        # Calculate individual mention factor
        # If reviews focus on individuals → higher concentration risk.
        if glassdoor_review_count > 0:
            individual_factor = min(1.0, glassdoor_individual_mentions / glassdoor_review_count)
        else:
            individual_factor = 0.5

        # Weighted combination
        tc = (0.4 * leadership_ratio +
              0.3 * team_size_factor +
              0.2 * skill_concentration +
              0.1 * individual_factor)

        # Bound to [0, 1]
        return Decimal(str(max(0, min(1, tc)))).quantize(Decimal("0.0001"))

    def analyze_job_postings(
        self,
        postings: List[dict],    
    ) -> JobAnalysis:
        """
        Categorize job postings by level.
        """
        senior_keywords = {"principal", "staff", "director", "vp", "vice president", "head", "chief"}
        mid_keywords = {"senior", "lead", "manager"}
        entry_keywords = {"junior", "associate", "entry", "intern", "new grad", "graduate"}

        senior_count = 0
        mid_count = 0
        entry_count = 0
        all_skills: Set[str] = set()

        for posting in postings:
            title = (posting.get("title") or "").lower()

            # Classify seniority
            if any(kw in title for kw in senior_keywords):
                senior_count += 1
            elif any(kw in title for kw in mid_keywords):
                mid_count += 1
            elif any(kw in title for kw in entry_keywords):
                entry_count += 1
            else:
                mid_count += 1  # Default to mid

            # Collect skills from metadata
            meta = posting.get("metadata_json") or posting.get("METADATA_JSON") or {}
            if isinstance(meta, str):
                import json
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            skills = meta.get("skills", [])
            all_skills.update(skills)

        return JobAnalysis(
            total_ai_jobs=len(postings),
            senior_ai_jobs=senior_count,
            mid_ai_jobs=mid_count,
            entry_ai_jobs=entry_count,
            unique_skills=all_skills,
        )
