from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from app.models.signal import CompanySignalSummary, ExternalSignal
from app.pipelines.job_signals import (
    JobPosting,
    scrape_job_postings,
    job_postings_to_signals,
    aggregate_job_signals,
)
from app.pipelines.tech_signals import (
    TechSignalInput,
    tech_inputs_to_signals,
    aggregate_tech_signals,
)
from app.pipelines.patent_signals import (
    PatentSignalInput,
    patent_inputs_to_signals,
    aggregate_patent_signals,
)
from app.pipelines.leadership_signals import (
    LeadershipProfile,
    leadership_profiles_to_signals,
    aggregate_leadership_signals,
)


@dataclass(frozen=True)
class ExternalSignalsRunResult:
    company_id: str
    jobs_signals: List[ExternalSignal]
    tech_signals: List[ExternalSignal]
    patent_signals: List[ExternalSignal]
    leadership_signals: List[ExternalSignal]
    summary: CompanySignalSummary


def build_company_signal_summary(
    company_id: str,
    jobs_score: int,
    tech_score: int,
    patents_score: int,
    leadership_score: int,
) -> CompanySignalSummary:
    # weights: Hiring 0.30, Innovation 0.25, TechStack 0.25, Leadership 0.20
    composite_score = int(
        round(
            0.30 * jobs_score
            + 0.25 * patents_score
            + 0.25 * tech_score
            + 0.20 * leadership_score
        )
    )

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        leadership_score=leadership_score,
        composite_score=composite_score,
        last_updated_at=datetime.utcnow(),
    )


def run_external_signals_pipeline(
    company_id: str,
    jobs_search_query: str,
    jobs_sources: Optional[list[str]] = None,
    jobs_location: str = "Boston, MA",
    jobs_max_results_per_source: int = 5,
    jobs_target_company_name: Optional[str] = None,         # ✅ existing
    jobs_target_company_ticker: Optional[str] = None,       # ✅ existing
    jobs_target_company_aliases: Optional[List[str]] = None,# ✅ NEW
    tech_items: Optional[List[TechSignalInput]] = None,
    patent_items: Optional[List[PatentSignalInput]] = None,
    leadership_profiles: Optional[List[LeadershipProfile]] = None,
) -> ExternalSignalsRunResult:
    # -------------------
    # JOB SIGNALS (real scraping)
    # -------------------
    jobs_sources = jobs_sources or ["indeed", "google"]

    # Build alias list safely (combine explicit aliases + ticker if present)
    combined_aliases: Optional[List[str]] = None
    if jobs_target_company_aliases:
        combined_aliases = list(jobs_target_company_aliases)
    if jobs_target_company_ticker:
        combined_aliases = (combined_aliases or []) + [jobs_target_company_ticker]

    jobs: List[JobPosting] = scrape_job_postings(
        jobs_search_query,
        sources=jobs_sources,
        location=jobs_location,
        max_results_per_source=jobs_max_results_per_source,
        target_company_name=jobs_target_company_name,
        target_company_aliases=combined_aliases,
    )

    jobs_signals = job_postings_to_signals(company_id, jobs)
    jobs_summary = aggregate_job_signals(company_id, jobs_signals)
    jobs_score = jobs_summary.jobs_score

    # -------------------
    # TECH SIGNALS
    # -------------------
    tech_items = tech_items or []
    tech_signals = tech_inputs_to_signals(company_id, tech_items)
    tech_summary = aggregate_tech_signals(company_id, tech_signals)
    tech_score = tech_summary.tech_score

    # -------------------
    # PATENT SIGNALS
    # -------------------
    patent_items = patent_items or []
    patent_signals = patent_inputs_to_signals(company_id, patent_items)
    patent_summary = aggregate_patent_signals(company_id, patent_signals)
    patents_score = patent_summary.patents_score

    # -------------------
    # LEADERSHIP SIGNALS
    # -------------------
    leadership_profiles = leadership_profiles or []
    leadership_signals = leadership_profiles_to_signals(company_id, leadership_profiles)
    leadership_summary = aggregate_leadership_signals(company_id, leadership_signals)
    leadership_score = leadership_summary.leadership_score

    # -------------------
    # FINAL SUMMARY
    # -------------------
    final_summary = build_company_signal_summary(
        company_id,
        jobs_score,
        tech_score,
        patents_score,
        leadership_score,
    )

    return ExternalSignalsRunResult(
        company_id=company_id,
        jobs_signals=jobs_signals,
        tech_signals=tech_signals,
        patent_signals=patent_signals,
        leadership_signals=leadership_signals,
        summary=final_summary,
    )