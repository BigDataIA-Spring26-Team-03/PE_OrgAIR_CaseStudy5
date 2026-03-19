from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from statistics import mean
from typing import Dict, List, Optional, Set

from jobspy import scrape_jobs

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


class SkillCategory(str, Enum):
    ML_ENGINEERING     = "ml_engineering"
    DATA_SCIENCE       = "data_science"
    AI_INFRASTRUCTURE  = "ai_infrastructure"
    AI_PRODUCT         = "ai_product"
    AI_STRATEGY        = "ai_strategy"
    HARDWARE_AI        = "hardware_ai"      


AI_SKILLS: Dict[SkillCategory, Set[str]] = {
    SkillCategory.ML_ENGINEERING: {
        "pytorch", "tensorflow", "keras", "mlops", "deep learning",
        "transformers", "llm", "fine-tuning", "model training",
        "tensorrt", "cuda", "deep learning algorithm", "inference engine",
        "neural reconstruction", "generative ai", "model inference",
        "machine learning framework", "triton", "cutlass",
        "large language model", "foundation model", "diffusion",
        "deep learning inference", "model evaluation", "training framework",
        "ai systems", "performance engineering",
    },
    SkillCategory.DATA_SCIENCE: {
        "data science", "statistics", "feature engineering",
        "scikit-learn", "sklearn", "xgboost", "lightgbm",
        "numpy", "pandas", "data scientist", "quantitative",
        "predictive modeling", "experimentation platform",
    },
    SkillCategory.AI_INFRASTRUCTURE: {
        "aws", "azure", "gcp", "docker", "kubernetes",
        "snowflake", "databricks", "spark", "airflow",
        "vector database", "faiss", "pinecone", "weaviate",
        "hpc", "high performance computing", "gpu cluster",
        "distributed training", "model serving", "mlflow",
        "kubeflow", "ray", "dask",
    },
    SkillCategory.AI_PRODUCT: {
        "prompt engineering", "rag", "retrieval augmented",
        "product analytics", "experimentation", "a/b testing",
        "recommendation", "personalization", "conversational ai",
        "generative", "agentic", "autonomous agent",
    },
    SkillCategory.AI_STRATEGY: {
        "ai strategy", "responsible ai", "model risk",
        "enterprise ai", "ai governance", "ai roadmap",
        "ai platform", "ai enablement", "ai benchmarking",
    },
    SkillCategory.HARDWARE_AI: {
        "asic", "soc", "rtl", "vlsi", "physical design",
        "verification engineer", "deep learning hardware",
        "neural network accelerator", "ai chip", "robotics",
        "autonomous vehicles", "embodied agent", "site reliability",
        "network ai", "ai for science", "hypervisor", "rtos",
        "nondestructive evaluation", 
        "digital twin", "industrial ai", "iot", "edge ai",
        "deep learning algorithm", "deep learning inference", "deep learning engineer",
        "ai systems", "ml framework", "performance engineer", "ai developer",
        "ai research", "generalist embodied", "ai for science", "ai benchmarking",
        "network ai platform", "ai enabling",
    },
}

# Title keywords that strongly indicate an AI/ML role
AI_TITLE_KEYWORDS = {
    "ai", "machine learning", "deep learning", "mlops",
    "artificial intelligence", "data scientist", "data science",
    "genai", "gen ai", "llm", "neural", "autonomous", "robotics",
    "tensorrt", "cuda", "generative", "hpc", "conversational ai",
    "ml engineer", "ai engineer", "applied ai", "agentic",
    "ai/ml", "ml/ai", "nlp", "computer vision", "reinforcement",
    "recommendation", "personalization", "analytics engineer",
}

# Titles that are generic IT — score capped even if some skills match
GENERIC_IT_TITLES = {
    "solution architect", "software architect", "enterprise architect",
    "lead software engineer", "senior software engineer",
    "staff software engineer", "principal software engineer",
    "it manager", "infrastructure engineer", "devops engineer",
    "security engineer", "network engineer", "systems engineer",
    "business analyst", "project manager", "program manager",
    "scrum master", "product owner",
}

SENIORITY_KEYWORDS = {
    "intern":   ["intern", "internship", "co-op", "coop"],
    "junior":   ["junior", "entry", "associate", "new grad", "graduate"],
    "mid":      ["engineer", "analyst", "developer", "scientist"],
    "senior":   ["senior", "sr", "lead", "principal", "staff"],
    "manager":  ["manager", "head", "director", "vp", "chief"],
}


@dataclass(frozen=True)
class JobPosting:
    title: str
    description: str
    company: str
    url: Optional[str] = None
    posted_date: Optional[str] = None


def classify_seniority(title: str) -> str:
    t = (title or "").lower()
    for level, kws in SENIORITY_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return level
    return "mid"


def extract_ai_skills(text: str) -> Set[str]:
    text_lower = (text or "").lower()
    found: Set[str] = set()
    for _, skills_set in AI_SKILLS.items():
        for skill in skills_set:
            if skill in text_lower:
                found.add(skill)
    return found


def _is_generic_it_title(title: str) -> bool:
    """Returns True if title is generic IT with no specific AI signal."""
    t = title.lower().strip()
    for generic in GENERIC_IT_TITLES:
        if generic in t:
            # Only generic if no AI qualifier also present
            if not any(ai_kw in t for ai_kw in AI_TITLE_KEYWORDS):
                return True
    return False


def calculate_ai_relevance_score(skills: Set[str], title: str) -> float:
    """
    Score 0..1 for AI relevance of a job posting.

    Scoring logic:
    - Base: skill breadth across ALL categories (including hardware_ai)
    - Title boost: strong AI/ML title keywords
    - Generic IT cap: plain IT roles without AI context capped at 0.25
    - Hardware AI boost: NVIDIA/GE-style hardware AI roles get recognition
    """
    title_lower = (title or "").lower()

    # Check for generic IT title with no AI qualifier → cap score
    if _is_generic_it_title(title):
        # Still score based on skills but cap at 0.25
        base = min(len(skills) / 10, 1.0) * 0.25
        return round(base, 3)

    # Count skills per category
    ml_hits       = sum(1 for s in skills if s in AI_SKILLS[SkillCategory.ML_ENGINEERING])
    ds_hits       = sum(1 for s in skills if s in AI_SKILLS[SkillCategory.DATA_SCIENCE])
    infra_hits    = sum(1 for s in skills if s in AI_SKILLS[SkillCategory.AI_INFRASTRUCTURE])
    product_hits  = sum(1 for s in skills if s in AI_SKILLS[SkillCategory.AI_PRODUCT])
    strategy_hits = sum(1 for s in skills if s in AI_SKILLS[SkillCategory.AI_STRATEGY])
    hardware_hits = sum(1 for s in skills if s in AI_SKILLS[SkillCategory.HARDWARE_AI])

    # Weighted skill score
    skill_score = (
        min(ml_hits,       6) * 0.10 +   # max 0.60
        min(ds_hits,       4) * 0.07 +   # max 0.28
        min(infra_hits,    4) * 0.05 +   # max 0.20
        min(product_hits,  3) * 0.06 +   # max 0.18
        min(strategy_hits, 2) * 0.05 +   # max 0.10
        min(hardware_hits, 6) * 0.12     # max 0.72 
    )

    # Title boost — explicit AI/ML title gets strong boost
    title_boost = 0.0
    ai_title_hits = sum(1 for kw in AI_TITLE_KEYWORDS if kw in title_lower)
    if ai_title_hits >= 2:
        title_boost = 0.40   # "AI/ML Engineer", "Machine Learning Data Scientist"
    elif ai_title_hits == 1:
        title_boost = 0.25   # "AI Engineer", "Data Scientist"

    # Hardware AI role boost — if title mentions hardware AI contexts
    hardware_title_keywords = {
        "deep learning", "neural", "autonomous", "robotics", "ai chip",
        "hpc", "tensorrt", "cuda", "embodied", "ai for science",
        "network ai", "ai benchmarking", "ai enabling",
    }
    if any(kw in title_lower for kw in hardware_title_keywords):
        title_boost = max(title_boost, 0.30)

    total = skill_score + title_boost
    return round(min(total, 1.0), 3)


def _signal_id(company_id: str, category: SignalCategory, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|{category.value}|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _norm_company(s: str) -> str:
    x = (s or "").lower().strip()
    x = re.sub(r"[^a-z0-9 ]+", " ", x)
    x = re.sub(
        r"\b(inc|incorporated|corp|corporation|llc|ltd|limited|co|company|plc)\b",
        " ", x,
    )
    x = re.sub(r"\s+", " ", x).strip()
    return x


def _squish(s: str) -> str:
    x = (s or "").lower()
    x = re.sub(r"[^a-z0-9]+", "", x)
    return x


def _is_ticker_like(a: str) -> bool:
    a = (a or "").strip()
    return bool(re.fullmatch(r"[A-Z]{1,5}", a)) and len(a) <= 2


def _clean_company_display_name(name: str) -> str:
    n = (name or "").strip()
    n = re.sub(
        r"\b(inc|inc\.|incorporated|corp|corporation|llc|ltd|limited|plc)\b\.?",
        "", n, flags=re.IGNORECASE,
    )
    n = re.sub(r"\s+", " ", n).strip(" ,")
    return n


def job_postings_to_signals(company_id: str, jobs: List[JobPosting]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for job in jobs:
        skills     = extract_ai_skills(job.description)
        seniority  = classify_seniority(job.title)
        relevance  = calculate_ai_relevance_score(skills, job.title)
        score_0_100 = int(round(relevance * 100))

        meta = {
            "company":      job.company,
            "seniority":    seniority,
            "skills":       sorted(list(skills)),
            "skill_count":  len(skills),
            "posted_date":  job.posted_date,
            "is_generic_it": _is_generic_it_title(job.title),
        }

        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, SignalCategory.TECHNOLOGY_HIRING, job.title, job.url),
                company_id=company_id,
                category=SignalCategory.TECHNOLOGY_HIRING,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=job.title,
                url=job.url,
                metadata_json=json.dumps(meta, default=str),
            )
        )

    return signals


def aggregate_job_signals(company_id: str, job_signals: List[ExternalSignal]) -> CompanySignalSummary:
    if not job_signals:
        jobs_score = 0
    else:
        jobs_score = int(round(mean(s.score for s in job_signals)))

    tech_score       = 0
    patents_score    = 0
    leadership_score = 0

    composite_score = int(round(
        0.30 * jobs_score +
        0.25 * patents_score +
        0.25 * tech_score +
        0.20 * leadership_score
    ))

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        leadership_score=leadership_score,
        composite_score=composite_score,
        last_updated_at=datetime.utcnow(),
    )


def scrape_job_postings(
    search_query: str,
    sources: list[str] = ["linkedin", "indeed", "glassdoor"],
    location: str = "United States",
    max_results_per_source: int = 25,
    hours_old: int = 24 * 30,
    target_company_name: Optional[str] = None,
    target_company_aliases: Optional[list[str]] = None,
) -> list[JobPosting]:
    """
    Scrape job postings using JobSpy and return JobPosting objects.
    Filters results to target company via alias matching.
    """
    aliases: list[str] = []
    if target_company_name:
        aliases.append(target_company_name)
        cleaned = _clean_company_display_name(target_company_name)
        if cleaned and cleaned.lower() != target_company_name.lower():
            aliases.append(cleaned)

    if target_company_aliases:
        aliases.extend([a for a in target_company_aliases if a])

    aliases = [a.strip() for a in aliases if a and a.strip()]
    alias_raws  = [a.lower() for a in aliases]
    alias_norms = [_norm_company(a) for a in aliases]

    def _scrape(effective_query: str):
        return scrape_jobs(
            site_name=sources,
            search_term=effective_query,
            location=location,
            results_wanted=max_results_per_source * len(sources) * 4,
            hours_old=hours_old,
            linkedin_fetch_description=True,
        )

    primary_query   = search_query
    secondary_query = None

    if aliases:
        preferred    = [a for a in aliases if not _is_ticker_like(a) and len(a) >= 5]
        best_human   = preferred[0] if preferred else aliases[0]
        primary_query = f'{search_query} "{best_human}"'

        brand_caps = [a for a in aliases if re.fullmatch(r"[A-Z]{3,5}", (a or "").strip())]
        if brand_caps:
            secondary_query = f'{search_query} "{brand_caps[0]}"'

    df = _scrape(primary_query)

    if (df is None or df.empty) and secondary_query:
        df = _scrape(secondary_query)

    if df is None or df.empty:
        return []

    if aliases and "company" in df.columns:
        def is_match(company_val: object) -> bool:
            c      = str(company_val or "")
            c_lower = c.lower()
            c_norm  = _norm_company(c)
            c_sq    = _squish(c)

            for a in alias_raws:
                if not a:
                    continue
                if len(a) <= 3:
                    if re.search(rf"\b{re.escape(a)}\b", c_lower):
                        return True
                    continue
                if " " not in a and a in c_lower:
                    return True

            for n in alias_norms:
                if n and n == c_norm:
                    return True

            for a in aliases:
                a_sq = _squish(a)
                if a_sq and a_sq == c_sq:
                    return True

            return False

        df = df[df["company"].apply(is_match)]
        if df.empty:
            return []

    jobs: list[JobPosting] = []
    for _, row in df.iterrows():
        jobs.append(JobPosting(
            title=str(row.get("title", "")),
            company=str(row.get("company", "Unknown")),
            description=str(row.get("description", "")),
            url=str(row.get("job_url", "")),
            posted_date=str(row.get("date_posted", "")),
        ))

    return jobs