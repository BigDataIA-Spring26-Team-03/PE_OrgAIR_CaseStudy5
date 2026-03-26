from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from statistics import mean
from typing import List, Optional, Set, Dict, Iterable

import requests
from bs4 import BeautifulSoup

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


@dataclass(frozen=True)
class TechSignalInput:
    """
    Represents a single tech-stack signal for a company.
    For Digital Presence we collect from a company's public website (domain).
    """
    title: str
    description: str
    company: str
    url: Optional[str] = None
    observed_date: Optional[str] = None


# -----------------------------
# Keyword dictionaries (expandable)
# -----------------------------
CORE_AI_TECH: Set[str] = {
    "openai", "chatgpt", "gpt", "llm", "transformers", "rag", "vector database",
    "pytorch", "tensorflow", "keras", "hugging face", "langchain", "llamaindex",
    "embedding", "embeddings", "genai", "generative ai", "large language model",
    "machine learning", "deep learning", "neural network", "natural language",
    "computer vision", "reinforcement learning", "mlops", "ai platform",
    "foundation model", "fine-tuning", "inference", "diffusion model",
}

DATA_PLATFORM_TECH: Set[str] = {
    "snowflake", "databricks", "spark", "airflow", "kafka", "dbt", "delta lake",
    "s3", "adls", "bigquery", "redshift", "data warehouse", "data lake",
    "data pipeline", "data platform", "real-time data", "stream processing",
    "elasticsearch", "pinecone", "weaviate", "chroma", "vector store",
}

CLOUD_AI_SERVICES: Set[str] = {
    "aws sagemaker", "bedrock", "azure openai", "azure ml", "vertex ai",
    "google cloud ai", "amazon comprehend", "aws rekognition", "azure cognitive",
    "google automl", "huggingface", "replicate", "modal", "together ai",
}

WEB_STACK_TECH: Set[str] = {
    "react", "next.js", "nextjs", "angular", "vue", "svelte",
    "node.js", "nodejs", "express", "django", "flask", "fastapi",
    "kubernetes", "docker", "terraform",
    "cloudflare", "akamai",
    "segment", "amplitude", "mixpanel",
    "google analytics", "gtag", "gtm", "tag manager",
    "stripe", "paypal",
}

# Pages to scan per company — subpages with highest tech signal density
# Ordered by expected signal richness
SCAN_PATHS = [
    "",                  # homepage
    "/about",
    "/technology",
    "/engineering",
    "/careers",
    "/platform",
    "/research",
    "/developers",
    "/innovation",
]

# Company-specific overrides for known high-signal pages
COMPANY_SCAN_OVERRIDES: Dict[str, List[str]] = {
    "NVDA": [
        "",
        "/en-us/about-nvidia/",
        "/en-us/research/",
        "/en-us/industries/",
        "/careers",
    ],
    "JPM": [
        "",
        "/technology",
        "/about-jpmorgan-chase",
        "/careers",
        "/technology/engineering",
    ],
    "WMT": [
        "",
        "/about",
        "/careers",
        "/technology",
        "/innovation",
    ],
    "GE": [
        "",
        "/about",
        "/digital",
        "/research",
        "/careers",
    ],
    "DG": [
        "",
        "/about",
        "/careers",
        "/technology",
    ],
    "TSLA": [
        "",
        "/about",
        "/careers",
        "/energy",
        "/ai",
        "/supercharger",
    ],
    "MSFT": [
        "",
        "/about",
        "/careers",
        "/en-us/research",
        "/azure",
    ],
    # Alphabet (GOOGL): yfinance returns abc.xyz — use google.com + AI-rich paths
    "GOOGL": [
        "",
        "/technology/",
        "/about/",
        "/search/howsearchworks/",
        "/cloud/",
        "/careers/",
    ],
    "GOOG": [
        "",
        "/technology/",
        "/about/",
        "/search/howsearchworks/",
        "/cloud/",
        "/careers/",
    ],
}


def _normalize(text: str) -> str:
    return (text or "").lower()


def _signal_id(company_id: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|tech|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def extract_tech_mentions(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()
    for kw in (CORE_AI_TECH | DATA_PLATFORM_TECH | CLOUD_AI_SERVICES | WEB_STACK_TECH):
        if kw in t:
            found.add(kw)
    return found


def calculate_tech_adoption_score(mentions: Set[str], title: str) -> float:
    """
    Score 0..1 based on number and type of tech mentions.
    Raised caps vs original to allow proper differentiation between companies.
    NVDA/JPM should score significantly higher than DG/GE.
    """
    if not mentions:
        return 0.0

    core_hits  = sum(1 for m in mentions if m in CORE_AI_TECH)
    data_hits  = sum(1 for m in mentions if m in DATA_PLATFORM_TECH)
    cloud_hits = sum(1 for m in mentions if m in CLOUD_AI_SERVICES)
    web_hits   = sum(1 for m in mentions if m in WEB_STACK_TECH)

    # Raised caps — allow genuine differentiation between tech-heavy vs light companies
    # Original caps (3,3,2,4) compressed everything into a narrow band
    score = (
        min(core_hits,  8) * 0.10 +   # max 0.80 — AI/ML is most important signal
        min(data_hits,  6) * 0.07 +   # max 0.42 — data platform maturity
        min(cloud_hits, 4) * 0.08 +   # max 0.32 — cloud AI services adoption
        min(web_hits,   6) * 0.03     # max 0.18 — web stack (weakest signal)
    )

    title_lower = _normalize(title)
    title_boost = 0.10 if any(
        k in title_lower for k in
        ["ai", "ml", "machine learning", "llm", "genai", "platform",
         "engineering", "research", "data", "cloud"]
    ) else 0.0

    return min(score + title_boost, 1.0)


# -----------------------------
# HTTP helpers
# -----------------------------
def _ensure_url(domain_or_url: str) -> str:
    x = (domain_or_url or "").strip()
    if not x:
        return ""
    if x.startswith("http://") or x.startswith("https://"):
        return x
    return f"https://{x}"


def _fetch_html(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text or ""


def _extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def _extract_script_srcs(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    srcs = []
    for s in soup.find_all("script"):
        src = s.get("src")
        if src:
            srcs.append(str(src))
    return srcs


def _extract_meta_content(html: str) -> str:
    """Extract meta description and keywords — often contain tech stack hints."""
    soup = BeautifulSoup(html, "html.parser")
    parts = []
    for tag in soup.find_all("meta"):
        content = tag.get("content", "")
        if content:
            parts.append(content)
    return " ".join(parts)


# -----------------------------
# REAL COLLECTION: multi-page scrape
# -----------------------------
def scrape_tech_signal_inputs(
    company: str,
    company_domain_or_url: str,
) -> List[TechSignalInput]:
    """
    Multi-page Digital Presence collector.

    Scans homepage + key subpages to detect tech stack evidence.
    Uses company-specific page overrides where available, falls back
    to generic high-signal paths. Each page that yields tech mentions
    becomes a separate TechSignalInput → separate ExternalSignal.

    This produces multiple signals per company (5-9), allowing the
    evidence_mapper confidence to scale with signal count, and giving
    a meaningful spread between NVDA (high AI tech) and DG (low AI tech).
    """
    base_url = _ensure_url(company_domain_or_url).rstrip("/")
    if not base_url:
        return []

    ticker = company.upper()
    paths = COMPANY_SCAN_OVERRIDES.get(ticker, SCAN_PATHS)

    results: List[TechSignalInput] = []
    homepage_added = False

    for path in paths:
        url = f"{base_url}{path}"
        try:
            html = _fetch_html(url)
        except Exception:
            # Skip pages that 404/block — don't fail entire scan
            continue

        visible_text  = _extract_visible_text(html)
        script_srcs   = " ".join(_extract_script_srcs(html))
        meta_content  = _extract_meta_content(html)
        combined      = f"{visible_text} {script_srcs} {meta_content}"
        mentions      = extract_tech_mentions(combined)

        page_label = path.strip("/") or "homepage"

        # Always keep homepage even if no AI mentions (proves site is live)
        if not mentions and path != "" and homepage_added:
            continue

        desc = (
            f"Scanned {url} for technology indicators. "
            f"Detected {len(mentions)} technologies."
        )
        if mentions:
            desc += f" Mentions: {', '.join(sorted(mentions))}."

        results.append(TechSignalInput(
            title=f"Digital presence scan: {page_label}",
            description=desc,
            company=company,
            url=url,
            observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
        ))

        if path == "":
            homepage_added = True

    # Fallback: if nothing returned at all, record a failed scan signal
    if not results:
        return [TechSignalInput(
            title="Digital presence scan failed",
            description=f"No pages accessible at {base_url}",
            company=company,
            url=base_url,
            observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
        )]

    return results


# -----------------------------
# Convert + Aggregate
# -----------------------------
def tech_inputs_to_signals(
    company_id: str,
    items: List[TechSignalInput],
) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for item in items:
        mentions = extract_tech_mentions(item.description)
        score_0_1   = calculate_tech_adoption_score(mentions, item.title)
        score_0_100 = int(round(score_0_1 * 100))

        meta = {
            "company": item.company,
            "mentions": sorted(list(mentions)),
            "mention_count": len(mentions),
            "observed_date": item.observed_date,
            "url": item.url,
        }

        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, item.title, item.url),
                company_id=company_id,
                category=SignalCategory.DIGITAL_PRESENCE,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=item.title,
                url=item.url,
                metadata_json=json.dumps(meta),
            )
        )

    return signals


def aggregate_tech_signals(
    company_id: str,
    tech_signals: List[ExternalSignal],
) -> CompanySignalSummary:
    if not tech_signals:
        tech_score = 0
    else:
        # Weight by score — higher-scoring pages contribute more to aggregate
        scores = [s.score for s in tech_signals]
        # Use top-3 average to avoid dilution from low-signal pages
        top_scores = sorted(scores, reverse=True)[:3]
        tech_score = int(round(mean(top_scores)))

    jobs_score       = 0
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