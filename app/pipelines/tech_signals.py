from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from statistics import mean
from typing import List, Optional, Set, Dict, Tuple

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


# ─────────────────────────────────────────────────────────────────────────────
# Keyword dictionaries
# ─────────────────────────────────────────────────────────────────────────────

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

SCAN_PATHS = [
    "",
    "/about",
    "/technology",
    "/engineering",
    "/careers",
    "/platform",
    "/research",
    "/developers",
    "/innovation",
]

COMPANY_SCAN_OVERRIDES: Dict[str, List[str]] = {
    "NVDA": [
        "https://www.nvidia.com/en-us/ai/",
        "https://www.nvidia.com/en-us/deep-learning-ai/",
        "https://nvidianews.nvidia.com/bios",
        "https://developer.nvidia.com/deep-learning",
    ],
    "JPM":  [
        "https://www.jpmorgan.com/technology",
        "https://www.jpmorganchase.com/about/our-leadership",
        "https://www.jpmorgan.com/technology/technology-blog",
        "https://careers.jpmorgan.com/us/en/our-business/technology",
    ],
    "WMT":  ["", "/about", "/careers", "/technology", "/innovation"],
    "GE":   ["", "/about", "/digital", "/research", "/careers"],
    "DG":   ["", "/about", "/careers", "/technology"],
    "TSLA": ["", "/about", "/careers", "/energy", "/ai"],
    "MSFT": [
        "https://azure.microsoft.com/en-us/solutions/ai",
        "https://www.microsoft.com/en-us/ai",
        "https://engineering.microsoft.com",
        "https://news.microsoft.com/source/features/ai/",
        "https://www.microsoft.com/en-us/research/",
    ],
    "META": [
        "https://engineering.fb.com",
        "https://ai.meta.com",
        "https://about.meta.com/company-info/",
        "https://engineering.fb.com/category/ml-applications/",
    ],
    "AAPL": [
        "https://machinelearning.apple.com",
        "https://www.apple.com/artificial-intelligence/",
        "https://developer.apple.com/machine-learning/",
        "https://www.apple.com/careers/us/machine-learning-and-ai.html",
    ],
    "AMZN": [
        "https://www.amazon.science",
        "https://aws.amazon.com/machine-learning/",
        "https://aws.amazon.com/ai/",
        "https://www.amazon.jobs/en/landing_pages/ML",
    ],
    "GOOGL": [
        "https://ai.google",
        "https://research.google",
        "https://cloud.google.com/ai",
        "https://deepmind.google",
        "https://about.google/intl/en/products/",
    ],
    "GOOG": [
        "https://ai.google",
        "https://research.google",
        "https://cloud.google.com/ai",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

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
    if not mentions:
        return 0.0

    core_hits  = sum(1 for m in mentions if m in CORE_AI_TECH)
    data_hits  = sum(1 for m in mentions if m in DATA_PLATFORM_TECH)
    cloud_hits = sum(1 for m in mentions if m in CLOUD_AI_SERVICES)
    web_hits   = sum(1 for m in mentions if m in WEB_STACK_TECH)

    score = (
        min(core_hits,  8) * 0.10 +
        min(data_hits,  6) * 0.07 +
        min(cloud_hits, 4) * 0.08 +
        min(web_hits,   6) * 0.03
    )

    title_lower = _normalize(title)
    title_boost = 0.10 if any(
        k in title_lower for k in
        ["ai", "ml", "machine learning", "llm", "genai", "platform",
         "engineering", "research", "data", "cloud"]
    ) else 0.0

    return min(score + title_boost, 1.0)


def _ensure_url(domain_or_url: str) -> str:
    x = (domain_or_url or "").strip()
    if not x:
        return ""
    if x.startswith("http://") or x.startswith("https://"):
        return x
    return f"https://{x}"


# ─────────────────────────────────────────────────────────────────────────────
# Fetching — StealthyFetcher primary, requests fallback
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_page_content(url: str, timeout: int = 20) -> Tuple[str, bool]:
    """
    Fetch page content. Returns (text, is_plain_text).
    - is_plain_text=True  → StealthyFetcher returned plain text (no BeautifulSoup needed)
    - is_plain_text=False → plain requests returned raw HTML (needs BeautifulSoup)

    Tries StealthyFetcher first for JS-rendered sites, falls back to requests.
    """
    # Primary: StealthyFetcher — handles JS-rendered / anti-bot sites
    try:
        from scrapling.fetchers import StealthyFetcher
        page = StealthyFetcher.fetch(url, headless=True, network_idle=True, timeout=30000)
        text = page.get_all_text(ignore_tags=("script", "style", "nav", "footer"))
        if text and len(text) > 200:
            return text, True
    except Exception:
        pass

    # Fallback: plain requests — fast, works for non-JS sites
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.text or "", False
    except Exception:
        return "", False


def _extract_from_html(html: str) -> str:
    """Extract visible text + script srcs + meta content from raw HTML."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    visible = soup.get_text(" ", strip=True)
    visible = re.sub(r"\s+", " ", visible)

    srcs = " ".join(
        str(s.get("src", "")) for s in soup.find_all("script") if s.get("src")
    )
    meta = " ".join(
        tag.get("content", "") for tag in soup.find_all("meta") if tag.get("content")
    )
    return f"{visible} {srcs} {meta}"


# ─────────────────────────────────────────────────────────────────────────────
# Main collector
# ─────────────────────────────────────────────────────────────────────────────

def scrape_tech_signal_inputs(
    company: str,
    company_domain_or_url: str,
) -> List[TechSignalInput]:
    """
    Multi-page Digital Presence collector.
    Uses StealthyFetcher (JS-capable) with requests fallback per page.
    """
    base_url = _ensure_url(company_domain_or_url).rstrip("/")
    if not base_url:
        return []

    ticker = company.upper()
    paths = COMPANY_SCAN_OVERRIDES.get(ticker, SCAN_PATHS)

    results: List[TechSignalInput] = []
    homepage_added = False

    for path in paths:
        # If path is already an absolute URL use it directly,
        # otherwise append to base_url
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        else:
            url = f"{base_url}{path}"
        raw, is_plain = _fetch_page_content(url)

        if not raw:
            continue

        # If StealthyFetcher returned plain text use directly,
        # otherwise parse HTML with BeautifulSoup
        combined = raw if is_plain else _extract_from_html(raw)
        mentions = extract_tech_mentions(combined)

        page_label = path.strip("/") or "homepage"

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

    if not results:
        return [TechSignalInput(
            title="Digital presence scan failed",
            description=f"No pages accessible at {base_url}",
            company=company,
            url=base_url,
            observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
        )]

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Convert + Aggregate
# ─────────────────────────────────────────────────────────────────────────────

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
        scores = [s.score for s in tech_signals]
        top_scores = sorted(scores, reverse=True)[:3]
        tech_score = int(round(mean(top_scores)))

    composite_score = int(round(0.25 * tech_score))

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=0,
        tech_score=tech_score,
        patents_score=0,
        leadership_score=0,
        composite_score=composite_score,
        last_updated_at=datetime.utcnow(),
    )