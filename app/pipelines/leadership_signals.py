from __future__ import annotations

import json
import re
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import List, Optional

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS + SCORING TABLES
# ============================================================================

class AIBackgroundType(str, Enum):
    CHIEF_AI_OFFICER    = "CHIEF_AI_OFFICER"
    AI_COMPANY_VETERAN  = "AI_COMPANY_VETERAN"
    PHD_AI_ML           = "PHD_AI_ML"
    ML_PUBLICATIONS     = "ML_PUBLICATIONS"
    AI_PATENTS          = "AI_PATENTS"
    AI_BOARD_MEMBER     = "AI_BOARD_MEMBER"
    AI_CERTIFICATION    = "AI_CERTIFICATION"
    AI_KEYWORDS_ONLY    = "AI_KEYWORDS_ONLY"


AI_INDICATOR_SCORES: dict[AIBackgroundType, float] = {
    AIBackgroundType.CHIEF_AI_OFFICER:   1.0,
    AIBackgroundType.AI_COMPANY_VETERAN: 0.9,
    AIBackgroundType.PHD_AI_ML:          0.8,
    AIBackgroundType.ML_PUBLICATIONS:    0.7,
    AIBackgroundType.AI_PATENTS:         0.6,
    AIBackgroundType.AI_BOARD_MEMBER:    0.5,
    AIBackgroundType.AI_CERTIFICATION:   0.3,
    AIBackgroundType.AI_KEYWORDS_ONLY:   0.1,
}

ROLE_WEIGHTS: dict[str, float] = {
    "ceo": 1.0,
    "cto": 0.9,
    "cdo": 0.85,
    "cai": 1.0,
    "vp":  0.7,
}


# ============================================================================
# INDICATOR KEYWORD MAPS
# ============================================================================

_TITLE_INDICATOR_MAP: list[tuple[re.Pattern, AIBackgroundType]] = [
    (re.compile(r"chief\s+ai\s+officer|c\.?a\.?i\.?o", re.I),                             AIBackgroundType.CHIEF_AI_OFFICER),
    (re.compile(r"chief\s+(data|digital|analytics)\s+officer|c\.?d\.?[ao]", re.I),        AIBackgroundType.AI_COMPANY_VETERAN),
    (re.compile(r"chief\s+technology\s+officer|c\.?t\.?o", re.I),                         AIBackgroundType.AI_COMPANY_VETERAN),
    (re.compile(r"chief\s+(information|innovation)\s+officer", re.I),                     AIBackgroundType.AI_COMPANY_VETERAN),
    (re.compile(r"(vp|vice\s+president).{0,40}(ai|machine.learning|data.science)", re.I), AIBackgroundType.AI_BOARD_MEMBER),
    (re.compile(r"(director|head|lead).{0,40}(ai|machine.learning|data.science|analytics)", re.I), AIBackgroundType.AI_BOARD_MEMBER),
    (re.compile(r"\b(ai|ml|machine.learning|data.science|analytics)\b", re.I),            AIBackgroundType.AI_KEYWORDS_ONLY),
]

_AI_COMPANY_NAMES = re.compile(
    r"\b(nvidia|openai|deepmind|google brain|google ai|meta ai|microsoft ai|"
    r"anthropic|databricks|hugging face|palantir|c3\.ai|scale ai|cohere|"
    r"mistral|stability ai|inflection)\b",
    re.I,
)

_BIO_INDICATOR_MAP: list[tuple[re.Pattern, AIBackgroundType]] = [
    (re.compile(r"ph\.?\s*d.{0,80}(machine.learning|artificial.intelligence|deep.learning|nlp|computer.vision)", re.I), AIBackgroundType.PHD_AI_ML),
    (re.compile(r"(machine.learning|artificial.intelligence|deep.learning).{0,80}ph\.?\s*d", re.I),                     AIBackgroundType.PHD_AI_ML),
    (re.compile(r"(google|deepmind|openai|meta ai|microsoft research|amazon|nvidia).{0,60}(engineer|scientist|researcher|director)", re.I), AIBackgroundType.AI_COMPANY_VETERAN),
    (re.compile(r"published.{0,60}(machine.learning|ai|nlp|neural)", re.I),               AIBackgroundType.ML_PUBLICATIONS),
    (re.compile(r"(patent|inventor).{0,60}(ai|machine.learning|algorithm)", re.I),        AIBackgroundType.AI_PATENTS),
    (re.compile(r"(certified|certification).{0,60}(ai|machine.learning|data.science)", re.I), AIBackgroundType.AI_CERTIFICATION),
    (re.compile(r"\b(machine.learning|deep.learning|artificial.intelligence|neural.network|nlp|generative.ai)\b", re.I), AIBackgroundType.AI_KEYWORDS_ONLY),
]

# Job posting title patterns
_AI_LEADERSHIP_JOB_PATTERNS = re.compile(
    r"(director|vp|vice.president|head|lead|principal|chief).{0,40}"
    r"(ai|machine.learning|data.science|analytics|artificial.intelligence|ml)",
    re.I,
)

# Noise words to filter out from name candidate lines
_NOISE_NAMES = {
    "read more", "learn more", "investor relations", "ceo letters",
    "view all", "see more", "our team", "meet the team", "back to top",
    "skip to", "contact us", "about us", "press releases",
    "annual report", "quarterly earnings", "investor day",
    "diversity, opportunity & inclusion", "diversity equity inclusion",
    "corporate responsibility", "sustainability", "news & insights",
    "media contacts", "all contacts", "newsroom updates",
}

# Leadership page URL path candidates
_LEADERSHIP_PATHS = [
    "/en-us/about/leadership",
    "/en-us/leadership",
    "/en-us/company/leadership",
    "/en/about/leadership",
    "/en/leadership",
    "/about/leadership",
    "/about-us/leadership",
    "/leadership",
    "/about/team",
    "/about/management",
    "/company/leadership",
    "/company/management",
    "/executive-team",
    "/management-team",
    "/governance/management-team",
    "/about/executives",
    "/about",
    "/about-us",
    "/company/about",
    "/company",
]

# Ticker-specific overrides
_TICKER_LEADERSHIP_OVERRIDES: dict[str, list[str]] = {
    "NVDA": [
        "https://nvidianews.nvidia.com/bios",
        "https://www.nvidia.com/en-us/about-nvidia/governance/management-team/",
    ],
    "MSFT": [
        "https://www.microsoft.com/en-us/about/leadership",
    ],
    "GOOGL": [
        "https://about.google/intl/en/our-story/leadership/",
    ],
    "GOOG": [
        "https://about.google/intl/en/our-story/leadership/",
    ],
    "META": [
        "https://about.meta.com/company-info/",
    ],
    "AMZN": [
        "https://www.aboutamazon.com/about-us/leadership",
    ],
    "JPM": [
        "https://www.jpmorganchase.com/about/our-leadership",
        "https://www.jpmorgan.com/about-us/our-leadership",
    ],
    "CAT": [
        "https://www.caterpillar.com/en/company/governance/executive-officers.html",
        "https://www.caterpillar.com/en/company/leadership.html",
        "https://www.caterpillar.com/en/company/governance.html",
        "https://investor.caterpillar.com/governance/executive-officers/default.aspx",
    ],
    "WMT": [
        "https://corporate.walmart.com/about/leadership",
    ],
    "GE": [
        "https://www.ge.com/about-us/leadership",
    ],
    "GS": [
        "https://www.goldmansachs.com/our-firm/leadership",
    ],
}

# Career page path candidates
_CAREERS_PATHS = [
    "/careers",
    "/jobs",
    "/about/careers",
    "/company/careers",
    "/work-with-us",
    "/join-us",
]


# ============================================================================
# DATA MODEL
# ============================================================================

@dataclass(frozen=True)
class LeadershipProfile:
    name: str
    title: str
    company: str
    ai_indicators: List[AIBackgroundType]
    url: Optional[str] = None
    observed_date: Optional[str] = None


# ============================================================================
# INTERNAL HELPERS
# ============================================================================

def _normalize(s: str) -> str:
    return (s or "").strip().lower()


def _role_weight(title: str) -> float:
    t = _normalize(title)
    if "chief ai officer" in t or "cai" in t:
        return ROLE_WEIGHTS["cai"]
    if "ceo" in t or "chief executive" in t or "founder" in t:
        return ROLE_WEIGHTS["ceo"]
    if t.startswith("cto") or "chief technology" in t:
        return ROLE_WEIGHTS["cto"]
    if t.startswith("cdo") or "chief data" in t:
        return ROLE_WEIGHTS["cdo"]
    if t.startswith("vp") or "vice president" in t:
        return ROLE_WEIGHTS["vp"]
    return 0.5


def _max_indicator_score(indicators: List[AIBackgroundType]) -> float:
    if not indicators:
        return 0.0
    return max(AI_INDICATOR_SCORES.get(i, 0.0) for i in indicators)


def _indicators_from_title(title: str) -> List[AIBackgroundType]:
    found = []
    for pattern, indicator in _TITLE_INDICATOR_MAP:
        if pattern.search(title):
            found.append(indicator)
            break
    return found


def _indicators_from_bio(bio: str) -> List[AIBackgroundType]:
    found = []
    seen: set[AIBackgroundType] = set()
    for pattern, indicator in _BIO_INDICATOR_MAP:
        if pattern.search(bio) and indicator not in seen:
            found.append(indicator)
            seen.add(indicator)
    if AIBackgroundType.AI_COMPANY_VETERAN not in seen and _AI_COMPANY_NAMES.search(bio):
        found.append(AIBackgroundType.AI_COMPANY_VETERAN)
    return found


def _indicators_from_company(company: str) -> List[AIBackgroundType]:
    """Baseline indicator derived from the company name itself."""
    if _AI_COMPANY_NAMES.search(company):
        return [AIBackgroundType.AI_COMPANY_VETERAN]
    return [AIBackgroundType.AI_KEYWORDS_ONLY]


def _signal_id(company_id: str, name: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|leadership|exec|{name}|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


# ============================================================================
# SCRAPING — Wikipedia REST API
# ============================================================================

def _scrape_wikipedia_bio(exec_name: str) -> str:
    """
    Fetch executive bio using Wikipedia REST API (returns clean summary text).
    Falls back to empty string on failure.
    """
    try:
        import requests
        name_slug = exec_name.strip().replace(" ", "_")
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{name_slug}"
        r = requests.get(url, headers={"User-Agent": "PE-OrgAIR research@example.com"}, timeout=10)
        if r.status_code != 200:
            return ""
        data = r.json()
        return data.get("extract", "")
    except Exception as e:
        logger.debug("Wikipedia API failed for %s: %s", exec_name, e)
        return ""


# ============================================================================
# SCRAPING — Company leadership page
# ============================================================================

def _fetch_page_text(url: str) -> Optional[str]:
    """
    Fetch a URL with StealthyFetcher (bypasses Cloudflare / anti-bot).
    Returns full page text or None on failure.
    """
    try:
        from scrapling.fetchers import StealthyFetcher
        page = StealthyFetcher.fetch(url, headless=True, network_idle=True, timeout=30000)
        return page.get_all_text(ignore_tags=("script", "style", "nav", "footer"))
    except Exception as e:
        logger.debug("StealthyFetcher failed for %s: %s", url, e)
        return None


def _find_leadership_page(base_url: str, ticker: str = "") -> Optional[tuple[str, str]]:
    """
    Try known leadership URL paths. Returns (url, page_text) for the first
    that loads and contains leadership-like content, else None.
    Checks ticker-specific overrides first, then generic paths on base_url.
    """
    candidates: list[str] = []

    if ticker and ticker.upper() in _TICKER_LEADERSHIP_OVERRIDES:
        candidates.extend(_TICKER_LEADERSHIP_OVERRIDES[ticker.upper()])

    for path in _LEADERSHIP_PATHS:
        candidates.append(base_url.rstrip("/") + path)

    for url in candidates:
        text = _fetch_page_text(url)
        if text and len(text) > 300:
            tl = text.lower()
            if any(kw in tl for kw in ("chief", "president", "officer", "vice", "director", "vp", "founder")):
                logger.info("Found leadership page: %s", url)
                return url, text

    return None


def _parse_executives_from_text(page_text: str, company: str, page_url: str) -> List[LeadershipProfile]:
    """
    Heuristic parser: finds Name + Title pairs from a leadership page.
    Looks for C-suite / VP / Director title keywords, then finds the
    adjacent line that is a person's name.
    """
    profiles: List[LeadershipProfile] = []
    seen_names: set[str] = set()

    title_pattern = re.compile(
        r"\b(chief|president|vice.president|vp|ceo|cto|cdo|cai|coo|cfo|"
        r"director|head\s+of|svp|evp|managing.director|founder)\b",
        re.I,
    )

    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]

    for i, line in enumerate(lines):
        if not title_pattern.search(line):
            continue

        # Strip trailing "since YYYY" or date noise from title
        title = re.sub(r"\s*(since|,)?\s*\d{4}.*$", "", line).strip() or line
        name = None

        for offset in (-1, -2, 1, 2):
            idx = i + offset
            if 0 <= idx < len(lines):
                candidate = lines[idx].strip()
                if (
                    2 <= len(candidate.split()) <= 5
                    and not re.search(r"\d", candidate)
                    and not title_pattern.search(candidate)
                    and len(candidate) < 60
                    and _normalize(candidate) not in _NOISE_NAMES
                    and not candidate.lower().startswith(("read ", "learn ", "view ", "see ", "skip ", "back ", "annual ", "quarterly "))
                ):
                    name = candidate
                    break

        if not name:
            continue

        name_key = _normalize(name)
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        indicators: List[AIBackgroundType] = []
        indicators.extend(_indicators_from_title(title))

        wiki_bio = _scrape_wikipedia_bio(name)
        if wiki_bio:
            indicators.extend(_indicators_from_bio(wiki_bio))

        # Fallback: use company-level signal if no indicators found
        if not indicators:
            indicators.extend(_indicators_from_company(company))

        indicators = list(dict.fromkeys(indicators))

        profiles.append(
            LeadershipProfile(
                name=name,
                title=title,
                company=company,
                ai_indicators=indicators,
                url=page_url,
                observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
            )
        )

    logger.info("Parsed %d executive profiles from %s", len(profiles), page_url)
    return profiles


# ============================================================================
# SCRAPING — Careers page (AI leadership hiring signals)
# ============================================================================

def _scrape_ai_leadership_jobs(base_url: str, company: str) -> List[LeadershipProfile]:
    """
    Scan the careers page for Director/VP/Head-level AI job postings.
    Each matching posting is treated as an AI_BOARD_MEMBER-level signal.
    """
    profiles: List[LeadershipProfile] = []
    seen: set[str] = set()

    for path in _CAREERS_PATHS:
        url = base_url.rstrip("/") + path
        text = _fetch_page_text(url)
        if not text:
            continue

        for line in text.splitlines():
            line = line.strip()
            if not line or len(line) > 120:
                continue
            if _AI_LEADERSHIP_JOB_PATTERNS.search(line):
                key = _normalize(line)
                if key in seen:
                    continue
                seen.add(key)
                profiles.append(
                    LeadershipProfile(
                        name=f"[Open Role] {line[:80]}",
                        title=line[:80],
                        company=company,
                        ai_indicators=[AIBackgroundType.AI_BOARD_MEMBER],
                        url=url,
                        observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
                    )
                )

        if profiles:
            logger.info("Found %d AI leadership job signals at %s", len(profiles), url)
            break

    return profiles


# ============================================================================
# PUBLIC SCRAPER
# ============================================================================

def scrape_leadership_profiles(company: str, base_url: str, ticker: str = "") -> List[LeadershipProfile]:
    """
    Real scraper. Two data sources merged:
      1. Company leadership/about page  → executive name + title + Wikipedia bio
      2. Company careers page           → open AI Director/VP/Head roles

    ticker is optional but recommended — enables ticker-specific URL overrides.
    Falls back to empty list if scraping fails — never raises.
    """
    profiles: List[LeadershipProfile] = []

    # Source 1: leadership page
    try:
        result = _find_leadership_page(base_url, ticker=ticker)
        if result:
            page_url, page_text = result
            profiles.extend(_parse_executives_from_text(page_text, company, page_url))
    except Exception as e:
        logger.warning("Leadership page scrape failed for %s: %s", company, e)

    # Source 2: careers page (AI leadership hiring)
    try:
        job_profiles = _scrape_ai_leadership_jobs(base_url, company)
        profiles.extend(job_profiles)
    except Exception as e:
        logger.warning("Careers page scrape failed for %s: %s", company, e)

    logger.info(
        "scrape_leadership_profiles: %s → %d profiles (exec=%d, jobs=%d)",
        company,
        len(profiles),
        sum(1 for p in profiles if not p.name.startswith("[Open Role]")),
        sum(1 for p in profiles if p.name.startswith("[Open Role]")),
    )
    return profiles


def scrape_leadership_profiles_mock(company: str) -> List[LeadershipProfile]:
    """Kept for backwards compatibility — always returns []."""
    logger.warning(
        "scrape_leadership_profiles_mock called without base_url — returning []. "
        "Use scrape_leadership_profiles(company, base_url, ticker) instead."
    )
    return []


# ============================================================================
# SCORING
# ============================================================================

def calculate_leadership_score_0_1(executives: List[LeadershipProfile]) -> float:
    """
    Weighted average of AI indicators across leadership team:
        sum(role_weight * max_ai_indicator) / sum(role_weight)
    """
    if not executives:
        return 0.0
    weighted_sum = 0.0
    weight_sum = 0.0
    for e in executives:
        w = _role_weight(e.title)
        weight_sum += w
        weighted_sum += w * _max_indicator_score(e.ai_indicators)
    return min(weighted_sum / (weight_sum or 1.0), 1.0)


# ============================================================================
# SIGNAL BUILDERS
# ============================================================================

def leadership_profiles_to_signals(company_id: str, executives: List[LeadershipProfile]) -> List[ExternalSignal]:
    """Per-executive signals (drill-down rows)."""
    now = datetime.utcnow()
    signals: List[ExternalSignal] = []
    for e in executives:
        ai_score = _max_indicator_score(e.ai_indicators)
        role_w = _role_weight(e.title)
        score_0_100 = int(round(ai_score * 100))
        meta = {
            "company": e.company,
            "executive_name": e.name,
            "executive_title": e.title,
            "ai_indicators": [i.value for i in e.ai_indicators],
            "max_indicator_score": ai_score,
            "role_weight": role_w,
            "weighted_contribution": role_w * ai_score,
            "observed_date": e.observed_date,
            "calculation": "individual_score = max_ai_indicator; aggregation uses role_weight",
        }
        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, e.name, e.title, e.url),
                company_id=company_id,
                category=SignalCategory.LEADERSHIP_SIGNALS,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=f"{e.name} — {e.title}",
                url=e.url,
                metadata_json=json.dumps(meta, default=str),
            )
        )
    return signals


def aggregate_leadership_signals(company_id: str, leadership_signals: List[ExternalSignal]) -> CompanySignalSummary:
    """Produce CompanySignalSummary.leadership_score from per-exec ExternalSignals."""
    if not leadership_signals:
        leadership_score = 0
    else:
        executives: List[LeadershipProfile] = []
        for s in leadership_signals:
            try:
                meta = json.loads(s.metadata_json or "{}")
                indicators = [AIBackgroundType(x) for x in meta.get("ai_indicators", [])]
                executives.append(
                    LeadershipProfile(
                        name=meta.get("executive_name", s.title or "Unknown"),
                        title=meta.get("executive_title", ""),
                        company=meta.get("company", ""),
                        ai_indicators=indicators,
                        url=s.url,
                        observed_date=meta.get("observed_date"),
                    )
                )
            except Exception:
                continue
        score_0_1 = calculate_leadership_score_0_1(executives)
        leadership_score = int(round(score_0_1 * 100))

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=0,
        tech_score=0,
        patents_score=0,
        leadership_score=leadership_score,
        composite_score=0,
        last_updated_at=datetime.utcnow(),
    )


def leadership_profiles_to_aggregated_signal(
    company_id: str,
    executives: List[LeadershipProfile],
) -> ExternalSignal:
    """One aggregated leadership ExternalSignal (summary row)."""
    now = datetime.utcnow()

    if not executives:
        meta = {
            "executive_count": 0,
            "company": "",
            "executives": [],
            "aggregated_score": 0,
            "calculation_method": "weighted_avg(role_weight × max_ai_indicator) / sum(role_weight)",
            "calculation": "No executives analyzed",
        }
        payload = json.dumps(meta, sort_keys=True, default=str)
        signal_id = sha256(f"{company_id}|leadership|aggregated|{payload}".encode()).hexdigest()
        return ExternalSignal(
            id=signal_id,
            company_id=company_id,
            category=SignalCategory.LEADERSHIP_SIGNALS,
            source=SignalSource.external,
            signal_date=now,
            score=0,
            title="Leadership Team AI Expertise (0 executives)",
            url=None,
            metadata_json=json.dumps(meta, default=str),
        )

    score_0_1 = calculate_leadership_score_0_1(executives)
    score_0_100 = int(round(score_0_1 * 100))

    exec_details = []
    for e in executives:
        ai_score = _max_indicator_score(e.ai_indicators)
        role_w = _role_weight(e.title)
        exec_details.append({
            "name": e.name,
            "title": e.title,
            "ai_indicators": [i.value for i in e.ai_indicators],
            "max_indicator_score": round(ai_score, 3),
            "role_weight": round(role_w, 3),
            "weighted_contribution": round(role_w * ai_score, 3),
            "individual_score": int(round(ai_score * 100)),
            "observed_date": e.observed_date,
            "url": e.url,
        })

    exec_details_sorted = sorted(exec_details, key=lambda x: (x["name"].lower(), x["title"].lower()))
    meta = {
        "executive_count": len(executives),
        "company": executives[0].company if executives else "",
        "aggregated_score": score_0_100,
        "calculation_method": "weighted_avg(role_weight × max_ai_indicator) / sum(role_weight)",
        "executives": exec_details_sorted,
    }
    payload = json.dumps(meta, sort_keys=True, default=str)
    signal_id = sha256(f"{company_id}|leadership|aggregated|{payload}".encode()).hexdigest()

    return ExternalSignal(
        id=signal_id,
        company_id=company_id,
        category=SignalCategory.LEADERSHIP_SIGNALS,
        source=SignalSource.external,
        signal_date=now,
        score=score_0_100,
        title=f"Leadership Team AI Expertise ({len(executives)} executives)",
        url=None,
        metadata_json=json.dumps(meta, default=str),
    )