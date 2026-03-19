from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import List, Optional

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


class AIBackgroundType(str, Enum):
    CHIEF_AI_OFFICER = "CHIEF_AI_OFFICER"
    AI_COMPANY_VETERAN = "AI_COMPANY_VETERAN"
    PHD_AI_ML = "PHD_AI_ML"
    ML_PUBLICATIONS = "ML_PUBLICATIONS"
    AI_PATENTS = "AI_PATENTS"
    AI_BOARD_MEMBER = "AI_BOARD_MEMBER"
    AI_CERTIFICATION = "AI_CERTIFICATION"
    AI_KEYWORDS_ONLY = "AI_KEYWORDS_ONLY"


AI_INDICATOR_SCORES: dict[AIBackgroundType, float] = {
    AIBackgroundType.CHIEF_AI_OFFICER: 1.0,
    AIBackgroundType.AI_COMPANY_VETERAN: 0.9,
    AIBackgroundType.PHD_AI_ML: 0.8,
    AIBackgroundType.ML_PUBLICATIONS: 0.7,
    AIBackgroundType.AI_PATENTS: 0.6,
    AIBackgroundType.AI_BOARD_MEMBER: 0.5,
    AIBackgroundType.AI_CERTIFICATION: 0.3,
    AIBackgroundType.AI_KEYWORDS_ONLY: 0.1,
}


ROLE_WEIGHTS: dict[str, float] = {
    "ceo": 1.0,
    "cto": 0.9,
    "cdo": 0.85,
    "cai": 1.0,
    "vp": 0.7,
}


@dataclass(frozen=True)
class LeadershipProfile:
    name: str
    title: str
    company: str
    ai_indicators: List[AIBackgroundType]
    url: Optional[str] = None
    observed_date: Optional[str] = None


def _normalize(s: str) -> str:
    return (s or "").strip().lower()


def _role_weight(title: str) -> float:
    t = _normalize(title)

    if "chief ai officer" in t or "cai" in t:
        return ROLE_WEIGHTS["cai"]
    if t.startswith("ceo") or "chief executive" in t:
        return ROLE_WEIGHTS["ceo"]
    if t.startswith("cto") or "chief technology" in t:
        return ROLE_WEIGHTS["cto"]
    if t.startswith("cdo") or "chief data" in t:
        return ROLE_WEIGHTS["cdo"]
    if t.startswith("vp") or "vice president" in t:
        return ROLE_WEIGHTS["vp"]

    # default for other leadership titles (COO, CFO, SVP, etc.)
    return 0.5


def _max_indicator_score(indicators: List[AIBackgroundType]) -> float:
    if not indicators:
        return 0.0
    return max(AI_INDICATOR_SCORES.get(i, 0.0) for i in indicators)


def calculate_leadership_score_0_1(executives: List[LeadershipProfile]) -> float:
    """
    Weighted average of AI indicators across leadership team:
        sum(role_weight * max_ai_indicator) / sum(role_weight)
    This avoids penalizing companies just for listing more executives.
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


def _signal_id(company_id: str, name: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|leadership|exec|{name}|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def leadership_profiles_to_signals(company_id: str, executives: List[LeadershipProfile]) -> List[ExternalSignal]:
    """
    Per-executive signals (drill-down rows).
    Score is the executive's max AI indicator (0-100).
    Role weight + weighted contribution live in metadata for auditability.
    """
    now = datetime.utcnow()
    signals: List[ExternalSignal] = []

    for e in executives:
        ai_score = _max_indicator_score(e.ai_indicators)
        role_w = _role_weight(e.title)

        # Individual score (not weighted) for interpretability
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
    """
    Produce CompanySignalSummary.leadership_score from per-exec ExternalSignals.
    This is the roll-up that other pipeline components can consume.
    """
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
    """
    One aggregated leadership ExternalSignal (summary row).
    ID is deterministic based on executives content so repeated runs don't insert duplicates.
    """
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
        exec_details.append(
            {
                "name": e.name,
                "title": e.title,
                "ai_indicators": [i.value for i in e.ai_indicators],
                "max_indicator_score": round(ai_score, 3),
                "role_weight": round(role_w, 3),
                "weighted_contribution": round(role_w * ai_score, 3),
                "individual_score": int(round(ai_score * 100)),
                "observed_date": e.observed_date,
                "url": e.url,
            }
        )

    # Stable ordering
    exec_details_sorted = sorted(exec_details, key=lambda x: (x["name"].lower(), x["title"].lower()))

    meta = {
        "executive_count": len(executives),
        "company": executives[0].company if executives else "",
        "aggregated_score": score_0_100,
        "calculation_method": "weighted_avg(role_weight × max_ai_indicator) / sum(role_weight)",
        "executives": exec_details_sorted,
    }

    # Deterministic ID based on aggregated content
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