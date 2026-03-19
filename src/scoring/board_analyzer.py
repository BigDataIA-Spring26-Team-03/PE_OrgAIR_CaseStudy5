"""
Board Composition Analyzer — AI Governance scoring from board-level signals.

Additive checklist: base 20 + up to 80 from six governance indicators.
Confidence scales with number of board members analyzed.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List

from scoring.utils import to_decimal, clamp


# ── Data Models ──────────────────────────────────────────────────────

@dataclass
class BoardMember:
    name: str
    title: str
    committees: List[str] = field(default_factory=list)
    bio: str = ""
    is_independent: bool = False
    tenure_years: float = 0.0


@dataclass
class GovernanceSignal:
    company_id: str
    ticker: str
    has_tech_committee: bool = False
    has_ai_expertise: bool = False
    has_data_officer: bool = False
    has_independent_majority: bool = False
    has_risk_tech_oversight: bool = False
    has_ai_strategy: bool = False
    governance_score: Decimal = Decimal("20")
    confidence: Decimal = Decimal("0.50")
    ai_experts: List[str] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)


# ── Analyzer ─────────────────────────────────────────────────────────

class BoardCompositionAnalyzer:
    """Score AI governance from board composition and strategy signals."""

    AI_EXPERTISE_KEYWORDS = [
        "artificial intelligence", "machine learning", "deep learning",
        "data science", "neural network", "nlp", "computer vision",
        "ai research", "ml engineer", "chief data", "chief ai",
    ]

    TECH_COMMITTEE_NAMES = [
        "technology", "digital", "innovation", "cyber", "data",
    ]

    DATA_OFFICER_TITLES = [
        "chief ai officer",
        "chief data officer",
        "chief technology officer",
        "chief digital officer",
        "chief information officer",
        "chief analytics officer",
    ]

    DATA_OFFICER_ABBREVIATIONS = {"caio", "cdo", "cto", "cio"}

    def analyze_board(
        self,
        company_id: str,
        ticker: str,
        members: List[BoardMember],
        committees: List[str],
        strategy_text: str = "",
    ) -> GovernanceSignal:
        signal = GovernanceSignal(company_id=company_id, ticker=ticker)
        score = Decimal("20")
        strategy_lower = strategy_text.lower()

        # 1. Tech / digital committee  (+15)
        for c in committees:
            if any(kw in c.lower() for kw in self.TECH_COMMITTEE_NAMES):
                signal.has_tech_committee = True
                signal.evidence.append(f"Tech committee found: {c}")
                score += Decimal("15")
                break

        # 2. AI expertise on board  (+20)
        for m in members:
            bio_lower = m.bio.lower()
            if any(kw in bio_lower for kw in self.AI_EXPERTISE_KEYWORDS):
                signal.ai_experts.append(m.name)
        if signal.ai_experts:
            signal.has_ai_expertise = True
            signal.evidence.append(
                f"AI experts: {', '.join(signal.ai_experts)}"
            )
            score += Decimal("20")

        # 3. Data / AI officer role  (+15)
        for m in members:
            title_lower = m.title.lower()
            title_words = set(title_lower.split())
            has_title = any(t in title_lower for t in self.DATA_OFFICER_TITLES)
            has_abbrev = bool(title_words & self.DATA_OFFICER_ABBREVIATIONS)
            if has_title or has_abbrev:
                signal.has_data_officer = True
                signal.evidence.append(f"Data officer: {m.name} ({m.title})")
                score += Decimal("15")
                break

        # 4. Independent majority  (+10)
        if members:
            ind_ratio = sum(1 for m in members if m.is_independent) / len(members)
            if ind_ratio > 0.5:
                signal.has_independent_majority = True
                signal.evidence.append(
                    f"Independent ratio: {ind_ratio:.2f}"
                )
                score += Decimal("10")

        # 5. Risk committee with tech oversight  (+10)
        has_risk = any("risk" in c.lower() for c in committees)
        tech_mention = any(
            kw in strategy_lower
            for kw in ["technology", "cyber", "digital", "data"]
        ) or any(
            kw in m.bio.lower()
            for m in members
            for kw in ["technology", "cyber", "digital"]
        )
        if has_risk and tech_mention:
            signal.has_risk_tech_oversight = True
            signal.evidence.append("Risk committee with tech oversight")
            score += Decimal("10")

        # 6. AI in strategy  (+10)
        if "artificial intelligence" in strategy_lower or "machine learning" in strategy_lower:
            signal.has_ai_strategy = True
            signal.evidence.append("AI mentioned in strategy")
            score += Decimal("10")

        # Cap & confidence
        signal.governance_score = clamp(score, Decimal("0"), Decimal("100"))
        n = Decimal(str(len(members)))
        signal.confidence = min(
            Decimal("0.5") + n / Decimal("20"),
            Decimal("0.95"),
        )

        return signal

    def extract_from_proxy(self, proxy_html: str):
        """Parse board data from proxy statement HTML (not yet implemented)."""
        raise NotImplementedError(
            "Proxy HTML parsing requires dedicated scraping logic."
        )
