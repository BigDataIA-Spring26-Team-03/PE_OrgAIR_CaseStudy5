# src/scoring/evidence_mapper.py
# Evidence sources → weighted contributions → 7 final dimension scores

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
from decimal import Decimal
import structlog

from .rubric_scorer import RubricScorer, concatenate_evidence_chunks

logger = structlog.get_logger()


# ----------------------------
# ENUMS & DATA MODELS
# ----------------------------

class Dimension(str, Enum):
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT = "talent"
    LEADERSHIP = "leadership"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE = "culture"


class SignalSource(str, Enum):
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"

    SEC_ITEM_1 = "Item 1 (Business)"
    SEC_ITEM_1A = "Item 1A (Risk)"
    SEC_ITEM_7 = "Item 7 (MD&A)"
    SEC_ITEM_2 = "Item 2 (MD&A)"
    SEC_ITEM_8_01 = "Item 8.01 (Events)"

    GLASSDOOR_REVIEWS = "glassdoor_reviews"
    BOARD_COMPOSITION = "board_composition"


@dataclass(frozen=True)
class DimensionMapping:
    source: SignalSource
    primary_dimension: Dimension
    primary_weight: Decimal
    secondary_mappings: Dict[Dimension, Decimal] = field(default_factory=dict)
    reliability: Decimal = Decimal("0.8")

    def __post_init__(self):
        total = self.primary_weight + sum(self.secondary_mappings.values())
        if abs(total - Decimal("1.0")) > Decimal("0.001"):
            raise ValueError(f"Weights for {self.source.value} must sum to 1.0, got {total}")


@dataclass(frozen=True)
class EvidenceScore:
    source: SignalSource
    raw_score: Decimal
    confidence: Decimal
    evidence_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not (Decimal("0") <= self.raw_score <= Decimal("100")):
            raise ValueError(f"raw_score must be in [0,100], got {self.raw_score}")
        if not (Decimal("0") <= self.confidence <= Decimal("1")):
            raise ValueError(f"confidence must be in [0,1], got {self.confidence}")


@dataclass(frozen=True)
class DimensionScore:
    dimension: Dimension
    score: Decimal
    contributing_sources: List[SignalSource]
    total_weight: Decimal
    confidence: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "score": float(self.score),
            "contributing_sources": [s.value for s in self.contributing_sources],
            "total_weight": float(self.total_weight),
            "confidence": float(self.confidence),
        }


# ----------------------------
# SIGNAL → DIMENSION MAPPING
# ----------------------------

SIGNAL_TO_DIMENSION_MAP: Dict[SignalSource, DimensionMapping] = {
    SignalSource.TECHNOLOGY_HIRING: DimensionMapping(
        source=SignalSource.TECHNOLOGY_HIRING,
        primary_dimension=Dimension.TALENT,
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.10"),
            Dimension.TECHNOLOGY_STACK: Decimal("0.20"),
        },
        reliability=Decimal("0.85"),
    ),
    SignalSource.INNOVATION_ACTIVITY: DimensionMapping(
        source=SignalSource.INNOVATION_ACTIVITY,
        primary_dimension=Dimension.TECHNOLOGY_STACK,
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
            Dimension.USE_CASE_PORTFOLIO: Decimal("0.30"),
        },
        reliability=Decimal("0.80"),
    ),
    SignalSource.DIGITAL_PRESENCE: DimensionMapping(
        source=SignalSource.DIGITAL_PRESENCE,
        primary_dimension=Dimension.DATA_INFRASTRUCTURE,
        primary_weight=Decimal("0.60"),
        secondary_mappings={
            Dimension.TECHNOLOGY_STACK: Decimal("0.40"),
        },
        reliability=Decimal("0.75"),
    ),
    SignalSource.LEADERSHIP_SIGNALS: DimensionMapping(
        source=SignalSource.LEADERSHIP_SIGNALS,
        primary_dimension=Dimension.LEADERSHIP,
        primary_weight=Decimal("0.60"),
        secondary_mappings={
            Dimension.AI_GOVERNANCE: Decimal("0.25"),
            Dimension.CULTURE: Decimal("0.15"),
        },
        reliability=Decimal("0.80"),
    ),
    SignalSource.SEC_ITEM_1: DimensionMapping(
        source=SignalSource.SEC_ITEM_1,
        primary_dimension=Dimension.USE_CASE_PORTFOLIO,
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            Dimension.TECHNOLOGY_STACK: Decimal("0.30"),
        },
        reliability=Decimal("0.90"),
    ),
    SignalSource.SEC_ITEM_1A: DimensionMapping(
        source=SignalSource.SEC_ITEM_1A,
        primary_dimension=Dimension.AI_GOVERNANCE,
        primary_weight=Decimal("0.80"),
        secondary_mappings={
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
        },
        reliability=Decimal("0.90"),
    ),
    SignalSource.SEC_ITEM_7: DimensionMapping(
        source=SignalSource.SEC_ITEM_7,
        primary_dimension=Dimension.LEADERSHIP,
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            Dimension.USE_CASE_PORTFOLIO: Decimal("0.30"),
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
        },
        reliability=Decimal("0.85"),
    ),
    SignalSource.SEC_ITEM_2: DimensionMapping(
        source=SignalSource.SEC_ITEM_2,
        primary_dimension=Dimension.LEADERSHIP,
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            Dimension.USE_CASE_PORTFOLIO: Decimal("0.30"),
            Dimension.DATA_INFRASTRUCTURE: Decimal("0.20"),
        },
        reliability=Decimal("0.85"),
    ),
    SignalSource.SEC_ITEM_8_01: DimensionMapping(
        source=SignalSource.SEC_ITEM_8_01,
        primary_dimension=Dimension.LEADERSHIP,
        primary_weight=Decimal("1.0"),
        secondary_mappings={},
        reliability=Decimal("0.70"),
    ),
    SignalSource.GLASSDOOR_REVIEWS: DimensionMapping(
        source=SignalSource.GLASSDOOR_REVIEWS,
        primary_dimension=Dimension.CULTURE,
        primary_weight=Decimal("0.80"),
        secondary_mappings={
            Dimension.TALENT: Decimal("0.10"),
            Dimension.LEADERSHIP: Decimal("0.10"),
        },
        reliability=Decimal("0.70"),
    ),
    SignalSource.BOARD_COMPOSITION: DimensionMapping(
        source=SignalSource.BOARD_COMPOSITION,
        primary_dimension=Dimension.AI_GOVERNANCE,
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            Dimension.LEADERSHIP: Decimal("0.30"),
        },
        reliability=Decimal("0.85"),
    ),
}


# ----------------------------
# CORE MAPPER
# ----------------------------

def _clamp_0_100(x: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("100"), x))

def _clamp_0_1(x: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), x))


class EvidenceMapper:

    DEFAULT_SCORE = Decimal("50")
    DEFAULT_CONFIDENCE = Decimal("0.00")

    def __init__(self):
        self.mappings = SIGNAL_TO_DIMENSION_MAP
        logger.info("EvidenceMapper initialized", signal_sources=len(self.mappings))

    def map_evidence_to_dimensions(self, evidence_scores: List[EvidenceScore]) -> Dict[Dimension, DimensionScore]:
        if not evidence_scores:
            return self._default_dimension_scores()

        sum_weighted: Dict[Dimension, Decimal] = {d: Decimal("0") for d in Dimension}
        sum_weights: Dict[Dimension, Decimal] = {d: Decimal("0") for d in Dimension}
        sources: Dict[Dimension, List[SignalSource]] = {d: [] for d in Dimension}
        conf_complements: Dict[Dimension, List[Decimal]] = {d: [] for d in Dimension}

        for ev in evidence_scores:
            mapping = self.mappings.get(ev.source)
            if not mapping:
                logger.debug("Skipping unmapped source", source=ev.source.value)
                continue

            effective_conf = _clamp_0_1(ev.confidence * mapping.reliability)
            effective_score = _clamp_0_100(ev.raw_score)

            dim_primary = mapping.primary_dimension
            w_primary = mapping.primary_weight
            sum_weighted[dim_primary] += effective_score * w_primary
            sum_weights[dim_primary] += w_primary
            sources[dim_primary].append(ev.source)
            conf_complements[dim_primary].append(Decimal("1") - effective_conf)

            for dim, w in mapping.secondary_mappings.items():
                sum_weighted[dim] += effective_score * w
                sum_weights[dim] += w
                sources[dim].append(ev.source)
                conf_complements[dim].append(Decimal("1") - effective_conf)

        out: Dict[Dimension, DimensionScore] = {}

        for dim in Dimension:
            if sum_weights[dim] == 0:
                out[dim] = DimensionScore(
                    dimension=dim,
                    score=self.DEFAULT_SCORE,
                    contributing_sources=[],
                    total_weight=Decimal("0"),
                    confidence=self.DEFAULT_CONFIDENCE,
                )
                continue

            score = _clamp_0_100(sum_weighted[dim] / sum_weights[dim])
            unique_sources = sorted(set(sources[dim]), key=lambda s: s.value)

            prod = Decimal("1")
            for c in conf_complements[dim]:
                prod *= _clamp_0_1(c)
            confidence = _clamp_0_1(Decimal("1") - prod)

            out[dim] = DimensionScore(
                dimension=dim,
                score=score,
                contributing_sources=list(unique_sources),
                total_weight=sum_weights[dim],
                confidence=confidence,
            )

        return out

    def _default_dimension_scores(self) -> Dict[Dimension, DimensionScore]:
        return {
            dim: DimensionScore(
                dimension=dim,
                score=self.DEFAULT_SCORE,
                contributing_sources=[],
                total_weight=Decimal("0"),
                confidence=self.DEFAULT_CONFIDENCE,
            )
            for dim in Dimension
        }

    def get_coverage_report(self, evidence_scores: List[EvidenceScore]) -> Dict[str, Any]:
        dim_scores = self.map_evidence_to_dimensions(evidence_scores)

        report = {
            "total_dimensions": len(Dimension),
            "dimensions_with_evidence": 0,
            "dimensions_without_evidence": [],
            "coverage_by_dimension": {},
        }

        for dim, ds in dim_scores.items():
            has_evidence = len(ds.contributing_sources) > 0
            if has_evidence:
                report["dimensions_with_evidence"] += 1
            else:
                report["dimensions_without_evidence"].append(dim.value)

            report["coverage_by_dimension"][dim.value] = {
                "has_evidence": has_evidence,
                "source_count": len(ds.contributing_sources),
                "sources": [s.value for s in ds.contributing_sources],
                "total_weight": float(ds.total_weight),
                "confidence": float(ds.confidence),
                "score": float(ds.score),
            }

        report["coverage_percentage"] = report["dimensions_with_evidence"] / report["total_dimensions"] * 100
        return report


# ----------------------------
# SNOWFLAKE LOADERS
# ----------------------------

def _get(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def load_external_signals_from_snowflake(ticker: str, snowflake_service) -> List[EvidenceScore]:
    q = """
        SELECT
            es.category,
            AVG(es.normalized_score) AS avg_score,
            AVG(es.confidence)       AS avg_confidence,
            COUNT(*)                 AS signal_count
        FROM external_signals es
        JOIN companies c ON es.company_id = c.id
        WHERE UPPER(c.ticker) = UPPER(%(ticker)s)
        GROUP BY es.category
    """
    try:
        rows = snowflake_service.execute_query(q, {"ticker": ticker})
    except Exception as e:
        logger.warning("Could not load external_signals", ticker=ticker, err=str(e))
        return []

    out: List[EvidenceScore] = []
    for r in rows or []:
        category = _get(r, "CATEGORY", "category")
        if not category:
            continue

        try:
            source = SignalSource(str(category))
        except ValueError:
            logger.debug("Skipping unknown category", category=category)
            continue

        raw = _get(r, "AVG_SCORE", "avg_score")
        conf = _get(r, "AVG_CONFIDENCE", "avg_confidence")
        cnt = _get(r, "SIGNAL_COUNT", "signal_count") or 0

        raw_score = _clamp_0_100(Decimal(str(raw)) if raw is not None else Decimal("50"))
        confidence = _clamp_0_1(Decimal(str(conf)) if conf is not None else Decimal("0.50"))

        if int(cnt) >= 10:
            confidence = _clamp_0_1(confidence * Decimal("1.10"))

        out.append(
            EvidenceScore(
                source=source,
                raw_score=raw_score,
                confidence=confidence,
                evidence_count=int(cnt),
                metadata={"ticker": ticker, "category": category, "signal_count": int(cnt)},
            )
        )

    return out


def load_culture_evidence_from_snowflake(ticker: str, snowflake_service) -> List[EvidenceScore]:
    q = """
        SELECT
            overall_score,
            confidence,
            review_count,
            avg_rating,
            current_employee_ratio,
            created_at
        FROM culture_signals
        WHERE UPPER(ticker) = UPPER(%(ticker)s)
        ORDER BY created_at DESC
        LIMIT 1
    """
    try:
        rows = snowflake_service.execute_query(q, {"ticker": ticker})
    except Exception as e:
        logger.warning("Could not load culture_signals", ticker=ticker, err=str(e))
        return []

    if not rows:
        logger.info("No Glassdoor data found", ticker=ticker)
        return []

    r = rows[0]
    overall = _get(r, "OVERALL_SCORE", "overall_score")
    conf = _get(r, "CONFIDENCE", "confidence")
    rcnt = _get(r, "REVIEW_COUNT", "review_count") or 0

    if overall is None:
        return []

    score_dec = Decimal(str(overall))
    conf_dec = Decimal(str(conf)) if conf is not None else Decimal("0.50")

    if score_dec == Decimal("50") and conf_dec < Decimal("0.3"):
        logger.info("Skipping default Glassdoor score", ticker=ticker, score=float(score_dec), confidence=float(conf_dec))
        return []

    return [
        EvidenceScore(
            source=SignalSource.GLASSDOOR_REVIEWS,
            raw_score=_clamp_0_100(score_dec),
            confidence=_clamp_0_1(conf_dec),
            evidence_count=int(rcnt),
            metadata={
                "ticker": ticker,
                "avg_rating": float(_get(r, "AVG_RATING", "avg_rating") or 0),
                "current_employee_ratio": float(_get(r, "CURRENT_EMPLOYEE_RATIO", "current_employee_ratio") or 0),
            },
        )
    ]


def load_board_evidence_from_snowflake(ticker: str, snowflake_service) -> List[EvidenceScore]:
    q = """
        SELECT
            governance_score,
            confidence,
            has_tech_committee,
            has_ai_expertise,
            has_data_officer,
            has_independent_majority,
            has_risk_tech_oversight,
            has_ai_strategy,
            ai_experts,
            evidence,
            created_at
        FROM board_governance_signals
        WHERE UPPER(ticker) = UPPER(%(ticker)s)
        ORDER BY created_at DESC
        LIMIT 1
    """
    try:
        rows = snowflake_service.execute_query(q, {"ticker": ticker})
    except Exception as e:
        logger.warning("Could not load board_governance_signals", ticker=ticker, err=str(e))
        return []

    if not rows:
        return []

    r = rows[0]
    score = _get(r, "GOVERNANCE_SCORE", "governance_score")
    conf = _get(r, "CONFIDENCE", "confidence")

    if score is None:
        return []

    return [
        EvidenceScore(
            source=SignalSource.BOARD_COMPOSITION,
            raw_score=_clamp_0_100(Decimal(str(score))),
            confidence=_clamp_0_1(Decimal(str(conf)) if conf is not None else Decimal("0.50")),
            evidence_count=1,
            metadata={
                "ticker": ticker,
                "has_tech_committee": bool(_get(r, "HAS_TECH_COMMITTEE", "has_tech_committee") or False),
                "has_ai_expertise": bool(_get(r, "HAS_AI_EXPERTISE", "has_ai_expertise") or False),
                "has_data_officer": bool(_get(r, "HAS_DATA_OFFICER", "has_data_officer") or False),
                "has_independent_majority": bool(_get(r, "HAS_INDEPENDENT_MAJORITY", "has_independent_majority") or False),
                "has_risk_tech_oversight": bool(_get(r, "HAS_RISK_TECH_OVERSIGHT", "has_risk_tech_oversight") or False),
                "has_ai_strategy": bool(_get(r, "HAS_AI_STRATEGY", "has_ai_strategy") or False),
            },
        )
    ]


def load_sec_evidence_from_snowflake_with_rubrics(ticker: str, snowflake_service) -> List[EvidenceScore]:
    scorer = RubricScorer()

    q = """
        SELECT
            c.section,
            c.content,
            c.chunk_index
        FROM document_chunks_sec c
        JOIN documents_sec d ON c.document_id = d.id
        WHERE UPPER(d.ticker) = UPPER(%(ticker)s)
          AND c.section IS NOT NULL
          AND c.section NOT IN ('Unknown', 'Intro')
        ORDER BY c.section, c.chunk_index
    """
    try:
        rows = snowflake_service.execute_query(q, {"ticker": ticker})
    except Exception as e:
        logger.error("Failed to load SEC chunks", ticker=ticker, err=str(e))
        return []

    if not rows:
        return []

    chunks_by_section: Dict[str, List[str]] = {}
    for r in rows:
        sec = _get(r, "SECTION", "section")
        txt = _get(r, "CONTENT", "content") or ""
        if not sec:
            continue
        chunks_by_section.setdefault(str(sec), []).append(str(txt))

    out: List[EvidenceScore] = []

    for section, chunks in chunks_by_section.items():
        try:
            source = SignalSource(section)
        except ValueError:
            logger.debug("Skipping unmapped SEC section", section=section)
            continue

        mapping = SIGNAL_TO_DIMENSION_MAP.get(source)
        if not mapping:
            continue

        text = concatenate_evidence_chunks(chunks)

        primary_dim = mapping.primary_dimension
        rr = scorer.score_dimension(primary_dim.value, text, quantitative_metrics={})

        raw_score = _clamp_0_100(rr.score)
        confidence = _clamp_0_1(rr.confidence * mapping.reliability)

        logger.info(
            "sec_section_scored",
            ticker=ticker,
            section=section,
            primary_dim=primary_dim.value,
            score=float(raw_score),
            level=rr.level.label,
            keywords_matched=rr.matched_keywords[:5],
        )

        out.append(
            EvidenceScore(
                source=source,
                raw_score=raw_score,
                confidence=confidence,
                evidence_count=len(chunks),
                metadata={
                    "ticker": ticker,
                    "section": section,
                    "primary_dimension": primary_dim.value,
                    "rubric_level": rr.level.label,
                    "keyword_matches": rr.keyword_match_count,
                    "matched_keywords": rr.matched_keywords[:10],
                    "chunk_count": len(chunks),
                },
            )
        )

    return out


def load_all_evidence_from_snowflake(ticker: str, snowflake_service) -> List[EvidenceScore]:
    evidence: List[EvidenceScore] = []
    evidence.extend(load_external_signals_from_snowflake(ticker, snowflake_service))
    evidence.extend(load_sec_evidence_from_snowflake_with_rubrics(ticker, snowflake_service))
    evidence.extend(load_culture_evidence_from_snowflake(ticker, snowflake_service))
    evidence.extend(load_board_evidence_from_snowflake(ticker, snowflake_service))
    return evidence


def map_evidence(evidence_scores: List[EvidenceScore]) -> Dict[Dimension, DimensionScore]:
    return EvidenceMapper().map_evidence_to_dimensions(evidence_scores)