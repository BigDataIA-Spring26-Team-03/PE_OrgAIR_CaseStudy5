"""Assessment History Tracking — stores score snapshots for trend analysis (Task 9.4)."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

import structlog

logger = structlog.get_logger()


@dataclass
class AssessmentSnapshot:
    """Single point-in-time assessment record."""
    company_id: str
    timestamp: datetime
    org_air: Decimal
    vr_score: Decimal
    hr_score: Decimal
    synergy_score: Decimal
    dimension_scores: Dict[str, Decimal]
    confidence_interval: tuple
    evidence_count: int
    assessor_id: str
    assessment_type: str  # "screening" | "limited" | "full"


@dataclass
class AssessmentTrend:
    """Trend analysis derived from historical snapshots."""
    company_id: str
    current_org_air: float
    entry_org_air: float
    delta_since_entry: float
    delta_30d: Optional[float]
    delta_90d: Optional[float]
    trend_direction: str   # "improving" | "stable" | "declining"
    snapshot_count: int


class AssessmentHistoryService:
    """
    Tracks assessment history.

    Storage: in-memory cache (production would persist to Snowflake via CS1).
    CS3 is called to capture the *current* score whenever record_assessment() runs.
    """

    def __init__(self, cs3_client=None):
        # Accept an optional cs3_client for dependency injection / testing.
        # If None, the caller is responsible for passing assessment data directly.
        self._cs3 = cs3_client
        self._cache: Dict[str, List[AssessmentSnapshot]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def record_assessment(
        self,
        company_id: str,
        assessor_id: str = "system",
        assessment_type: str = "full",
    ) -> AssessmentSnapshot:
        """
        Capture the current CS3 score as a snapshot and persist it.

        Flow:
        1. Call CS3 get_assessment() for live scores.
        2. Build AssessmentSnapshot with UTC timestamp.
        3. Append to in-memory cache (+ Snowflake stub).
        """
        if self._cs3 is None:
            raise RuntimeError("cs3_client not configured — cannot record assessment")

        assessment = await self._cs3.get_assessment(company_id)

        snapshot = AssessmentSnapshot(
            company_id=company_id,
            timestamp=datetime.utcnow(),
            org_air=Decimal(str(assessment.org_air_score)),
            vr_score=Decimal(str(assessment.vr_score)),
            hr_score=Decimal(str(assessment.hr_score)),
            synergy_score=Decimal(str(assessment.synergy_score)),
            dimension_scores={
                d.value: Decimal(str(s.score))
                for d, s in assessment.dimension_scores.items()
            },
            confidence_interval=assessment.confidence_interval,
            evidence_count=assessment.evidence_count,
            assessor_id=assessor_id,
            assessment_type=assessment_type,
        )

        await self._store_snapshot(snapshot)

        self._cache.setdefault(company_id, []).append(snapshot)

        logger.info(
            "assessment_recorded",
            company_id=company_id,
            org_air=float(snapshot.org_air),
            assessor=assessor_id,
        )
        return snapshot

    async def record_from_data(
        self,
        company_id: str,
        org_air: float,
        vr_score: float,
        hr_score: float,
        synergy_score: float,
        dimension_scores: Dict[str, float],
        confidence_interval: tuple,
        evidence_count: int,
        assessor_id: str = "system",
        assessment_type: str = "full",
        timestamp: Optional[datetime] = None,
    ) -> AssessmentSnapshot:
        """
        Record a snapshot from pre-computed score data (no CS3 call needed).
        Useful when the caller already has scores from portfolio_data_service.
        """
        snapshot = AssessmentSnapshot(
            company_id=company_id,
            timestamp=timestamp or datetime.utcnow(),
            org_air=Decimal(str(org_air)),
            vr_score=Decimal(str(vr_score)),
            hr_score=Decimal(str(hr_score)),
            synergy_score=Decimal(str(synergy_score)),
            dimension_scores={k: Decimal(str(v)) for k, v in dimension_scores.items()},
            confidence_interval=confidence_interval,
            evidence_count=evidence_count,
            assessor_id=assessor_id,
            assessment_type=assessment_type,
        )
        await self._store_snapshot(snapshot)
        self._cache.setdefault(company_id, []).append(snapshot)
        logger.info("assessment_recorded_from_data", company_id=company_id, org_air=org_air)
        return snapshot

    async def get_history(
        self,
        company_id: str,
        days: int = 365,
    ) -> List[AssessmentSnapshot]:
        """Return snapshots for company_id within the last `days` days."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        cached = self._cache.get(company_id, [])
        return [s for s in cached if s.timestamp >= cutoff]

    async def calculate_trend(self, company_id: str) -> AssessmentTrend:
        """Derive trend metrics from stored history."""
        history = await self.get_history(company_id, days=365)

        if not history:
            # No history yet — return a neutral trend with current score if CS3 available
            if self._cs3 is not None:
                current_assessment = await self._cs3.get_assessment(company_id)
                current_score = current_assessment.org_air_score
            else:
                current_score = 0.0

            return AssessmentTrend(
                company_id=company_id,
                current_org_air=current_score,
                entry_org_air=current_score,
                delta_since_entry=0.0,
                delta_30d=None,
                delta_90d=None,
                trend_direction="stable",
                snapshot_count=0,
            )

        # Sort chronologically
        history.sort(key=lambda s: s.timestamp)

        current = float(history[-1].org_air)
        entry = float(history[0].org_air)

        # Rolling deltas
        now = datetime.utcnow()
        delta_30d: Optional[float] = None
        delta_90d: Optional[float] = None

        for snapshot in reversed(history):
            age_days = (now - snapshot.timestamp).days
            if age_days >= 30 and delta_30d is None:
                delta_30d = current - float(snapshot.org_air)
            if age_days >= 90 and delta_90d is None:
                delta_90d = current - float(snapshot.org_air)
                break

        delta = current - entry
        if delta > 5:
            direction = "improving"
        elif delta < -5:
            direction = "declining"
        else:
            direction = "stable"

        return AssessmentTrend(
            company_id=company_id,
            current_org_air=current,
            entry_org_air=entry,
            delta_since_entry=round(delta, 1),
            delta_30d=round(delta_30d, 1) if delta_30d is not None else None,
            delta_90d=round(delta_90d, 1) if delta_90d is not None else None,
            trend_direction=direction,
            snapshot_count=len(history),
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _store_snapshot(self, snapshot: AssessmentSnapshot) -> None:
        """
        Persist snapshot to Snowflake via CS1.
        Stub in this implementation — production would INSERT INTO assessment_history.
        """
        pass  # Production: await cs1_client.insert_assessment_history(snapshot)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_history_service(cs3_client=None) -> AssessmentHistoryService:
    return AssessmentHistoryService(cs3_client=cs3_client)


# Module-level singleton (no CS3 client — use record_from_data or inject later)
assessment_history_service = AssessmentHistoryService()