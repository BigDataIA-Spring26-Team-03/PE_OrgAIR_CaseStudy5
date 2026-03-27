# src/services/tracking/assessment_history.py
"""
Assessment History Tracking — stores Org-AI-R score snapshots over time.

Why this exists
---------------
A single CS3 call tells you where a company stands *today*.  This service
layers time onto that signal: every time `record_assessment()` is called it
saves a timestamped snapshot, enabling:
  - Trend lines in the portfolio dashboard (improving / stable / declining)
  - 30-day and 90-day delta calculations for IC meeting prep
  - Drift detection in LangGraph agents (e.g. alert if score drops >5 pts)

Storage design
--------------
  Primary:    Snowflake `assessment_history` table via `SnowflakeHistoryStore`.
  Fallback:   In-memory dict (`_cache`) when Snowflake env vars are absent.

  SnowflakeHistoryStore uses a fresh connection per call (same pattern as
  `app/services/snowflake.py`) to avoid Snowflake 390114 token-expiry errors.
  The sync connector is wrapped in `run_in_executor` so it never blocks the
  asyncio event loop.

  If any of the six required env vars are missing the store silently disables
  itself (available=False), the service falls back to in-memory-only, and unit
  tests / CI without a Snowflake account continue to pass.

CS client dependencies
----------------------
  CS3Client  — fetches the current assessment inside record_assessment().
  CS1Client  — reserved for future Snowflake schema migrations via the CS1 API;
               not used for direct persistence (SnowflakeHistoryStore handles that).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from src.services.integration.cs1_client import CS1Client
from src.services.integration.cs3_client import CS3Client, CompanyAssessment

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Required Snowflake environment variables
# ---------------------------------------------------------------------------
_SNOWFLAKE_REQUIRED_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
)

# DDL executed once on first use — safe to re-run (IF NOT EXISTS)
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS assessment_history (
    id               VARCHAR(36)    NOT NULL DEFAULT uuid_string(),
    company_id       VARCHAR(20)    NOT NULL,
    assessed_at      TIMESTAMP_NTZ  NOT NULL,
    org_air          NUMBER(10,4)   NOT NULL,
    vr_score         NUMBER(10,4)   NOT NULL,
    hr_score         NUMBER(10,4)   NOT NULL,
    synergy_score    NUMBER(10,4)   NOT NULL,
    dimension_scores VARIANT,
    ci_lower         NUMBER(10,4),
    ci_upper         NUMBER(10,4),
    evidence_count   INTEGER,
    assessor_id      VARCHAR(100),
    assessment_type  VARCHAR(20),
    created_at       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
"""

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class AssessmentSnapshot:
    """
    Single point-in-time Org-AI-R assessment for one company.

    Scores are stored as Decimal to preserve precision across serialisation
    round-trips (floating-point repr can drift for scores like 71.82).
    """
    company_id:          str
    timestamp:           datetime
    org_air:             Decimal
    vr_score:            Decimal
    hr_score:            Decimal
    synergy_score:       Decimal
    # Keyed by dimension string value ("talent", "culture", …)
    dimension_scores:    Dict[str, Decimal]
    # (ci_lower, ci_upper) overall confidence interval from CS3
    confidence_interval: tuple
    # Total evidence items contributing to this snapshot (sum across dims)
    evidence_count:      int
    # Who triggered this assessment (agent name, user id, or "system")
    assessor_id:         str
    # "screening" (quick), "limited" (partial dims), or "full" (all dims)
    assessment_type:     str


@dataclass
class AssessmentTrend:
    """
    Trend summary derived from a company's snapshot history.

    Computed by `calculate_trend()` — never stored directly.
    """
    company_id:        str
    current_org_air:   float
    entry_org_air:     float           # score at the oldest snapshot
    delta_since_entry: float           # current − entry
    delta_30d:         Optional[float] # None if no snapshot >=30 days old
    delta_90d:         Optional[float] # None if no snapshot >=90 days old
    trend_direction:   str             # "improving" | "stable" | "declining"
    snapshot_count:    int


# ---------------------------------------------------------------------------
# Snowflake persistence layer
# ---------------------------------------------------------------------------

class SnowflakeHistoryStore:
    """
    Thin Snowflake persistence helper for assessment_history table.

    """

    def __init__(self) -> None:
        self._params = self._build_params()
        self._table_ready = False  # True after ensure_table() succeeds once

        if not self.available:
            missing = [v for v in _SNOWFLAKE_REQUIRED_VARS if not os.getenv(v)]
            logger.warning(
                "SnowflakeHistoryStore disabled — missing env vars: %s. "
                "AssessmentHistoryService will run in in-memory-only mode.",
                ", ".join(missing),
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """True when all required env vars are present."""
        return self._params is not None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _build_params(self) -> Optional[dict]:
        """Return connection params dict, or None if any env var is missing."""
        params = {v: os.getenv(v) for v in _SNOWFLAKE_REQUIRED_VARS}
        if any(v is None for v in params.values()):
            return None
        return {
            "account":                  params["SNOWFLAKE_ACCOUNT"],
            "user":                     params["SNOWFLAKE_USER"],
            "password":                 params["SNOWFLAKE_PASSWORD"],
            "database":                 params["SNOWFLAKE_DATABASE"],
            "schema":                   params["SNOWFLAKE_SCHEMA"],
            "warehouse":                params["SNOWFLAKE_WAREHOUSE"],
            "client_session_keep_alive": True,  # Prevents 390114 token expiry
        }

    def _new_connection(self):
        """
        Open a fresh Snowflake connection.
        """
        import snowflake.connector  # lazy import — not available in all envs
        return snowflake.connector.connect(**self._params)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def ensure_table(self) -> None:
        """
        Create the assessment_history table if it does not already exist.

        """
        if self._table_ready or not self.available:
            return
        conn = self._new_connection()
        try:
            conn.cursor().execute(_CREATE_TABLE_SQL)
            conn.commit()
            self._table_ready = True
            logger.info("Snowflake assessment_history table ready.")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert(self, snapshot: AssessmentSnapshot) -> None:
        """
        INSERT one snapshot row into assessment_history.

        """
        if not self.available:
            return

        self.ensure_table()

        ci_lower, ci_upper = (
            (snapshot.confidence_interval[0], snapshot.confidence_interval[1])
            if snapshot.confidence_interval
            else (None, None)
        )

        # Convert Decimal dimension scores to plain floats for JSON serialisation
        dim_json = json.dumps(
            {k: float(v) for k, v in snapshot.dimension_scores.items()}
        )

        sql = """
            INSERT INTO assessment_history
                (id, company_id, assessed_at, org_air, vr_score, hr_score,
                 synergy_score, dimension_scores, ci_lower, ci_upper,
                 evidence_count, assessor_id, assessment_type)
            SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s
        """
        params = (
            str(uuid.uuid4()),
            snapshot.company_id,
            snapshot.timestamp,
            float(snapshot.org_air),
            float(snapshot.vr_score),
            float(snapshot.hr_score),
            float(snapshot.synergy_score),
            dim_json,               # fed into PARSE_JSON(%s)
            ci_lower,
            ci_upper,
            snapshot.evidence_count,
            snapshot.assessor_id,
            snapshot.assessment_type,
        )

        conn = self._new_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            logger.debug("Snapshot persisted to Snowflake for %s", snapshot.company_id)
        except Exception as exc:
            # Log but do not raise — in-memory cache remains the source of truth
            logger.error(
                "Failed to persist snapshot for %s to Snowflake: %s",
                snapshot.company_id, exc,
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def query(
        self,
        company_id: str,
        cutoff: datetime,
    ) -> List[AssessmentSnapshot]:
        """
        SELECT snapshots for a company from `cutoff` onwards, oldest first.

        Used by get_history() for cold-start cache repopulation: when the
        service restarts and _cache is empty, this query recovers persisted
        history from Snowflake so trend analysis continues correctly.

        Returns an empty list on any error (graceful degradation).
        """
        if not self.available:
            return []

        sql = """
            SELECT company_id, assessed_at, org_air, vr_score, hr_score,
                   synergy_score, dimension_scores, ci_lower, ci_upper,
                   evidence_count, assessor_id, assessment_type
            FROM   assessment_history
            WHERE  company_id = %s
              AND  assessed_at >= %s
            ORDER BY assessed_at ASC
        """
        conn = self._new_connection()
        try:
            import snowflake.connector
            cursor = conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(sql, (company_id, cutoff))
            rows = cursor.fetchall()
        except Exception as exc:
            logger.error("Snowflake query failed for %s: %s", company_id, exc)
            return []
        finally:
            try:
                conn.close()
            except Exception:
                pass

        snapshots: List[AssessmentSnapshot] = []
        for row in rows:
            # Snowflake DictCursor returns UPPERCASE column names — normalize to lowercase
            row = {k.lower(): v for k, v in row.items()}
            raw_dims = row.get("dimension_scores") or {}
            # VARIANT columns may come back as a JSON string — parse if needed
            if isinstance(raw_dims, str):
                raw_dims = json.loads(raw_dims) if raw_dims else {}
            dim_scores = {k: Decimal(str(v)) for k, v in raw_dims.items()}

            ci_lower = row.get("ci_lower")
            ci_upper = row.get("ci_upper")

            snapshots.append(AssessmentSnapshot(
                company_id=row["company_id"],
                timestamp=row["assessed_at"],
                org_air=Decimal(str(row["org_air"])),
                vr_score=Decimal(str(row["vr_score"])),
                hr_score=Decimal(str(row["hr_score"])),
                synergy_score=Decimal(str(row["synergy_score"])),
                dimension_scores=dim_scores,
                confidence_interval=(
                    (float(ci_lower), float(ci_upper))
                    if ci_lower is not None and ci_upper is not None
                    else ()
                ),
                evidence_count=int(row.get("evidence_count") or 0),
                assessor_id=row.get("assessor_id") or "",
                assessment_type=row.get("assessment_type") or "full",
            ))

        return snapshots


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class AssessmentHistoryService:
    """
    Tracks Org-AI-R assessment history using CS3 for scores and Snowflake
    for durable persistence.

    Usage
    -----
    service = AssessmentHistoryService(cs1_client, cs3_client)
    snapshot = await service.record_assessment("NVDA", assessor_id="agent-1")
    trend    = await service.calculate_trend("NVDA")
    history  = await service.get_history("NVDA", days=90)

    The optional `store` parameter lets tests inject a disabled/mocked store:
    service = AssessmentHistoryService(cs1, cs3, store=SnowflakeHistoryStore())
    """

    def __init__(
        self,
        cs1_client: CS1Client,
        cs3_client: CS3Client,
        store: Optional[SnowflakeHistoryStore] = None,
    ) -> None:
        self.cs1 = cs1_client
        self.cs3 = cs3_client
        # In-memory store: company_id -> list of snapshots (chronological order)
        self._cache: Dict[str, List[AssessmentSnapshot]] = {}
        # Snowflake store (self-disables if env vars missing)
        self._store: SnowflakeHistoryStore = (
            store if store is not None else SnowflakeHistoryStore()
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def record_assessment(
        self,
        company_id:      str,
        assessor_id:     str,
        assessment_type: str = "full",
    ) -> AssessmentSnapshot:
        """
        Fetch the current Org-AI-R assessment from CS3 and store it as a
        timestamped snapshot.

        Flow
        ----
        1. Call CS3.get_assessment() for live scores (cached in CS3 client)
        2. Build AssessmentSnapshot with UTC timestamp
        3. Persist to Snowflake via _store_snapshot() (non-blocking executor)
        4. Append to in-memory cache
        5. Return snapshot

        Parameters
        ----------
        company_id:      Company ticker, e.g. "NVDA".
        assessor_id:     Caller identifier ("supervisor-agent", "user-1", ...).
        assessment_type: Granularity -- "screening" | "limited" | "full".
        """
        # 1. Fetch live assessment from CS3 (uses CS3's in-memory cache if
        #    the same ticker was already scored this server session)
        assessment: CompanyAssessment = await self.cs3.get_assessment(company_id)

        # 2. Map CS3 dimension scores to Decimal-keyed dict.
        #    DimensionScore.evidence_count is the per-dimension source count;
        #    we sum them for the snapshot-level total.
        dim_scores: Dict[str, Decimal] = {}
        total_evidence = 0
        for dim, ds in assessment.dimension_scores.items():
            dim_scores[dim.value] = Decimal(str(ds.score))
            total_evidence += ds.evidence_count

        snapshot = AssessmentSnapshot(
            company_id=company_id,
            timestamp=datetime.now(timezone.utc),
            org_air=Decimal(str(assessment.org_air_score)),
            vr_score=Decimal(str(assessment.vr_score)),
            hr_score=Decimal(str(assessment.hr_score)),
            synergy_score=Decimal(str(assessment.synergy_score)),
            dimension_scores=dim_scores,
            confidence_interval=assessment.confidence_interval,
            evidence_count=total_evidence,
            assessor_id=assessor_id,
            assessment_type=assessment_type,
        )

        # 3. Persist to Snowflake (runs in thread pool — does not block event loop)
        await self._store_snapshot(snapshot)

        # 4. Append to in-memory cache (initialise list if first entry)
        self._cache.setdefault(company_id, []).append(snapshot)

        logger.info(
            "assessment_recorded company=%s org_air=%s type=%s evidence=%d",
            company_id, float(snapshot.org_air), assessment_type, total_evidence,
        )
        return snapshot

    async def get_history(
        self,
        company_id: str,
        days: int = 365,
    ) -> List[AssessmentSnapshot]:
        """
        Return assessment snapshots for a company within the last `days` days,
        oldest first.

        Cold-start recovery
        -------------------
        If the cache is empty for this company AND Snowflake is available, this
        method queries Snowflake to repopulate the cache.  This means history
        is recovered correctly after a server restart.

        Parameters
        ----------
        company_id: Company ticker.
        days:       Lookback window in calendar days (default: 1 year).
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        if company_id not in self._cache:
            # Cold-start: attempt Snowflake read to repopulate cache
            if self._store.available:
                rows = await asyncio.get_event_loop().run_in_executor(
                    None, self._store.query, company_id, cutoff
                )
                if rows:
                    # Populate cache with ALL history (not just the cutoff window)
                    # so future calls with different day ranges also benefit
                    self._cache[company_id] = rows
                    # Apply the cutoff filter before returning
                    return [s for s in rows if s.timestamp >= cutoff]
            return []

        return [s for s in self._cache[company_id] if s.timestamp >= cutoff]

    async def calculate_trend(self, company_id: str) -> AssessmentTrend:
        """
        Compute trend metrics from snapshot history.

        If no history exists, fetches the current CS3 score and returns a
        baseline trend (delta=0, direction="stable").

        Trend direction rules:
          delta_since_entry >  5 pts -> "improving"
          delta_since_entry < -5 pts -> "declining"
          otherwise                  -> "stable"

        30-day / 90-day deltas are None when there are no snapshots old
        enough to anchor the comparison.
        """
        history = await self.get_history(company_id, days=365)

        if not history:
            # Bootstrap: no recorded history yet — use live CS3 score as baseline.
            # If CS3 is unreachable return a null trend rather than crashing.
            try:
                current_assessment = await self.cs3.get_assessment(company_id)
                current_score = current_assessment.org_air_score
            except Exception as exc:
                logger.warning(
                    "calculate_trend: CS3 unreachable for %s, returning null trend: %s",
                    company_id, exc,
                )
                return AssessmentTrend(
                    company_id=company_id,
                    current_org_air=0.0,
                    entry_org_air=0.0,
                    delta_since_entry=0.0,
                    delta_30d=None,
                    delta_90d=None,
                    trend_direction="stable",
                    snapshot_count=0,
                )
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

        # Ensure chronological order (oldest -> newest)
        history.sort(key=lambda s: s.timestamp)

        current_score = float(history[-1].org_air)
        entry_score   = float(history[0].org_air)

        # Walk backwards through history to find 30-day and 90-day anchors.
        # Walking newest->oldest means the first match at >=N days is the most
        # recent anchor, so deltas represent "movement in the last N days".
        now = datetime.now(timezone.utc)
        delta_30d: Optional[float] = None
        delta_90d: Optional[float] = None

        for snapshot in reversed(history):
            age_days = (now - snapshot.timestamp).days
            if age_days >= 30 and delta_30d is None:
                delta_30d = current_score - float(snapshot.org_air)
            if age_days >= 90 and delta_90d is None:
                delta_90d = current_score - float(snapshot.org_air)
                break   # 90-day anchor is the furthest we need; stop early

        # Determine overall trend from full entry-to-now delta
        overall_delta = current_score - entry_score
        if overall_delta > 5:
            direction = "improving"
        elif overall_delta < -5:
            direction = "declining"
        else:
            direction = "stable"

        return AssessmentTrend(
            company_id=company_id,
            current_org_air=current_score,
            entry_org_air=entry_score,
            delta_since_entry=round(overall_delta, 1),
            delta_30d=round(delta_30d, 1) if delta_30d is not None else None,
            delta_90d=round(delta_90d, 1) if delta_90d is not None else None,
            trend_direction=direction,
            snapshot_count=len(history),
        )

    def get_all_companies(self) -> List[str]:
        """Return every company_id that has at least one recorded snapshot."""
        return list(self._cache.keys())

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _store_snapshot(self, snapshot: AssessmentSnapshot) -> None:
        """
        Persist a snapshot to Snowflake via SnowflakeHistoryStore.

        The snowflake-connector-python library is synchronous, so we run the
        INSERT in a thread-pool executor to avoid blocking the asyncio event
        loop.  If Snowflake is unavailable (store.available=False) this is a
        no-op; the in-memory cache remains the source of truth.
        """
        if self._store.available:
            await asyncio.get_event_loop().run_in_executor(
                None, self._store.insert, snapshot
            )


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------

def create_history_service(
    cs1: CS1Client,
    cs3: CS3Client,
) -> AssessmentHistoryService:
    """
    Factory function matching the reference API.

    Prefer this over direct instantiation in tests and exercises so the
    dependency graph stays explicit.  The SnowflakeHistoryStore is created
    internally and self-disables if env vars are absent.
    """
    return AssessmentHistoryService(cs1, cs3)
