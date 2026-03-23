"""
Assessment History API — Org-AI-R score snapshots over time.

GET /api/v1/assessment-history/{ticker} — history + trend for a company.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from src.services.integration.cs1_client import CS1Client
from src.services.integration.cs3_client import CS3Client
from src.services.tracking.assessment_history import create_history_service

router = APIRouter(prefix="/assessment-history", tags=["Assessment History"])


def _snapshot_to_dict(snap: Any) -> dict:
    """Convert AssessmentSnapshot to JSON-serializable dict."""
    ci = snap.confidence_interval if snap.confidence_interval else []
    return {
        "company_id": snap.company_id,
        "assessed_at": snap.timestamp.isoformat(),
        "org_air": float(snap.org_air),
        "vr_score": float(snap.vr_score),
        "hr_score": float(snap.hr_score),
        "synergy_score": float(snap.synergy_score),
        "confidence_interval": list(ci),
        "evidence_count": snap.evidence_count,
        "assessor_id": snap.assessor_id,
        "assessment_type": snap.assessment_type,
        "dimension_scores": {k: float(v) for k, v in snap.dimension_scores.items()},
    }


def _trend_to_dict(trend: Any) -> dict:
    """Convert AssessmentTrend to JSON-serializable dict."""
    return {
        "company_id": trend.company_id,
        "current_org_air": trend.current_org_air,
        "entry_org_air": trend.entry_org_air,
        "delta_since_entry": trend.delta_since_entry,
        "delta_30d": trend.delta_30d,
        "delta_90d": trend.delta_90d,
        "trend_direction": trend.trend_direction,
        "snapshot_count": trend.snapshot_count,
    }


@router.get("/{ticker}")
async def get_assessment_history(
    ticker: str,
    days: int = Query(365, ge=1, le=730, description="Lookback window in days"),
) -> Dict[str, Any]:
    """
    Get assessment history and trend for a company.
    Returns timeline of Org-AI-R snapshots plus trend metrics.
    """
    ticker = ticker.upper().strip()
    try:
        async with CS1Client() as cs1:
            async with CS3Client() as cs3:
                service = create_history_service(cs1, cs3)
                history = await service.get_history(ticker, days=days)
                trend = await service.calculate_trend(ticker)

        return {
            "company_id": ticker,
            "days": days,
            "trend": _trend_to_dict(trend),
            "history": [_snapshot_to_dict(s) for s in sorted(history, key=lambda x: x.timestamp)],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
