"""
PE Org-AI-R MCP HTTP Bridge — HTTP transport for MCP tools.

Exposes all 6 MCP tools from pe_mcp/server.py as HTTP endpoints
so LangGraph agents can call tools via POST to localhost:3000.
"""
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Any, Dict
import uvicorn
import asyncio
import json

from src.scoring.integration_service import ScoringIntegrationService
from src.services.retrieval.hybrid import HybridRetriever
from src.services.justification.generator import JustificationGenerator
from src.services.integration.portfolio_data_service import portfolio_data_service
from src.services.value_creation.ebitda import ebitda_calculator
from src.services.value_creation.gap_analysis import gap_analyzer
import structlog

logger = structlog.get_logger()

# Initialize services at module level
scoring_service = ScoringIntegrationService()
retriever = HybridRetriever()
justification_gen = JustificationGenerator()

http_app = FastAPI(title="PE Org-AI-R MCP HTTP Bridge", version="1.0.0")


class ToolRequest(BaseModel):
    company_id: str = ""
    dimension: str = "all"
    limit: int = 10
    entry_score: float = 0.0
    target_score: float = 0.0
    h_r_score: float = 0.0
    target_org_air: float = 75.0
    fund_id: str = "growth_fund_v"
    days: int = 365


@http_app.post("/tools/calculate_org_air_score")
async def calculate_org_air_score(req: ToolRequest):
    try:
        result = await asyncio.to_thread(
            scoring_service.score_company, req.company_id, "technology"
        )
        ci = result.get("confidence") or {}
        return {"result": json.dumps({
            "company_id": req.company_id,
            "org_air": result["final_score"],
            "vr_score": result["vr_score"],
            "hr_score": result["hr_score"],
            "synergy_score": result["synergy_score"],
            "confidence_interval": [ci.get("ci_lower", 0), ci.get("ci_upper", 100)],
            "dimension_scores": result["dimension_scores"],
        })}
    except Exception as e:
        logger.error("tool_error", tool="calculate_org_air_score", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.post("/tools/get_company_evidence")
async def get_company_evidence(req: ToolRequest):
    try:
        docs = await asyncio.to_thread(
            retriever.search,
            query=req.dimension if req.dimension != "all" else "AI readiness",
            top_k=req.limit,
            company_id=req.company_id or None,
            dimension=req.dimension if req.dimension != "all" else None,
        )
        return {"result": json.dumps([{
            "source_type": d.metadata.get("source_type", ""),
            "content": d.content[:500],
            "confidence": d.metadata.get("confidence", 0.0),
            "signal_category": d.metadata.get("signal_category", ""),
        } for d in docs])}
    except Exception as e:
        logger.error("tool_error", tool="get_company_evidence", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.post("/tools/generate_justification")
async def generate_justification(req: ToolRequest):
    try:
        from src.services.integration.cs3_client import Dimension
        justification = await justification_gen.generate_justification(
            company_id=req.company_id,
            dimension=Dimension(req.dimension),
        )
        return {"result": json.dumps({
            "dimension": req.dimension,
            "score": justification.score,
            "level": justification.level,
            "level_name": justification.level_name,
            "evidence_strength": justification.evidence_strength,
            "rubric_criteria": justification.rubric_criteria,
            "supporting_evidence": [
                {
                    "source_type": e.source_type,
                    "content": e.content[:300],
                    "confidence": e.confidence,
                }
                for e in justification.supporting_evidence[:5]
            ],
            "gaps_identified": justification.gaps_identified,
        })}
    except Exception as e:
        logger.error("tool_error", tool="generate_justification", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.post("/tools/project_ebitda_impact")
async def project_ebitda_impact(req: ToolRequest):
    try:
        projection = ebitda_calculator.project(
            company_id=req.company_id,
            entry_score=req.entry_score,
            exit_score=req.target_score,
            h_r_score=req.h_r_score,
        )
        return {"result": json.dumps({
            "delta_air": projection.delta_air,
            "scenarios": {
                "conservative": f"{projection.conservative_pct:.2f}%",
                "base": f"{projection.base_pct:.2f}%",
                "optimistic": f"{projection.optimistic_pct:.2f}%",
            },
            "risk_adjusted": f"{projection.risk_adjusted_pct:.2f}%",
            "requires_approval": projection.requires_approval,
        })}
    except Exception as e:
        logger.error("tool_error", tool="project_ebitda_impact", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.post("/tools/run_gap_analysis")
async def run_gap_analysis(req: ToolRequest):
    try:
        scoring_result = await asyncio.to_thread(
            scoring_service.score_company, req.company_id, "technology"
        )
        current_scores = scoring_result.get("dimension_scores") or {}
        # gap_analyzer expects {dim: score} not {dim: {full dict}}
        flat_scores = {
            k: v.get("score", v) if isinstance(v, dict) else v
            for k, v in current_scores.items()
        }
        analysis = gap_analyzer.analyze(
            company_id=req.company_id,
            current_scores=flat_scores,
            target_org_air=req.target_org_air,
        )
        return {"result": json.dumps(analysis)}
    except Exception as e:
        logger.error("tool_error", tool="run_gap_analysis", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.post("/tools/get_assessment_history")
async def get_assessment_history(req: ToolRequest):
    try:
        from src.services.integration.cs1_client import CS1Client
        from src.services.integration.cs3_client import CS3Client
        from src.services.tracking.assessment_history import create_history_service

        async with CS1Client() as cs1:
            async with CS3Client() as cs3:
                service = create_history_service(cs1, cs3)
                history = await service.get_history(req.company_id or "", days=req.days)
                trend = await service.calculate_trend(req.company_id or "")

        result = {
            "company_id": req.company_id,
            "days": req.days,
            "trend": {
                "current_org_air": trend.current_org_air,
                "entry_org_air": trend.entry_org_air,
                "delta_since_entry": trend.delta_since_entry,
                "delta_30d": trend.delta_30d,
                "delta_90d": trend.delta_90d,
                "trend_direction": trend.trend_direction,
                "snapshot_count": trend.snapshot_count,
            },
            "history": [
                {
                    "assessed_at": s.timestamp.isoformat(),
                    "org_air": float(s.org_air),
                    "vr_score": float(s.vr_score),
                    "hr_score": float(s.hr_score),
                    "synergy_score": float(s.synergy_score),
                    "confidence_interval": list(s.confidence_interval) if s.confidence_interval else [],
                    "evidence_count": s.evidence_count,
                    "assessor_id": s.assessor_id,
                    "assessment_type": s.assessment_type,
                    "dimension_scores": {k: float(v) for k, v in s.dimension_scores.items()},
                }
                for s in sorted(history, key=lambda x: x.timestamp)
            ],
        }
        return {"result": json.dumps(result, indent=2)}
    except Exception as e:
        logger.error("tool_error", tool="get_assessment_history", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.post("/tools/get_portfolio_summary")
async def get_portfolio_summary(req: ToolRequest):
    try:
        portfolio = await portfolio_data_service.get_portfolio_view(req.fund_id)
        fund_air = sum(c.org_air for c in portfolio) / len(portfolio) if portfolio else 0
        return {"result": json.dumps({
            "fund_id": req.fund_id,
            "fund_air": round(fund_air, 1),
            "company_count": len(portfolio),
            "companies": [
                {"ticker": c.ticker, "org_air": c.org_air, "sector": c.sector}
                for c in portfolio
            ],
        })}
    except Exception as e:
        logger.error("tool_error", tool="get_portfolio_summary", error=str(e))
        return {"result": json.dumps({"error": str(e)})}


@http_app.get("/health")
async def health():
    return {"status": "healthy", "service": "PE Org-AI-R MCP HTTP Bridge"}


if __name__ == "__main__":
    uvicorn.run(http_app, host="0.0.0.0", port=3000)
