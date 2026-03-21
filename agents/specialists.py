import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
import structlog

from agents.state import DueDiligenceState

# Direct service calls — no HTTP bridge required
from src.services.integration.cs2_client import CS2Client, SignalCategory
from src.services.integration.cs3_client import CS3Client
from src.services.cs4_client import cs4_client
from src.services.integration.portfolio_data_service import portfolio_data_service
from src.services.value_creation.ebitda import ebitda_calculator
from src.services.value_creation.gap_analysis import gap_analyzer

logger = structlog.get_logger()

_DIMENSION_TO_SIGNALS: Dict[str, Optional[List[SignalCategory]]] = {
    "data_infrastructure": [SignalCategory.INNOVATION_ACTIVITY, SignalCategory.DIGITAL_PRESENCE],
    "ai_governance": [SignalCategory.GOVERNANCE_SIGNALS, SignalCategory.BOARD_COMPOSITION],
    "technology_stack": [SignalCategory.INNOVATION_ACTIVITY, SignalCategory.TECHNOLOGY_HIRING],
    "talent": [SignalCategory.TECHNOLOGY_HIRING, SignalCategory.CULTURE_SIGNALS],
    "leadership": [SignalCategory.LEADERSHIP_SIGNALS, SignalCategory.BOARD_COMPOSITION],
    "use_case_portfolio": [SignalCategory.INNOVATION_ACTIVITY, SignalCategory.DIGITAL_PRESENCE],
    "culture": [SignalCategory.CULTURE_SIGNALS, SignalCategory.GLASSDOOR_REVIEWS],
    "all": None,
}


class ToolCaller:
    """
    Invokes MCP tools by calling the underlying services directly.
    No HTTP bridge needed — agents work when run in the same process as the app.
    """

    async def call_tool(self, tool_name: str, arguments: dict) -> str:
        """Execute tool logic and return JSON string result."""
        try:
            if tool_name == "calculate_org_air_score":
                return await self._calculate_org_air_score(arguments)
            if tool_name == "get_company_evidence":
                return await self._get_company_evidence(arguments)
            if tool_name == "generate_justification":
                return await self._generate_justification(arguments)
            if tool_name == "run_gap_analysis":
                return await self._run_gap_analysis(arguments)
            if tool_name == "project_ebitda_impact":
                return self._project_ebitda_impact(arguments)
            if tool_name == "get_portfolio_summary":
                return await self._get_portfolio_summary(arguments)
        except Exception as exc:
            logger.warning("tool_call_failed", tool=tool_name, error=str(exc))
        return "{}"

    async def _calculate_org_air_score(self, args: dict) -> str:
        async with CS3Client() as cs3:
            a = await cs3.get_assessment(args["company_id"])
        result = {
            "company_id": args["company_id"],
            "assessed_at": datetime.now(timezone.utc).isoformat(),
            "org_air": a.org_air_score,
            "vr_score": a.vr_score,
            "hr_score": a.hr_score,
            "synergy_score": a.synergy_score,
            "confidence_interval": list(a.confidence_interval),
            "dimension_scores": {d.value: s.score for d, s in a.dimension_scores.items()},
        }
        return json.dumps(result, indent=2)

    async def _get_company_evidence(self, args: dict) -> str:
        dim_key = args.get("dimension", "all")
        signal_cats = _DIMENSION_TO_SIGNALS.get(dim_key)
        limit = args.get("limit", 10)
        async with CS2Client() as cs2:
            evidence = await cs2.get_evidence(
                company_id=args["company_id"],
                signal_categories=signal_cats,
                limit=limit,
            )
        items = [
            {
                "source_type": e.source_type.value,
                "content": e.content[:500],
                "confidence": e.confidence,
                "signal_category": e.signal_category.value,
                "filing_type": e.filing_type,
                "extracted_at": e.extracted_at.isoformat() if e.extracted_at else None,
            }
            for e in evidence
        ]
        return json.dumps(items, indent=2)

    async def _generate_justification(self, args: dict) -> str:
        j = await cs4_client.generate_justification(
            company_id=args["company_id"],
            dimension=args["dimension"],
        )
        result = {
            "dimension": args["dimension"],
            "score": j.score,
            "level": j.level,
            "level_name": j.level_name,
            "evidence_strength": j.evidence_strength,
            "rubric_criteria": j.rubric_criteria,
            "rubric_keywords": j.rubric_keywords,
            "generated_summary": j.generated_summary,
            "supporting_evidence": [
                {
                    "source_type": e.source_type,
                    "content": e.content[:300],
                    "confidence": e.confidence,
                    "matched_keywords": e.matched_keywords,
                }
                for e in j.supporting_evidence[:5]
            ],
            "gaps_identified": j.gaps_identified,
        }
        return json.dumps(result, indent=2)

    async def _run_gap_analysis(self, args: dict) -> str:
        async with CS3Client() as cs3:
            a = await cs3.get_assessment(args["company_id"])
        current_scores = {d.value: s.score for d, s in a.dimension_scores.items()}
        analysis = gap_analyzer.analyze(
            company_id=args["company_id"],
            current_scores=current_scores,
            target_org_air=args["target_org_air"],
        )
        return json.dumps(analysis, indent=2)

    def _project_ebitda_impact(self, args: dict) -> str:
        p = ebitda_calculator.project(
            company_id=args["company_id"],
            entry_score=args["entry_score"],
            exit_score=args["target_score"],
            h_r_score=args["h_r_score"],
        )
        result = {
            "company_id": args["company_id"],
            "delta_air": float(p.delta_air),
            "scenarios": {
                "conservative": f"{p.conservative_pct:.2f}%",
                "base": f"{p.base_pct:.2f}%",
                "optimistic": f"{p.optimistic_pct:.2f}%",
            },
            "risk_adjusted": f"{p.risk_adjusted_pct:.2f}%",
            "requires_approval": p.requires_approval,
        }
        return json.dumps(result, indent=2)

    async def _get_portfolio_summary(self, args: dict) -> str:
        companies = await portfolio_data_service.get_portfolio_view(args["fund_id"])
        fund_air = round(sum(c.org_air for c in companies) / len(companies), 1) if companies else 0.0
        result = {
            "fund_id": args["fund_id"],
            "fund_air": fund_air,
            "company_count": len(companies),
            "companies": [
                {
                    "ticker": c.ticker,
                    "name": c.name,
                    "sector": c.sector,
                    "org_air": c.org_air,
                    "vr_score": c.vr_score,
                    "hr_score": c.hr_score,
                    "delta_since_entry": c.delta_since_entry,
                }
                for c in companies
            ],
        }
        return json.dumps(result, indent=2)


mcp_client = ToolCaller()


class SECAnalysisAgent:
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0.3)

    async def analyze(self, state: DueDiligenceState) -> Dict[str, Any]:
        """
        1. Get company_id from state
        2. Call mcp_client.call_tool("get_company_evidence", ...)
        3. Parse the result as JSON
        4. Return partial state dict
        """
        company_id = state.get("company_id", "")
        raw = await mcp_client.call_tool(
            "get_company_evidence",
            {"company_id": company_id, "dimension": "all", "limit": 10},
        )
        try:
            findings = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(findings, list):
                findings = []
        except (json.JSONDecodeError, TypeError):
            findings = []

        return {
            "sec_analysis": {
                "company_id": company_id,
                "findings": findings,
                "dimensions_covered": [
                    "data_infrastructure",
                    "ai_governance",
                    "technology_stack",
                ],
            },
            "messages": [
                {
                    "role": "assistant",
                    "content": f"SEC analysis complete for {company_id}. Found {len(findings)} evidence items.",
                    "agent_name": "sec_analyst",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }


class ScoringAgent:
    def __init__(self):
        self.llm = ChatAnthropic(model="claude-sonnet-4-20250514", temperature=0.2)

    async def calculate(self, state: DueDiligenceState) -> Dict[str, Any]:
        """
        1. Get company_id from state
        2. Call mcp_client.call_tool("calculate_org_air_score", ...)
        3. Parse result
        4. HITL check: org_air > 85 or org_air < 40
        5. Return partial state dict
        """
        company_id = state.get("company_id", "")
        raw = await mcp_client.call_tool(
            "calculate_org_air_score",
            {"company_id": company_id},
        )
        try:
            score_data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(score_data, dict):
                score_data = {}
        except (json.JSONDecodeError, TypeError):
            score_data = {}

        org_air = float(score_data.get("org_air", 0))
        requires_approval = org_air > 85 or org_air < 40
        if requires_approval:
            approval_reason = f"Score {org_air:.1f} outside normal range [40, 85]"
            approval_status = "pending"
        else:
            approval_reason = None
            approval_status = None

        return {
            "scoring_result": score_data,
            "requires_approval": requires_approval,
            "approval_reason": approval_reason,
            "approval_status": approval_status,
            "messages": [
                {
                    "role": "assistant",
                    "content": f"Scoring complete: Org-AI-R = {org_air:.1f}"
                    + (" [REQUIRES APPROVAL]" if requires_approval else ""),
                    "agent_name": "scorer",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }


class EvidenceAgent:
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0.3)

    async def justify(self, state: DueDiligenceState) -> Dict[str, Any]:
        """
        1. Get company_id from state
        2. For each dimension call generate_justification
        3. Return partial state dict
        """
        company_id = state.get("company_id", "")
        dimensions = ["data_infrastructure", "talent", "use_case_portfolio"]
        justifications: Dict[str, Any] = {}

        for dim in dimensions:
            raw = await mcp_client.call_tool(
                "generate_justification",
                {"company_id": company_id, "dimension": dim},
            )
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                justifications[dim] = parsed if isinstance(parsed, dict) else {}
            except (json.JSONDecodeError, TypeError):
                justifications[dim] = {}

        return {
            "evidence_justifications": {
                "company_id": company_id,
                "justifications": justifications,
            },
            "messages": [
                {
                    "role": "assistant",
                    "content": f"Generated justifications for {len(justifications)} dimensions.",
                    "agent_name": "evidence_agent",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }


class ValueCreationAgent:
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0.3)

    async def plan(self, state: DueDiligenceState) -> Dict[str, Any]:
        """
        1. Get company_id from state
        2. Call run_gap_analysis
        3. HITL check: projected_ebitda_pct > 5.0 or existing requires_approval
        4. Return partial state dict
        """
        company_id = state.get("company_id", "")
        raw = await mcp_client.call_tool(
            "run_gap_analysis",
            {"company_id": company_id, "target_org_air": 80.0},
        )
        try:
            gap_data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(gap_data, dict):
                gap_data = {}
        except (json.JSONDecodeError, TypeError):
            gap_data = {}

        projected_ebitda_pct = float(gap_data.get("projected_ebitda_pct", 0))
        ebitda_trigger = projected_ebitda_pct > 5.0
        requires_approval = ebitda_trigger or state.get("requires_approval", False)

        if ebitda_trigger:
            approval_reason = state.get("approval_reason") or (
                f"EBITDA projection {projected_ebitda_pct:.1f}% > 5%"
            )
        else:
            approval_reason = state.get("approval_reason")

        return {
            "value_creation_plan": {
                "company_id": company_id,
                "gap_analysis": gap_data,
            },
            "requires_approval": requires_approval,
            "approval_reason": approval_reason,
            "messages": [
                {
                    "role": "assistant",
                    "content": f"Value creation plan complete. Projected EBITDA impact: {projected_ebitda_pct:.1f}%",
                    "agent_name": "value_creator",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }


sec_agent = SECAnalysisAgent()
scoring_agent = ScoringAgent()
evidence_agent = EvidenceAgent()
value_agent = ValueCreationAgent()
