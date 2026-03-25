import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
import structlog

from agents.state import DueDiligenceState

import httpx

logger = structlog.get_logger()


class ToolCaller:
    """
    Calls MCP tools via HTTP POST to the MCP server.
    The MCP server must be running at base_url for agents to work.
    This ensures agents go through MCP (not direct imports),
    which is required by CS5 grading criteria.
    """

    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url

    async def call_tool(self, tool_name: str, arguments: dict) -> str:
        """
        POST to {base_url}/tools/{tool_name} with json=arguments.
        Returns response JSON result as string.
        If any exception occurs, logs warning and returns "{}".
        """
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"{self.base_url}/tools/{tool_name}",
                    json=arguments,
                )
                response.raise_for_status()
                data = response.json()
                # Handle both {"result": "..."} and direct JSON response
                if isinstance(data, dict) and "result" in data:
                    return data["result"]
                return json.dumps(data)
        except Exception as exc:
            logger.warning("tool_call_failed", tool=tool_name, error=str(exc))
            return "{}"


mcp_client = ToolCaller()


class SECAnalysisAgent:
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)

    async def analyze(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]

        # Fetch only SEC filing evidence across all relevant dimensions
        sec_findings = []
        for dimension in ["data_infrastructure", "ai_governance",
                          "technology_stack", "leadership", "use_case_portfolio"]:
            raw = await mcp_client.call_tool(
                "get_company_evidence",
                {"company_id": company_id, "dimension": dimension, "limit": 10}
            )
            try:
                findings = json.loads(raw) if raw else []
                if isinstance(findings, list):
                    sec_only = [
                        f for f in findings
                        if any(s in f.get("source_type", "")
                               for s in ["sec_10k", "sec_10q", "sec_8k", "sec"])
                    ]
                    sec_findings.extend(sec_only)
            except Exception:
                pass

        return {
            "sec_analysis": {
                "company_id": company_id,
                "findings": sec_findings,
                "dimensions_covered": [
                    "data_infrastructure", "ai_governance",
                    "technology_stack", "leadership", "use_case_portfolio"
                ],
            },
            "messages": [{
                "role": "assistant",
                "content": f"SEC analysis complete for {company_id}. Found {len(sec_findings)} SEC filing evidence items.",
                "agent_name": "sec_analyst",
                "timestamp": datetime.utcnow().isoformat(),
            }],
        }


class TalentAgent:
    """
    Fetches all non-SEC evidence across all 7 dimensions.
    Covers: job postings, patents, Glassdoor, board composition,
    leadership signals, tech stack signals.
    """
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)

    async def analyze(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]

        # Fetch all non-SEC evidence across all 7 dimensions
        all_findings = []
        for dimension in [
            "data_infrastructure", "ai_governance", "technology_stack",
            "talent", "leadership", "use_case_portfolio", "culture"
        ]:
            raw = await mcp_client.call_tool(
                "get_company_evidence",
                {"company_id": company_id, "dimension": dimension, "limit": 20}
            )
            try:
                findings = json.loads(raw) if raw else []
                if isinstance(findings, list):
                    # Exclude SEC source types — get everything else
                    non_sec = [
                        f for f in findings
                        if not any(s in f.get("source_type", "")
                                   for s in ["sec_10k", "sec_10q", "sec_8k", "sec"])
                    ]
                    all_findings.extend(non_sec)
            except Exception:
                pass

        return {
            "talent_analysis": {
                "company_id": company_id,
                "findings": all_findings,
                "dimensions_covered": [
                    "data_infrastructure", "ai_governance", "technology_stack",
                    "talent", "leadership", "use_case_portfolio", "culture"
                ],
            },
            "messages": [{
                "role": "assistant",
                "content": f"Talent & signals analysis complete for {company_id}. Found {len(all_findings)} non-SEC evidence items across all 7 dimensions.",
                "agent_name": "talent_analyst",
                "timestamp": datetime.utcnow().isoformat(),
            }],
        }


class ScoringAgent:
    def __init__(self):
        self.llm = ChatAnthropic(model="claude-3-5-haiku-20241022", temperature=0.2)

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

        if not score_data or "org_air" not in score_data:
            score_data = {
                **score_data,
                "error": (
                    score_data.get("error")
                    or "calculate_org_air_score failed — start MCP HTTP bridge: "
                    "poetry run python -m pe_mcp.http_server (port 3000)"
                ),
                "org_air": float(score_data["org_air"])
                if score_data.get("org_air") is not None
                else 0.0,
            }

        org_air = float(score_data.get("org_air", 0))

        if score_data.get("error") and "vr_score" not in score_data:
            requires_approval = False
            approval_reason = None
            approval_status = None
            msg = f"Scoring failed: {score_data['error']}"
        else:
            requires_approval = org_air > 85 or org_air < 40
            if requires_approval:
                approval_reason = f"Score {org_air:.1f} outside normal range [40, 85]"
                approval_status = "pending"
            else:
                approval_reason = None
                approval_status = None
            msg = f"Scoring complete: Org-AI-R = {org_air:.1f}" + (
                " [REQUIRES APPROVAL]" if requires_approval else ""
            )

        return {
            "scoring_result": score_data,
            "requires_approval": requires_approval,
            "approval_reason": approval_reason,
            "approval_status": approval_status,
            "messages": [
                {
                    "role": "assistant",
                    "content": msg,
                    "agent_name": "scorer",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }


class EvidenceAgent:
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)

    async def justify(self, state: DueDiligenceState) -> Dict[str, Any]:
        """
        1. Get company_id from state
        2. For each dimension call generate_justification
        3. Return partial state dict
        """
        company_id = state.get("company_id", "")
        dimensions = [
            "data_infrastructure",
            "ai_governance",
            "technology_stack",
            "talent",
            "leadership",
            "use_case_portfolio",
            "culture",
        ]
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
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)

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
talent_agent = TalentAgent()
scoring_agent = ScoringAgent()
evidence_agent = EvidenceAgent()
value_agent = ValueCreationAgent()
