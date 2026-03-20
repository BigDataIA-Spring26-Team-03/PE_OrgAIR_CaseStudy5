# pe_mcp/server.py
"""
PE Org-AI-R MCP Server — Universal agent interoperability layer.

This server exposes YOUR CS1-CS4 APIs as MCP tools so that Claude,
GPT-4, or any MCP-compatible client can call your platform.

Entry point: python -m pe_mcp.server
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import logging

import nest_asyncio
nest_asyncio.apply()

# Import installed MCP SDK (no shadow — we live in pe_mcp, not mcp)
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Prompt,
    PromptMessage,
    Resource,
    TextContent,
    Tool,
)

from src.services.integration.cs2_client import CS2Client, SignalCategory
from src.services.integration.cs3_client import CS3Client, Dimension
from src.services.cs4_client import CS4Client, cs4_client
from src.services.integration.portfolio_data_service import portfolio_data_service
from src.services.value_creation.ebitda import ebitda_calculator
from src.services.value_creation.gap_analysis import gap_analyzer

logger = logging.getLogger(__name__)

cs2_client = CS2Client()
cs3_client = CS3Client()
mcp_server = Server("pe-orgair-server")

_DIMENSION_TO_SIGNALS: Dict[str, Optional[List[SignalCategory]]] = {
    "data_infrastructure": [SignalCategory.INNOVATION_ACTIVITY, SignalCategory.DIGITAL_PRESENCE],
    "ai_governance":       [SignalCategory.GOVERNANCE_SIGNALS, SignalCategory.BOARD_COMPOSITION],
    "technology_stack":    [SignalCategory.INNOVATION_ACTIVITY, SignalCategory.TECHNOLOGY_HIRING],
    "talent":              [SignalCategory.TECHNOLOGY_HIRING, SignalCategory.CULTURE_SIGNALS],
    "leadership":          [SignalCategory.LEADERSHIP_SIGNALS, SignalCategory.BOARD_COMPOSITION],
    "use_case_portfolio":  [SignalCategory.INNOVATION_ACTIVITY, SignalCategory.DIGITAL_PRESENCE],
    "culture":             [SignalCategory.CULTURE_SIGNALS, SignalCategory.GLASSDOOR_REVIEWS],
    "all":                 None,
}


@mcp_server.list_tools()
async def list_tools() -> List[Tool]:
    """Advertise all available tools to connecting MCP clients."""
    return [
        Tool(
            name="calculate_org_air_score",
            description=(
                "Calculate the Org-AI-R readiness score for a company using the CS3 "
                "scoring engine. Returns the final score, V^R sub-score, H^R sub-score, "
                "synergy score, confidence interval, and a per-dimension breakdown."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {
                        "type": "string",
                        "description": "Company ticker symbol, e.g. 'NVDA'.",
                    },
                },
                "required": ["company_id"],
            },
        ),
        Tool(
            name="get_company_evidence",
            description=(
                "Retrieve AI-readiness evidence for a company from the CS2 evidence "
                "store.  Filter by dimension to focus on a specific scoring area, or "
                "use 'all' to get a broad evidence sample."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {"type": "string"},
                    "dimension": {
                        "type": "string",
                        "enum": [
                            "data_infrastructure", "ai_governance",
                            "technology_stack", "talent", "leadership",
                            "use_case_portfolio", "culture", "all",
                        ],
                        "default": "all",
                        "description": "Scoring dimension to filter evidence by.",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10,
                        "description": "Maximum number of evidence items to return.",
                    },
                },
                "required": ["company_id"],
            },
        ),
        Tool(
            name="generate_justification",
            description=(
                "Generate an evidence-backed score justification using the CS4 RAG "
                "pipeline (HyDE + hybrid retrieval).  Returns the score, rubric match, "
                "up to 5 cited evidence items, and an LLM-generated IC-ready summary."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {"type": "string"},
                    "dimension": {
                        "type": "string",
                        "enum": [
                            "data_infrastructure", "ai_governance",
                            "technology_stack", "talent", "leadership",
                            "use_case_portfolio", "culture",
                        ],
                    },
                },
                "required": ["company_id", "dimension"],
            },
        ),
        Tool(
            name="project_ebitda_impact",
            description=(
                "Project EBITDA uplift from an Org-AI-R improvement using the v2.0 "
                "impact model.  Returns conservative, base, and optimistic scenarios "
                "plus a risk-adjusted figure that accounts for the H^R score. "
                "Sets requires_approval=True when base projection exceeds 5%."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {"type": "string"},
                    "entry_score": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 100,
                        "description": "Org-AI-R score at investment entry.",
                    },
                    "target_score": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 100,
                        "description": "Projected exit Org-AI-R score.",
                    },
                    "h_r_score": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 100,
                        "description": "H^R (Human Readiness) score — adjusts risk.",
                    },
                },
                "required": ["company_id", "entry_score", "target_score", "h_r_score"],
            },
        ),
        Tool(
            name="run_gap_analysis",
            description=(
                "Analyse the gap between current dimension scores and a target "
                "Org-AI-R, then generate a prioritised improvement roadmap with "
                "investment estimates.  Current scores are fetched live from CS3 — "
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company_id": {"type": "string"},
                    "target_org_air": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 100,
                        "description": "Target overall Org-AI-R score.",
                    },
                },
                "required": ["company_id", "target_org_air"],
            },
        ),
        Tool(
            name="get_portfolio_summary",
            description=(
                "Fetch a full portfolio summary including Fund-AI-R (average Org-AI-R "
                "across all companies), per-company scores, and sector breakdown. "
                "Scores are fetched concurrently from CS3 "
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "fund_id": {
                        "type": "string",
                        "description": "Fund identifier, e.g. 'fund-001'.",
                    },
                },
                "required": ["fund_id"],
            },
        ),
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> List[TextContent]:
    """Route tool calls to the appropriate CS1-CS4 API."""
    logger.info("mcp_tool_call: tool=%s args=%s", name, arguments)

    try:
        if name == "calculate_org_air_score":
            assessment = await cs3_client.get_assessment(arguments["company_id"])
            result = {
                "company_id":          arguments["company_id"],
                "assessed_at":         datetime.utcnow().isoformat(),
                "org_air":             assessment.org_air_score,
                "vr_score":            assessment.vr_score,
                "hr_score":            assessment.hr_score,
                "synergy_score":       assessment.synergy_score,
                "confidence_interval": list(assessment.confidence_interval),
                "dimension_scores": {
                    d.value: s.score
                    for d, s in assessment.dimension_scores.items()
                },
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_company_evidence":
            dim_key = arguments.get("dimension", "all")
            signal_cats = _DIMENSION_TO_SIGNALS.get(dim_key)

            evidence = await cs2_client.get_evidence(
                company_id=arguments["company_id"],
                signal_categories=signal_cats,
                limit=arguments.get("limit", 10),
            )
            items = [
                {
                    "source_type":     e.source_type.value if hasattr(e.source_type, "value") else e.source_type,
                    "content":         e.content[:500],
                    "confidence":      e.confidence,
                    "signal_category": e.signal_category.value if hasattr(e.signal_category, "value") else str(e.signal_category),
                    "filing_type":     e.filing_type,
                    "extracted_at":    e.extracted_at.isoformat() if e.extracted_at else None,
                }
                for e in evidence
            ]
            return [TextContent(type="text", text=json.dumps(items, indent=2))]

        elif name == "generate_justification":
            justification = await cs4_client.generate_justification(
                company_id=arguments["company_id"],
                dimension=arguments["dimension"],
            )
            result = {
                "dimension":          arguments["dimension"],
                "score":              justification.score,
                "level":              justification.level,
                "level_name":         justification.level_name,
                "evidence_strength":  justification.evidence_strength,
                "rubric_criteria":    justification.rubric_criteria,
                "rubric_keywords":    justification.rubric_keywords,
                "generated_summary":  justification.generated_summary,
                "supporting_evidence": [
                    {
                        "source_type": e.source_type,
                        "content":     e.content[:300],
                        "confidence":  e.confidence,
                        "matched_keywords": e.matched_keywords,
                    }
                    for e in justification.supporting_evidence[:5]
                ],
                "gaps_identified": justification.gaps_identified,
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "project_ebitda_impact":
            projection = ebitda_calculator.project(
                company_id=arguments["company_id"],
                entry_score=arguments["entry_score"],
                exit_score=arguments["target_score"],
                h_r_score=arguments["h_r_score"],
            )
            result = {
                "company_id":       arguments["company_id"],
                "delta_air":        float(projection.delta_air),
                "scenarios": {
                    "conservative": f"{projection.conservative_pct:.2f}%",
                    "base":         f"{projection.base_pct:.2f}%",
                    "optimistic":   f"{projection.optimistic_pct:.2f}%",
                },
                "risk_adjusted":    f"{projection.risk_adjusted_pct:.2f}%",
                "requires_approval": projection.requires_approval,
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "run_gap_analysis":
            assessment = await cs3_client.get_assessment(arguments["company_id"])
            current_scores = {
                d.value: s.score
                for d, s in assessment.dimension_scores.items()
            }
            analysis = gap_analyzer.analyze(
                company_id=arguments["company_id"],
                current_scores=current_scores,
                target_org_air=arguments["target_org_air"],
            )
            return [TextContent(type="text", text=json.dumps(analysis, indent=2))]

        elif name == "get_portfolio_summary":
            companies = await portfolio_data_service.get_portfolio_view(
                arguments["fund_id"]
            )
            fund_air = (
                round(sum(c.org_air for c in companies) / len(companies), 1)
                if companies else 0.0
            )
            result = {
                "fund_id":       arguments["fund_id"],
                "fund_air":      fund_air,
                "company_count": len(companies),
                "companies": [
                    {
                        "ticker":            c.ticker,
                        "name":              c.name,
                        "sector":            c.sector,
                        "org_air":           c.org_air,
                        "vr_score":          c.vr_score,
                        "hr_score":          c.hr_score,
                        "delta_since_entry": c.delta_since_entry,
                    }
                    for c in companies
                ],
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except Exception as exc:
        logger.error("mcp_tool_error: tool=%s error=%s", name, str(exc), exc_info=True)
        return [TextContent(type="text", text=f"Error executing '{name}': {exc}")]


@mcp_server.list_resources()
async def list_resources() -> List[Resource]:
    """List addressable resources available to MCP clients."""
    return [
        Resource(
            uri="orgair://parameters/v2.0",
            name="Org-AI-R Scoring Parameters v2.0",
            description=(
                "Alpha, beta, and gamma constants used in the v2.0 EBITDA projection "
                "model and the V^R / H^R weighting formula."
            ),
        ),
        Resource(
            uri="orgair://sectors",
            name="Sector Baselines",
            description=(
                "Sector-specific H^R base rates and dimension weight overrides used "
                "when comparing a company's readiness against its peer group."
            ),
        ),
    ]


@mcp_server.read_resource()
async def read_resource(uri: str) -> str:
    """Serve resource content by URI."""
    if uri == "orgair://parameters/v2.0":
        params = {
            "version": "2.0",
            "alpha": 0.60,
            "beta":  0.12,
            "gamma_0": 0.0025,
            "gamma_1": 0.05,
            "gamma_2": 0.025,
            "gamma_3": 0.01,
        }
        return json.dumps(params, indent=2)

    elif uri == "orgair://sectors":
        sectors = {
            "technology": {
                "h_r_base":        85,
                "weight_talent":   0.18,
                "description":     "High talent concentration; strong digital baseline.",
            },
            "healthcare": {
                "h_r_base":        75,
                "weight_governance": 0.18,
                "description":     "Elevated governance weight due to regulatory exposure.",
            },
            "industrials": {
                "h_r_base":        65,
                "weight_data_infrastructure": 0.20,
                "description":     "Legacy infra modernisation is the primary value driver.",
            },
            "financial_services": {
                "h_r_base":        70,
                "weight_ai_governance": 0.20,
                "description":     "AI governance critical given model-risk regulations.",
            },
        }
        return json.dumps(sectors, indent=2)

    return "{}"


@mcp_server.list_prompts()
async def list_prompts() -> List[Prompt]:
    """List prompt templates available to MCP clients."""
    return [
        Prompt(
            name="due_diligence_assessment",
            description=(
                "Step-by-step due diligence workflow for a single portfolio company.  "
                "Guides the agent through scoring, evidence retrieval, gap analysis, "
                "and EBITDA projection."
            ),
            arguments=[
                {"name": "company_id", "required": True},
            ],
        ),
        Prompt(
            name="ic_meeting_prep",
            description=(
                "Investment Committee meeting preparation package.  Produces a "
                "structured agenda covering Org-AI-R highlights, top risks, and "
                "value-creation initiatives."
            ),
            arguments=[
                {"name": "company_id", "required": True},
            ],
        ),
    ]


@mcp_server.get_prompt()
async def get_prompt(name: str, arguments: dict) -> List[PromptMessage]:
    """Return the prompt messages for the requested template."""
    company_id = arguments.get("company_id", "<company_id>")

    if name == "due_diligence_assessment":
        return [
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=(
                    f"Perform a full Org-AI-R due diligence for **{company_id}**.\n\n"
                    "Follow these steps in order:\n"
                    "1. Call `calculate_org_air_score` to get the current Org-AI-R score "
                    "and dimension breakdown.\n"
                    "2. For every dimension scoring below 60, call `generate_justification` "
                    "to produce evidence-backed IC narrative.\n"
                    "3. Call `run_gap_analysis` with `target_org_air=75` to identify the "
                    "highest-priority improvement areas and investment required.\n"
                    "4. Call `project_ebitda_impact` using the current score as entry and "
                    "75 as the target to model value creation upside.\n"
                    "5. Summarise findings in a concise PE memo: score, top risks, "
                    "recommended initiatives, and projected EBITDA impact.\n"
                )),
            )
        ]

    elif name == "ic_meeting_prep":
        return [
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=(
                    f"Prepare an Investment Committee package for **{company_id}**.\n\n"
                    "Structure:\n"
                    "1. **Org-AI-R Snapshot** — current score, trend, and confidence "
                    "interval (use `calculate_org_air_score`).\n"
                    "2. **Dimension Highlights** — top 3 strengths and top 3 gaps "
                    "(use `generate_justification` for weak dimensions).\n"
                    "3. **Value-Creation Roadmap** — 100-day initiatives, owners, "
                    "and investment ($) (use `run_gap_analysis`).\n"
                    "4. **EBITDA Bridge** — entry vs. exit scenario with risk adjustment "
                    "(use `project_ebitda_impact`).\n"
                    "5. **Go / No-Go Recommendation** — one paragraph with rationale.\n"
                )),
            )
        ]

    return []


async def main() -> None:
    """Run the MCP server over stdio transport (default for Claude Desktop)."""
    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            mcp_server.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
