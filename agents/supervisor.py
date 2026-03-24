"""
Supervisor agent with HITL approval gates (Task 10.3).

Routing logic:
  supervisor → sec_analyst → scorer → evidence_agent → value_creator → complete
  At any point: if requires_approval + approval_status == "pending" → hitl_approval
  hitl_approval → supervisor (resume)
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import structlog
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from agents.state import DueDiligenceState
from agents.specialists import sec_agent, scoring_agent, evidence_agent, value_agent, talent_agent
from src.services.observability.metrics import (
    track_agent,
    HITL_APPROVALS,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Node functions
# ---------------------------------------------------------------------------

async def supervisor_node(state: DueDiligenceState) -> Dict[str, Any]:
    """Decide which agent runs next."""
    # HITL gate — intercept before any further agent runs
    if state.get("requires_approval") and state.get("approval_status") == "pending":
        return {"next_agent": "hitl_approval"}

    if not state.get("sec_analysis"):
        return {"next_agent": "sec_analyst"}
    elif not state.get("talent_analysis"):
        return {"next_agent": "talent_analyst"}
    elif not state.get("scoring_result"):
        return {"next_agent": "scorer"}
    elif not state.get("evidence_justifications"):
        return {"next_agent": "evidence_agent"}
    elif (
        not state.get("value_creation_plan")
        and state.get("assessment_type") != "screening"
    ):
        return {"next_agent": "value_creator"}
    else:
        return {"next_agent": "complete"}


@track_agent("sec_analyst")
async def sec_analyst_node(state: DueDiligenceState) -> Dict[str, Any]:
    logger.info("agent_start", agent="sec_analyst", company=state.get("company_id"))
    return await sec_agent.analyze(state)


async def talent_analyst_node(state: DueDiligenceState) -> Dict[str, Any]:
    return await talent_agent.analyze(state)


@track_agent("scorer")
async def scorer_node(state: DueDiligenceState) -> Dict[str, Any]:
    logger.info("agent_start", agent="scorer", company=state.get("company_id"))
    return await scoring_agent.calculate(state)


@track_agent("evidence_agent")
async def evidence_node(state: DueDiligenceState) -> Dict[str, Any]:
    logger.info("agent_start", agent="evidence_agent", company=state.get("company_id"))
    return await evidence_agent.justify(state)


@track_agent("value_creator")
async def value_creator_node(state: DueDiligenceState) -> Dict[str, Any]:
    logger.info("agent_start", agent="value_creator", company=state.get("company_id"))
    return await value_agent.plan(state)


@track_agent("hitl_approval")
async def hitl_approval_node(state: DueDiligenceState) -> Dict[str, Any]:
    """
    Human-in-the-Loop approval gate.

    HITL triggers when:
      - Org-AI-R score > 85 or < 40  (set by ScoringAgent)
      - EBITDA projection > 5%        (set by ValueCreationAgent)

    In production: send Slack/email and wait for a human response.
    For this exercise: auto-approve after logging the reason.
    """
    reason = state.get("approval_reason", "unspecified reason")
    company_id = state.get("company_id", "")

    logger.warning(
        "hitl_approval_required",
        company_id=company_id,
        reason=reason,
    )

    HITL_APPROVALS.labels(reason=reason[:64], decision="approved").inc()

    return {
        "approval_status": "approved",
        "approved_by": "exercise_auto_approve",
        "messages": [
            {
                "role": "system",
                "content": f"HITL approval granted for {company_id}: {reason}",
                "agent_name": "hitl",
                "timestamp": datetime.utcnow().isoformat(),
            }
        ],
    }


async def complete_node(state: DueDiligenceState) -> Dict[str, Any]:
    company_id = state.get("company_id", "")
    logger.info("workflow_complete", company_id=company_id)
    return {
        "completed_at": datetime.utcnow().isoformat(),
        "messages": [
            {
                "role": "assistant",
                "content": f"Due diligence complete for {company_id}.",
                "agent_name": "supervisor",
                "timestamp": datetime.utcnow().isoformat(),
            }
        ],
    }


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def create_due_diligence_graph():
    """Build and compile the LangGraph due-diligence workflow."""
    workflow = StateGraph(DueDiligenceState)

    # Register nodes
    workflow.add_node("supervisor",     supervisor_node)
    workflow.add_node("sec_analyst",    sec_analyst_node)
    workflow.add_node("talent_analyst", talent_analyst_node)
    workflow.add_node("scorer",         scorer_node)
    workflow.add_node("evidence_agent", evidence_node)
    workflow.add_node("value_creator",  value_creator_node)
    workflow.add_node("hitl_approval",  hitl_approval_node)
    workflow.add_node("complete",       complete_node)

    # Conditional routing from supervisor
    workflow.add_conditional_edges(
        "supervisor",
        lambda s: s["next_agent"],
        {
            "sec_analyst":    "sec_analyst",
            "talent_analyst": "talent_analyst",
            "scorer":         "scorer",
            "evidence_agent": "evidence_agent",
            "value_creator":  "value_creator",
            "hitl_approval":  "hitl_approval",
            "complete":       "complete",
        },
    )

    # All specialist agents return to supervisor after finishing
    for agent_node in ["sec_analyst", "scorer", "evidence_agent", "value_creator"]:
        workflow.add_edge(agent_node, "supervisor")
    workflow.add_edge("talent_analyst", "supervisor")

    # HITL returns to supervisor so routing can resume
    workflow.add_edge("hitl_approval", "supervisor")

    # Terminal node
    workflow.add_edge("complete", END)

    workflow.set_entry_point("supervisor")

    return workflow.compile(checkpointer=MemorySaver())


# Module-level graph instance (imported by exercises and tests)
dd_graph = create_due_diligence_graph()