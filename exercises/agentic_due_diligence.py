"""
Agentic Due Diligence — Complete Multi-Agent Workflow (Task 10.4).

Runs the full LangGraph due-diligence pipeline for a given company:
  SEC Analysis → Scoring → Evidence Justification → Value Creation
  with HITL approval gates when scores are outside normal range.

Usage:
    python exercises/agentic_due_diligence.py           # defaults to NVDA, full
    python exercises/agentic_due_diligence.py MSFT screening
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime

from agents.state import DueDiligenceState
from agents.supervisor import dd_graph


async def run_due_diligence(
    company_id: str,
    assessment_type: str = "full",
    requested_by: str = "analyst",
) -> DueDiligenceState:
    """
    Execute the full due-diligence workflow and return the final state.

    Parameters
    ----------
    company_id:      Company ticker, e.g. "NVDA".
    assessment_type: "screening" | "limited" | "full"
                     "screening" skips the value_creation step.
    requested_by:    Identifier of the requester (user / agent name).
    """
    initial_state: DueDiligenceState = {
        # ---- inputs ----
        "company_id":      company_id,
        "assessment_type": assessment_type,
        "requested_by":    requested_by,
        # ---- message log (append-only) ----
        "messages": [],
        # ---- agent output slots ----
        "sec_analysis":           None,
        "talent_analysis":        None,
        "scoring_result":         None,
        "evidence_justifications": None,
        "value_creation_plan":    None,
        # ---- workflow control ----
        "next_agent":      None,
        "requires_approval": False,
        "approval_reason": None,
        "approval_status": None,
        "approved_by":     None,
        # ---- metadata ----
        "started_at":   datetime.utcnow().isoformat(),
        "completed_at": None,
        "total_tokens": 0,
        "error":        None,
    }

    # Each run gets a unique thread so MemorySaver checkpoints don't collide
    config = {
        "configurable": {
            "thread_id": f"dd-{company_id}-{datetime.utcnow().isoformat()}"
        }
    }

    result: DueDiligenceState = await dd_graph.ainvoke(initial_state, config)
    return result


def _print_result(result: DueDiligenceState) -> None:
    """Pretty-print the final workflow state."""
    sep = "=" * 60
    print(sep)
    print("PE Org-AI-R: Agentic Due Diligence — Results")
    print(sep)

    company_id = result.get("company_id", "N/A")
    print(f"\nCompany       : {company_id}")
    print(f"Assessment    : {result.get('assessment_type', 'N/A')}")
    print(f"Started       : {result.get('started_at', 'N/A')}")
    print(f"Completed     : {result.get('completed_at', 'N/A')}")

    # Scoring
    scoring = result.get("scoring_result") or {}
    if scoring:
        print(f"\nOrg-AI-R      : {scoring.get('org_air', 'N/A'):.1f}")
        print(f"V^R           : {scoring.get('vr_score', 'N/A'):.1f}")
        print(f"H^R           : {scoring.get('hr_score', 'N/A'):.1f}")
        ci = scoring.get("confidence_interval", [])
        if ci:
            print(f"95% CI        : [{ci[0]:.1f}, {ci[1]:.1f}]")

    # HITL
    requires = result.get("requires_approval", False)
    status   = result.get("approval_status", "N/A")
    reason   = result.get("approval_reason", "")
    approved = result.get("approved_by", "N/A")
    print(f"\nHITL Required : {requires}")
    if requires:
        print(f"Reason        : {reason}")
        print(f"Status        : {status}")
        print(f"Approved by   : {approved}")

    # Value creation
    vc = result.get("value_creation_plan") or {}
    gap = vc.get("gap_analysis") or {}
    if gap:
        ebitda_pct = gap.get("projected_ebitda_pct", 0)
        print(f"\nEBITDA Impact : {ebitda_pct:.1f}%")

    # Message log
    messages = result.get("messages") or []
    print(f"\nAgent Messages ({len(messages)} total):")
    for msg in messages:
        agent = msg.get("agent_name", "?")
        content = msg.get("content", "")
        print(f"  [{agent}] {content}")

    print(f"\n{sep}")
    print("All data sourced from CS1-CS4 via ToolCaller (no mock data).")
    print(sep)


async def main() -> None:
    company_id     = sys.argv[1] if len(sys.argv) > 1 else "NVDA"
    assessment_type = sys.argv[2] if len(sys.argv) > 2 else "full"

    print(f"\nRunning due diligence for {company_id} ({assessment_type})...\n")
    result = await run_due_diligence(company_id, assessment_type)
    _print_result(result)


if __name__ == "__main__":
    asyncio.run(main())