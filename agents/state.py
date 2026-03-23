from typing import TypedDict, Annotated, List, Dict, Any, Optional, Literal
from datetime import datetime
import operator


class AgentMessage(TypedDict):
    role: Literal["user", "assistant", "system", "tool"]
    content: str
    agent_name: Optional[str]
    timestamp: str  # datetime.utcnow().isoformat()


class DueDiligenceState(TypedDict):
    # Input
    company_id: str
    assessment_type: Literal["screening", "limited", "full"]
    requested_by: str

    # Messages — append-only, never overwrite
    # operator.add means each agent APPENDS to this list, not replaces it
    messages: Annotated[List[AgentMessage], operator.add]

    # Agent output slots — each agent fills ONLY its own slot
    sec_analysis: Optional[Dict[str, Any]]
    talent_analysis: Optional[Dict[str, Any]]
    scoring_result: Optional[Dict[str, Any]]
    evidence_justifications: Optional[Dict[str, Any]]
    value_creation_plan: Optional[Dict[str, Any]]

    # Workflow control
    next_agent: Optional[str]
    requires_approval: bool
    approval_reason: Optional[str]
    approval_status: Optional[Literal["pending", "approved", "rejected"]]
    approved_by: Optional[str]

    # Metadata
    started_at: str
    completed_at: Optional[str]
    total_tokens: int
    error: Optional[str]
    
