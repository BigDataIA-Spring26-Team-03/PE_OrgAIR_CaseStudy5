"""Prometheus Metrics for MCP Server and LangGraph Agents (Task 10.6)."""
from __future__ import annotations

import time
from functools import wraps

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# MCP SERVER METRICS
# ---------------------------------------------------------------------------
MCP_TOOL_CALLS = Counter(
    "mcp_tool_calls_total",
    "Total MCP tool invocations",
    ["tool_name", "status"],
)
MCP_TOOL_DURATION = Histogram(
    "mcp_tool_duration_seconds",
    "MCP tool execution duration in seconds",
    ["tool_name"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# ---------------------------------------------------------------------------
# LANGGRAPH AGENT METRICS
# ---------------------------------------------------------------------------
AGENT_INVOCATIONS = Counter(
    "agent_invocations_total",
    "Total agent node invocations",
    ["agent_name", "status"],
)
AGENT_DURATION = Histogram(
    "agent_duration_seconds",
    "Agent node execution duration in seconds",
    ["agent_name"],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

# ---------------------------------------------------------------------------
# HITL METRICS
# ---------------------------------------------------------------------------
HITL_APPROVALS = Counter(
    "hitl_approvals_total",
    "HITL approval requests",
    ["reason", "decision"],
)

# ---------------------------------------------------------------------------
# CS1-CS4 INTEGRATION METRICS
# ---------------------------------------------------------------------------
CS_CLIENT_CALLS = Counter(
    "cs_client_calls_total",
    "Calls to CS1-CS4 services",
    ["service", "endpoint", "status"],
)

# ---------------------------------------------------------------------------
# PORTFOLIO METRICS (bonus observability)
# ---------------------------------------------------------------------------
PORTFOLIO_LOAD_DURATION = Histogram(
    "portfolio_load_duration_seconds",
    "Time to load and score the full portfolio",
    buckets=[1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)
FUND_AIR_GAUGE = Gauge(
    "fund_air_score",
    "Current Fund-AI-R score",
    ["fund_id"],
)


# ---------------------------------------------------------------------------
# DECORATORS
# ---------------------------------------------------------------------------

def track_mcp_tool(tool_name: str):
    """Decorator — track call count + duration for an MCP tool handler."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                MCP_TOOL_CALLS.labels(tool_name=tool_name, status="success").inc()
                return result
            except Exception:
                MCP_TOOL_CALLS.labels(tool_name=tool_name, status="error").inc()
                raise
            finally:
                MCP_TOOL_DURATION.labels(tool_name=tool_name).observe(
                    time.perf_counter() - start
                )
        return wrapper
    return decorator


def track_agent(agent_name: str):
    """Decorator — track call count + duration for a LangGraph agent node."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                AGENT_INVOCATIONS.labels(agent_name=agent_name, status="success").inc()
                return result
            except Exception:
                AGENT_INVOCATIONS.labels(agent_name=agent_name, status="error").inc()
                raise
            finally:
                AGENT_DURATION.labels(agent_name=agent_name).observe(
                    time.perf_counter() - start
                )
        return wrapper
    return decorator


def track_cs_client(service: str, endpoint: str):
    """Decorator — track calls to CS1-CS4 client methods."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                CS_CLIENT_CALLS.labels(
                    service=service, endpoint=endpoint, status="success"
                ).inc()
                return result
            except Exception:
                CS_CLIENT_CALLS.labels(
                    service=service, endpoint=endpoint, status="error"
                ).inc()
                raise
        return wrapper
    return decorator