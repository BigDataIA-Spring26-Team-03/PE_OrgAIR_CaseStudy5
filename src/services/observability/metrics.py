from prometheus_client import Counter, Histogram
from functools import wraps
import time

MCP_TOOL_CALLS = Counter(
    "mcp_tool_calls_total",
    "Total MCP tool invocations",
    ["tool_name", "status"]
)
MCP_TOOL_DURATION = Histogram(
    "mcp_tool_duration_seconds",
    "MCP tool execution time in seconds",
    ["tool_name"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)
AGENT_INVOCATIONS = Counter(
    "agent_invocations_total",
    "Total agent invocations",
    ["agent_name", "status"]
)
AGENT_DURATION = Histogram(
    "agent_duration_seconds",
    "Agent execution time in seconds",
    ["agent_name"],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
)
HITL_APPROVALS = Counter(
    "hitl_approvals_total",
    "HITL approval events",
    ["reason", "decision"]
)
CS_CLIENT_CALLS = Counter(
    "cs_client_calls_total",
    "Calls into CS1-CS4 services",
    ["service", "method", "status"]
)


def track_mcp_tool(tool_name: str):
    """
    Decorator factory for async MCP tool handlers.
    - Times execution using time.perf_counter()
    - On success: increments MCP_TOOL_CALLS with status="success"
    - On exception: increments MCP_TOOL_CALLS with status="error", then re-raises
    - Always: observes MCP_TOOL_DURATION with elapsed time
    """
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
    """
    Same pattern as track_mcp_tool but uses AGENT_INVOCATIONS
    and AGENT_DURATION metrics instead.
    """
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


def track_cs_client(service: str, method: str):
    """
    Same pattern but uses CS_CLIENT_CALLS metric.
    Labels: service, method, status ("success" or "error").
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                CS_CLIENT_CALLS.labels(
                    service=service, method=method, status="success"
                ).inc()
                return result
            except Exception:
                CS_CLIENT_CALLS.labels(
                    service=service, method=method, status="error"
                ).inc()
                raise
        return wrapper
    return decorator
