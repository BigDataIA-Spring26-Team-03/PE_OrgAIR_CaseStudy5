# src/services/cs3_client.py
"""
Alias shim — re-exports CS3 client and its public types from the
integration sub-package so MCP server and agent files can use the
short import path `from services.cs3_client import CS3Client`.
"""
from services.integration.cs3_client import (  # noqa: F401
    CS3Client,
    CompanyAssessment,
    Dimension,
    DimensionScore,
    RubricCriteria,
    ScoreLevel,
    HARDCODED_RUBRICS,
)
