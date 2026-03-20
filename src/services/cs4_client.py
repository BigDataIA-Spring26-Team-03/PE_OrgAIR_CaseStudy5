# src/services/cs4_client.py
"""
CS4 Client — thin wrapper around JustificationGenerator.
"""
from __future__ import annotations

from src.services.integration.cs3_client import Dimension
from src.services.justification.generator import JustificationGenerator, ScoreJustification


class CS4Client:
    """
    Async wrapper around JustificationGenerator.

    Matches the call convention used by CS2/CS3 clients so the MCP
    server can treat all four CS clients symmetrically.

    Usage
    -----
    cs4 = CS4Client()
    result = await cs4.generate_justification("NVDA", "talent")
    """

    async def generate_justification(
        self,
        company_id: str,
        dimension: str | Dimension,
    ) -> ScoreJustification:
        """
        Generate a RAG-backed justification for one scoring dimension.

        Parameters
        ----------
        company_id:
            Ticker symbol, e.g. "NVDA".
        dimension:
            Either the string value ("talent") or the Dimension enum
            member.  String input is converted automatically so callers
            don't need to import Dimension themselves.

        Returns
        -------
        ScoreJustification
            Includes score, rubric match, up to 5 cited evidence items,
            gaps to the next rubric level, and an LLM-generated summary.
        """
        # Accept both string ("talent") and enum (Dimension.TALENT)
        if isinstance(dimension, str):
            dimension = Dimension(dimension)

        generator = JustificationGenerator()
        return await generator.generate_justification(company_id, dimension)


# Module-level singleton — consistent with ebitda_calculator / gap_analyzer
# pattern.  MCP server imports this directly.
cs4_client = CS4Client()
