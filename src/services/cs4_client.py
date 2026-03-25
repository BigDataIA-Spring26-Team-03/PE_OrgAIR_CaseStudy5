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

    def __init__(self) -> None:
        # Create once — avoids reopening ChromaDB on every call (especially
        # during IC prep where all 7 dimensions are generated back-to-back).
        self._generator = JustificationGenerator()

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
        if isinstance(dimension, str):
            dimension = Dimension(dimension)

        return await self._generator.generate_justification(company_id, dimension)


# Lazy singleton — delays SentenceTransformer + ChromaDB initialization
# until the first generate_justification call so MCP server startup is fast.
class _LazyCS4Client:
    """Proxy that creates CS4Client on first use, not at import time."""

    def __init__(self) -> None:
        self._client: CS4Client | None = None

    def _get(self) -> CS4Client:
        if self._client is None:
            self._client = CS4Client()
        return self._client

    async def generate_justification(
        self,
        company_id: str,
        dimension: str | Dimension,
    ) -> ScoreJustification:
        return await self._get().generate_justification(company_id, dimension)


cs4_client = _LazyCS4Client()
