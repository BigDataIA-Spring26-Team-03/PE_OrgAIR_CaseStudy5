"""
src/services/on_demand_scoring.py

"""
from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from services.integration.cs3_client import CompanyAssessment

logger = logging.getLogger(__name__)

# Thread pool for running synchronous collectors without blocking the event loop
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ods")

# Per-ticker locks: prevents two simultaneous scoring runs for the same ticker
_scoring_locks: Dict[str, asyncio.Lock] = {}


# ---------------------------------------------------------------------------
# Public service class
# ---------------------------------------------------------------------------

class OnDemandScoringService:
    """
    Returns a CompanyAssessment for any ticker.
    """

    async def get_or_score_company(
        self,
        ticker: str,
        force_refresh: bool = False,
    ) -> "CompanyAssessment":
        ticker = ticker.upper().strip()

        # ── fast path ────────────────────────────────────────────────────
        if not force_refresh:
            assessment = await self._try_fetch_assessment(ticker)
            if assessment is not None:
                return assessment

        # ── acquire per-ticker lock (prevents parallel duplicate runs) ──
        if ticker not in _scoring_locks:
            _scoring_locks[ticker] = asyncio.Lock()

        async with _scoring_locks[ticker]:
            # Re-check after acquiring lock: another coroutine may have
            # just finished scoring this ticker while we were waiting
            if not force_refresh:
                assessment = await self._try_fetch_assessment(ticker)
                if assessment is not None:
                    return assessment

            # ── slow path: full pipeline ─────────────────────────────
            t0 = time.monotonic()
            logger.info("on_demand_scoring_started ticker=%s force=%s", ticker, force_refresh)

            # Step 1: Detect sector + company name via yfinance
            sector       = await asyncio.get_event_loop().run_in_executor(
                _executor, lambda: _detect_sector(ticker)
            )
            company_name = await asyncio.get_event_loop().run_in_executor(
                _executor, lambda: _detect_company_name(ticker)
            )
            logger.info(
                "company_resolved ticker=%s name=%s sector=%s",
                ticker, company_name, sector
            )

            # Step 2: Register company in Snowflake → get company_id
            # This must happen BEFORE signals collection so external_signals
            # can be attributed to the correct company_id.
            company_data = await asyncio.get_event_loop().run_in_executor(
                _executor, lambda: _register_company(ticker, sector)
            )
            company_id = company_data.get("id", ticker)
            logger.info(
                "company_registered ticker=%s company_id=%s",
                ticker, company_id
            )

            # Step 2.5: Collect board BEFORE signals — leadership signal derives from
            # board_governance_signals, so we must populate it first.
            await self._collect_board_for_signals(ticker)

            # Step 3: Collect and PERSIST evidence to Snowflake
            # Two parallel tracks — each writes to Snowflake tables so
            # score_company() reads real data:
            #   - SEC   → documents_sec + document_chunks_sec
            #   - Signals (job/patent/tech/leadership) → external_signals
            # Glassdoor + Board are triggered by score_company() itself.
            try:
                await asyncio.wait_for(
                    self._collect_all_evidence(ticker, company_id, company_name),
                    timeout=300.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "evidence_collection_timed_out ticker=%s — continuing to score "
                    "with whatever data was persisted so far",
                    ticker,
                )

            # Step 4: Index CS2 evidence into ChromaDB for generate_justification
            await self._index_evidence(ticker)

            # Step 5: Run CS3 scoring pipeline (sync → thread executor)
            # score_company() reads from Snowflake tables populated above,
            # and also triggers Glassdoor + Board collection if needed.
            await asyncio.get_event_loop().run_in_executor(
                _executor,
                lambda: _run_scoring(ticker, sector),
            )

            elapsed = time.monotonic() - t0
            logger.info(
                "on_demand_scoring_complete ticker=%s elapsed=%.1fs",
                ticker, elapsed,
            )

            # Step 6: Fetch and return freshly computed assessment
            assessment = await self._try_fetch_assessment(ticker)
            if assessment is None:
                raise RuntimeError(
                    f"Scoring pipeline completed for '{ticker}' but the assessment "
                    "still cannot be read from CS3. Check CS3 API logs."
                )

            # Step 7: Record snapshot in assessment_history for trend tracking
            await self._record_assessment_history(ticker)

            return assessment

    # ── helpers ─────────────────────────────────────────────────────────

    async def _try_fetch_assessment(self, ticker: str) -> Optional["CompanyAssessment"]:
        """Return CompanyAssessment if CS3 has it, else None (no exception)."""
        try:
            from src.services.integration.cs3_client import CS3Client
            async with CS3Client() as cs3:
                return await cs3.get_assessment(ticker)
        except Exception:
            return None

    async def _record_assessment_history(self, ticker: str) -> None:
        """
        Persist a snapshot to assessment_history for trend tracking.
        Best-effort: if Snowflake is down or history service fails, log and continue.
        """
        try:
            from src.services.integration.cs1_client import CS1Client
            from src.services.integration.cs3_client import CS3Client
            from src.services.tracking.assessment_history import create_history_service

            async with CS1Client() as cs1, CS3Client() as cs3:
                svc = create_history_service(cs1, cs3)
                await svc.record_assessment(
                    company_id=ticker,
                    assessor_id="on_demand_scoring",
                    assessment_type="full",
                )
        except Exception as exc:
            logger.warning(
                "assessment_history_record_failed ticker=%s error=%s — "
                "trend data may be incomplete",
                ticker, exc,
            )

    async def _collect_board_for_signals(self, ticker: str) -> None:
        """
        Collect board governance data before signals collection.
        The signals track derives leadership from board_governance_signals;
        without this, leadership is empty on first run for new tickers.
        Best-effort: log and continue if collection fails.
        """
        try:
            # collect_board is sync (requests) — run in executor
            def _collect() -> None:
                from src.scoring.integration_service import ScoringIntegrationService
                svc = ScoringIntegrationService()
                svc.collect_board(ticker)

            await asyncio.get_event_loop().run_in_executor(_executor, _collect)
            logger.info("board_collected_for_signals ticker=%s", ticker)
        except Exception as exc:
            logger.warning(
                "board_collect_for_signals_failed ticker=%s error=%s — "
                "leadership signal may be empty",
                ticker, exc,
            )

    # ── parallel evidence collection ─────────────────────────────────────

    async def _collect_all_evidence(
        self, ticker: str, company_id: str, company_name: str
    ) -> None:
        """
        Persist evidence to Snowflake in two parallel tracks.

        Track 1 — SEC 10-K filing:
            SECPipeline.run() downloads from EDGAR and inserts into
            documents_sec + document_chunks_sec tables.
            score_company().fetch_sec_evidence() then reads these via
            GET /api/v1/documents?ticker={ticker}&status=chunked.

        Track 2 — External signals (job / patent / tech / leadership):
            run_comprehensive_collection_task() scrapes all sources and
            calls db.insert_external_signals() → external_signals table.
            score_company().fetch_cs2_evidence() reads these via
            GET /api/v1/signals/company/{ticker}.

        Glassdoor + Board are NOT pre-collected here because
        score_company() already calls:
            collect_glassdoor() → POST /api/v1/culture-signals/collect/{ticker}
            collect_board()     → POST /api/v1/board-governance/collect/{ticker}
        """
        tasks = {
            "sec":     self._run_sec_collection(ticker),
            "signals": self._run_signals_collection(ticker, company_id, company_name),
        }
        labels  = list(tasks.keys())
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for label, result in zip(labels, results):
            if isinstance(result, Exception):
                logger.warning(
                    "%s_collection_failed ticker=%s error=%s",
                    label, ticker, result,
                )
            else:
                logger.info("%s_collection_complete ticker=%s", label, ticker)

    # ── SEC track ────────────────────────────────────────────────────────

    async def _run_sec_collection(self, ticker: str) -> None:
        """
        Download + parse + chunk 10-K SEC filings via SECPipeline.
        Stores to documents_sec + document_chunks_sec tables in Snowflake.
        score_company().fetch_sec_evidence() reads these back.
        """
        from app.pipelines.sec_pipeline import SECPipeline

        def _collect() -> None:
            result = SECPipeline().run(ticker)
            logger.info(
                "sec_pipeline_done ticker=%s docs=%s chunks=%s",
                ticker,
                result.get("docs_processed", "?"),
                result.get("chunks_created", "?"),
            )

        await asyncio.get_event_loop().run_in_executor(_executor, _collect)

    # ── Signals track ────────────────────────────────────────────────────

    async def _run_signals_collection(
        self, ticker: str, company_id: str, company_name: str
    ) -> None:
        """
        Run comprehensive job / patent / tech / leadership collection.

        """
        from app.routers.signals import run_comprehensive_collection_task

        await run_comprehensive_collection_task(
            company_id=company_id,
            company_name=company_name,
            ticker=ticker,
            years=5,
            job_location="United States",
        )

    # ── ChromaDB indexing ────────────────────────────────────────────────

    async def _index_evidence(self, ticker: str) -> None:
        """
        Fetch CS2 evidence for ticker and index it into ChromaDB + BM25
        so generate_justification can retrieve supporting evidence.
        """
        try:
            from services.integration.cs2_client import CS2Client
            from services.retrieval.hybrid import HybridRetriever
            from services.retrieval.dimension_mapper import DimensionMapper

            retriever = HybridRetriever()
            mapper    = DimensionMapper()

            async with CS2Client() as cs2:
                evidence_list = await cs2.get_evidence(company_id=ticker)

            if not evidence_list:
                logger.info("no_evidence_to_index ticker=%s", ticker)
                return

            # index_evidence is synchronous — run in thread pool
            await asyncio.get_event_loop().run_in_executor(
                _executor,
                lambda: retriever.index_evidence(evidence_list, mapper),
            )
            logger.info("evidence_indexed ticker=%s count=%d", ticker, len(evidence_list))

        except Exception as exc:
            logger.warning(
                "evidence_indexing_failed ticker=%s error=%s — "
                "generate_justification may return weak evidence",
                ticker, exc,
            )

    async def ensure_evidence_indexed(self, ticker: str) -> None:
        """
        Public wrapper: ensure CS2 evidence for ticker is indexed in ChromaDB.
        Safe to call multiple times — ChromaDB upserts (no duplicates).
        Called by generate_justification before CS4 RAG search.
        """
        await self._index_evidence(ticker)


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no state)
# ---------------------------------------------------------------------------

def _detect_sector(ticker: str) -> str:
    """Look up sector via yfinance. Falls back to 'technology'."""
    _SECTOR_MAP = {
        "Technology":             "technology",
        "Financial Services":     "financial_services",
        "Consumer Defensive":     "retail",
        "Consumer Cyclical":      "retail",
        "Industrials":            "industrials",
        "Healthcare":             "healthcare",
        "Energy":                 "energy",
        "Communication Services": "technology",
        "Basic Materials":        "industrials",
        "Real Estate":            "technology",
        "Utilities":              "technology",
    }
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        raw  = info.get("sector") or "technology"
        return _SECTOR_MAP.get(raw, raw.lower().replace(" ", "_"))
    except Exception:
        return "technology"


def _detect_company_name(ticker: str) -> str:
    """Return the company's full name via yfinance, or ticker as fallback."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return (
            info.get("longName")
            or info.get("shortName")
            or ticker
        )
    except Exception:
        return ticker


def _register_company(ticker: str, sector: str) -> dict:
    """
    Register company in Snowflake and return its metadata dict.
    """
    from src.scoring.integration_service import ScoringIntegrationService
    svc = ScoringIntegrationService()
    return svc.fetch_company(ticker)


def _run_scoring(ticker: str, sector: str) -> None:
    """
    Run ScoringIntegrationService.score_company() synchronously.
    """
    from src.scoring.integration_service import ScoringIntegrationService

    svc    = ScoringIntegrationService()
    result = svc.score_company(ticker, sector)
    logger.info(
        "scoring_pipeline_complete ticker=%s org_air=%.1f evidence_count=%d",
        ticker,
        result.get("final_score", 0.0),
        result.get("evidence_count", 0),
    )
