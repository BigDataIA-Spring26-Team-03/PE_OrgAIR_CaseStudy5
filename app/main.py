from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.routers.health import router as health_router
from app.routers.companies import router as companies_router
from app.routers.assessments import router as assessments_router
from app.routers.dimension import router as dimension_router, scores_router
from app.routers.documents import router as documents_router
from app.routers.signals import router as signals_router
from app.routers.culture import router as culture_router  # ← ADD THIS
from app.routers.board import router as board_router
from app.routers.scoring import router as scoring_router
from app.routers.search import router as search_router
from app.routers.justification import router as justification_router
from app.routers.evidence import router as evidence_router
from app.routers.analyst_notes import router as analyst_notes_router
from app.routers.assessment_history import router as assessment_history_router
from app.core.deps import get_retriever


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Eagerly init the shared retriever so BM25 is reloaded from ChromaDB
    # before the first request arrives.
    retriever = get_retriever()

    # ---------------------------------------------------------------------
    # CS4: Auto-index ChromaDB if empty (Option A)
    # ---------------------------------------------------------------------
    try:
        existing = int(retriever.vector_store.collection.count() or 0)
    except Exception:
        existing = 0

    auto_index_enabled = getattr(settings, "AUTO_INDEX_CHROMA_ON_STARTUP", False)
    if existing == 0 and auto_index_enabled:
        import structlog
        from datetime import datetime

        from app.services.snowflake import SnowflakeService
        from app.routers.evidence import get_evidence
        from src.services.retrieval.dimension_mapper import DimensionMapper
        from src.services.integration.cs2_client import CS2Evidence, SourceType, SignalCategory

        log = structlog.get_logger().bind(component="cs4_startup_index")
        log.info("chroma_empty_starting_index")

        db = SnowflakeService()
        tickers: list[str] = []
        try:
            rows = db.execute_query(
                """
                SELECT ticker
                FROM companies
                WHERE is_deleted = FALSE AND ticker IS NOT NULL
                ORDER BY ticker
                """
            )
            tickers = [str(r["ticker"]).strip().upper() for r in rows if r.get("ticker")]
        except Exception as exc:
            log.warning("ticker_fetch_failed", error=str(exc))

        evidence_list: list[CS2Evidence] = []
        for t in tickers:
            try:
                raw_rows = get_evidence(
                    company_id=t,
                    indexed=None,
                    min_confidence=0.0,
                    since=None,
                )
            except Exception as exc:
                log.warning("evidence_fetch_failed", ticker=t, error=str(exc))
                continue

            for r in raw_rows or []:
                content = (r.get("content") or "").strip()
                if not content:
                    continue
                st = SourceType.from_raw(str(r.get("source_type") or "")) or SourceType.SEC_10K_ITEM_1
                sc = SignalCategory.from_raw(str(r.get("signal_category") or "")) or SignalCategory.DIGITAL_PRESENCE
                try:
                    evidence_list.append(
                        CS2Evidence(
                            evidence_id=str(r.get("evidence_id")),
                            company_id=str(r.get("company_id") or t),
                            source_type=st,
                            signal_category=sc,
                            content=content,
                            extracted_at=datetime.now(),
                            confidence=float(r.get("confidence") or 0.5),
                            fiscal_year=r.get("fiscal_year"),
                            source_url=r.get("source_url"),
                        )
                    )
                except Exception:
                    # Skip malformed rows; indexing should be best-effort
                    continue

        if evidence_list:
            mapper = DimensionMapper()
            try:
                n = retriever.index_evidence(evidence_list, mapper)
                # Ensure BM25 reflects everything persisted in ChromaDB
                retriever._reload_from_chroma()
                log.info("chroma_index_complete", tickers=len(tickers), indexed=n)
            except Exception as exc:
                log.error("chroma_index_failed", error=str(exc))
        else:
            log.warning("no_evidence_to_index", tickers=len(tickers))
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        debug=settings.DEBUG,
        lifespan=lifespan,
    )

    app.include_router(health_router)
    app.include_router(companies_router, prefix="/api/v1")
    app.include_router(assessments_router, prefix="/api/v1")
    app.include_router(dimension_router, prefix="/api/v1")
    app.include_router(scores_router, prefix="/api/v1")
    app.include_router(documents_router)
    app.include_router(signals_router, prefix="/api/v1")
    app.include_router(culture_router, prefix="/api/v1")  # ← ADD THIS
    app.include_router(board_router, prefix="/api/v1")
    app.include_router(scoring_router, prefix="/api/v1")
    app.include_router(search_router, prefix="/api/v1")
    app.include_router(justification_router, prefix="/api/v1")
    app.include_router(evidence_router, prefix="/api/v1")
    app.include_router(analyst_notes_router, prefix="/api/v1")
    app.include_router(assessment_history_router, prefix="/api/v1")
    return app


app = create_app()