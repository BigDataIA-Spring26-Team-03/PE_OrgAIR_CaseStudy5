# app/routers/board.py

from typing import List
from fastapi import APIRouter, HTTPException, Query
import json
from datetime import datetime
import uuid as uuid_lib
import sys
from pathlib import Path

# Ensure src/ is on the path so scoring.board_analyzer can be imported
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from app.models.board import (
    BoardGovernanceSignalResponse,
    BoardGovernanceSignalSummary,
    BoardGovernanceListResponse,
    BoardMemberResponse,
)
from app.services.snowflake import db

router = APIRouter(prefix="/board-governance", tags=["Board Governance"])


# ============================================================================
# LIST ALL SIGNALS
# ============================================================================

@router.get("", response_model=BoardGovernanceListResponse)
async def list_all_signals(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """List ALL board governance signals across all companies."""
    offset = (page - 1) * limit

    count_result = db.execute_query(
        "SELECT COUNT(*) as total FROM board_governance_signals"
    )
    total = (
        count_result[0].get("TOTAL") or count_result[0].get("total")
        if count_result
        else 0
    )

    query = """
        SELECT * FROM board_governance_signals
        ORDER BY created_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    results = db.execute_query(query, {"limit": limit, "offset": offset})

    items = [
        BoardGovernanceSignalSummary(
            id=row.get("ID") or row.get("id"),
            company_id=row.get("COMPANY_ID") or row.get("company_id"),
            ticker=row.get("TICKER") or row.get("ticker"),
            governance_score=row.get("GOVERNANCE_SCORE") or row.get("governance_score"),
            confidence=row.get("CONFIDENCE") or row.get("confidence"),
            created_at=row.get("CREATED_AT") or row.get("created_at"),
        )
        for row in results
    ]

    pages = (total + limit - 1) // limit if total > 0 else 0

    return BoardGovernanceListResponse(
        items=items, total=total, page=page, limit=limit, pages=pages
    )


# ============================================================================
# SIGNALS BY TICKER
# ============================================================================

@router.get("/ticker/{ticker}", response_model=List[BoardGovernanceSignalResponse])
async def list_signals_by_ticker(ticker: str):
    """List board governance signals for a specific company."""
    query = """
        SELECT * FROM board_governance_signals
        WHERE ticker = %(ticker)s
        ORDER BY created_at DESC
    """
    results = db.execute_query(query, {"ticker": ticker.upper()})

    if not results:
        return []

    signals = []
    for row in results:
        ai_experts = json.loads(
            row.get("AI_EXPERTS") or row.get("ai_experts") or "[]"
        )
        evidence = json.loads(
            row.get("EVIDENCE") or row.get("evidence") or "[]"
        )

        signals.append(
            BoardGovernanceSignalResponse(
                id=row.get("ID") or row.get("id"),
                company_id=row.get("COMPANY_ID") or row.get("company_id"),
                ticker=row.get("TICKER") or row.get("ticker"),
                governance_score=row.get("GOVERNANCE_SCORE") or row.get("governance_score"),
                has_tech_committee=row.get("HAS_TECH_COMMITTEE") or row.get("has_tech_committee") or False,
                has_ai_expertise=row.get("HAS_AI_EXPERTISE") or row.get("has_ai_expertise") or False,
                has_data_officer=row.get("HAS_DATA_OFFICER") or row.get("has_data_officer") or False,
                has_independent_majority=row.get("HAS_INDEPENDENT_MAJORITY") or row.get("has_independent_majority") or False,
                has_risk_tech_oversight=row.get("HAS_RISK_TECH_OVERSIGHT") or row.get("has_risk_tech_oversight") or False,
                has_ai_strategy=row.get("HAS_AI_STRATEGY") or row.get("has_ai_strategy") or False,
                ai_experts=ai_experts,
                evidence=evidence,
                confidence=row.get("CONFIDENCE") or row.get("confidence"),
                created_at=row.get("CREATED_AT") or row.get("created_at"),
                updated_at=row.get("UPDATED_AT") or row.get("updated_at"),
            )
        )

    return signals


# ============================================================================
# BOARD MEMBERS BY TICKER
# ============================================================================

@router.get("/members/ticker/{ticker}", response_model=List[BoardMemberResponse])
async def list_members_by_ticker(ticker: str):
    """List board members for a specific company."""
    query = """
        SELECT bm.*
        FROM board_members bm
        JOIN board_governance_signals bgs ON bm.governance_signal_id = bgs.id
        WHERE bgs.ticker = %(ticker)s
        ORDER BY bm.name
    """
    results = db.execute_query(query, {"ticker": ticker.upper()})

    members = []
    for row in results:
        committees = json.loads(
            row.get("COMMITTEES") or row.get("committees") or "[]"
        )
        members.append(
            BoardMemberResponse(
                id=row.get("ID") or row.get("id"),
                company_id=row.get("COMPANY_ID") or row.get("company_id"),
                governance_signal_id=row.get("GOVERNANCE_SIGNAL_ID") or row.get("governance_signal_id"),
                name=row.get("NAME") or row.get("name"),
                title=row.get("TITLE") or row.get("title"),
                committees=committees,
                bio=row.get("BIO") or row.get("bio"),
                is_independent=row.get("IS_INDEPENDENT") or row.get("is_independent") or False,
                tenure_years=row.get("TENURE_YEARS") or row.get("tenure_years") or 0,
                created_at=row.get("CREATED_AT") or row.get("created_at"),
            )
        )

    return members


# ============================================================================
# COLLECT (SINGLE TICKER)
# ============================================================================

@router.post("/collect/{ticker}", status_code=201)
async def collect_by_ticker(
    ticker: str,
    use_cache: bool = Query(True, description="Use cached data if available"),
):
    """
    Collect board composition data for a company.

    Scrapes SEC EDGAR proxy, runs BoardCompositionAnalyzer, persists to Snowflake.
    """
    # Resolve company_id
    company_query = "SELECT id FROM companies WHERE ticker = %(ticker)s"
    company_result = db.execute_query(company_query, {"ticker": ticker.upper()})

    if not company_result:
        raise HTTPException(
            status_code=404,
            detail=f"Company {ticker} not found. Add company first.",
        )

    company_id = company_result[0].get("ID") or company_result[0].get("id")

    try:
        from app.pipelines.board_collector import BoardCompositionCollector
        from app.pipelines.board_chunker import chunk_proxy_text
        from app.pipelines.board_llm_extractor import (
            extract_from_chunks,
            merge_extractions,
            quality_check,
        )
        from src.scoring.board_analyzer import (
        BoardCompositionAnalyzer,
        BoardMember,
        )

        # 1. Collect raw text from EDGAR
        collector = BoardCompositionCollector()
        raw = collector.collect_board_data(ticker.upper(), use_cache=use_cache)

        # If no cached members, run LLM pipeline
        if not raw.get("members"):
            raw_text_data = collector.collect_raw_text(ticker.upper())
            raw_text = raw_text_data.get("raw_text", "")
            if not raw_text:
                raise HTTPException(status_code=422, detail="No proxy text available")

            chunks = chunk_proxy_text(raw_text)
            if not chunks:
                raise HTTPException(status_code=422, detail="No usable chunks from proxy")

            extractions = extract_from_chunks(chunks, ticker.upper(), use_cache=use_cache)
            if not extractions:
                raise HTTPException(status_code=422, detail="LLM extraction produced no results")

            members_raw = merge_extractions(extractions)
            raw["members"] = members_raw
            collector.cache_with_members(ticker.upper(), raw)
        else:
            members_raw = raw["members"]

        ok, reason = quality_check(members_raw)
        if not ok:
            raise HTTPException(status_code=422, detail=f"Quality gate failed: {reason}")

        # 2. Convert member dicts to BoardMember dataclasses
        members = [
            BoardMember(
                name=m["name"],
                title=m.get("title", "Director"),
                committees=m.get("committees", []),
                bio=m.get("bio", ""),
                is_independent=m.get("is_independent", False),
                tenure_years=m.get("tenure_years", 0.0),
            )
            for m in members_raw
        ]

        # 3. Analyze with BoardCompositionAnalyzer
        analyzer = BoardCompositionAnalyzer()
        signal = analyzer.analyze_board(
            company_id=str(company_id),
            ticker=ticker.upper(),
            members=members,
            committees=raw.get("committees", []),
            strategy_text=raw.get("strategy_text", ""),
        )

        # 4. Persist governance signal
        signal_id = str(uuid_lib.uuid4())

        insert_signal = """
            INSERT INTO board_governance_signals (
                id, company_id, ticker,
                governance_score, has_tech_committee, has_ai_expertise,
                has_data_officer, has_independent_majority,
                has_risk_tech_oversight, has_ai_strategy,
                ai_experts, evidence, confidence, created_at
            ) VALUES (
                %(id)s, %(company_id)s, %(ticker)s,
                %(governance_score)s, %(has_tech_committee)s, %(has_ai_expertise)s,
                %(has_data_officer)s, %(has_independent_majority)s,
                %(has_risk_tech_oversight)s, %(has_ai_strategy)s,
                %(ai_experts)s, %(evidence)s, %(confidence)s, %(created_at)s
            )
        """

        db.execute_update(
            insert_signal,
            {
                "id": signal_id,
                "company_id": str(company_id),
                "ticker": ticker.upper(),
                "governance_score": float(signal.governance_score),
                "has_tech_committee": signal.has_tech_committee,
                "has_ai_expertise": signal.has_ai_expertise,
                "has_data_officer": signal.has_data_officer,
                "has_independent_majority": signal.has_independent_majority,
                "has_risk_tech_oversight": signal.has_risk_tech_oversight,
                "has_ai_strategy": signal.has_ai_strategy,
                "ai_experts": json.dumps(signal.ai_experts),
                "evidence": json.dumps(signal.evidence),
                "confidence": float(signal.confidence),
                "created_at": datetime.utcnow(),
            },
        )

        # 5. Persist individual board members
        insert_member = """
            INSERT INTO board_members (
                id, company_id, governance_signal_id,
                name, title, committees, bio,
                is_independent, tenure_years, created_at
            ) VALUES (
                %(id)s, %(company_id)s, %(governance_signal_id)s,
                %(name)s, %(title)s, %(committees)s, %(bio)s,
                %(is_independent)s, %(tenure_years)s, %(created_at)s
            )
        """

        for m in members:
            db.execute_update(
                insert_member,
                {
                    "id": str(uuid_lib.uuid4()),
                    "company_id": str(company_id),
                    "governance_signal_id": signal_id,
                    "name": m.name,
                    "title": m.title,
                    "committees": json.dumps(m.committees),
                    "bio": m.bio[:2000] if m.bio else "",
                    "is_independent": m.is_independent,
                    "tenure_years": m.tenure_years,
                    "created_at": datetime.utcnow(),
                },
            )

        return {
            "message": f"Board governance signal collected for {ticker}",
            "ticker": ticker.upper(),
            "signal_id": signal_id,
            "governance_score": float(signal.governance_score),
            "member_count": len(members),
            "confidence": float(signal.confidence),
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Collection failed: {str(e)}"
        )


# ============================================================================
# COLLECT ALL
# ============================================================================

@router.post("/collect-all")
async def collect_all_companies(
    use_cache: bool = Query(True, description="Use cached data"),
):
    """Collect board governance signals for ALL companies."""
    companies = db.execute_query(
        "SELECT id, ticker FROM companies ORDER BY ticker"
    )

    if not companies:
        raise HTTPException(status_code=404, detail="No companies found")

    collected = []
    failed = []

    for company in companies:
        ticker = company.get("TICKER") or company.get("ticker")
        try:
            result = await collect_by_ticker.__wrapped__(ticker, use_cache=use_cache)
            collected.append(result)
        except Exception as e:
            failed.append({"ticker": ticker, "error": str(e)})

    return {
        "status": "completed",
        "total_companies": len(companies),
        "collected": len(collected),
        "failed": len(failed),
        "collected_tickers": [c["ticker"] for c in collected],
        "failed_tickers": [f["ticker"] for f in failed],
    }
