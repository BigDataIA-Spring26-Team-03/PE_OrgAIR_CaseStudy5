# app/routers/scoring.py
"""
CS3 Scoring Endpoints - Org-AI-R Integration Service
"""

import asyncio
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pathlib import Path
import json
from typing import Dict, Any, Optional
from datetime import datetime
import structlog

logger = structlog.get_logger()

router = APIRouter(prefix="/scoring", tags=["CS3 Scoring"])

# Store for background task status
scoring_status = {}


# ============================================================================
# BACKGROUND SCORING TASK
# ============================================================================

def run_scoring_task(ticker: str, sector: str, task_id: str):
    """
    Run scoring in background thread.
    This avoids timeout issues.
    """
    
    try:
        logger.info("background_scoring_started", ticker=ticker, task_id=task_id)
        
        # Update status
        scoring_status[task_id] = {
            "status": "running",
            "ticker": ticker,
            "started_at": datetime.now().isoformat(),
            "progress": "Fetching data..."
        }
        
        # Import and run integration service
        from src.scoring.integration_service import ScoringIntegrationService
        
        service = ScoringIntegrationService(api_base_url="http://localhost:8000")
        
        # Run scoring (this takes 2-5 minutes)
        result = service.score_company(ticker=ticker.upper(), sector=sector)
        
        # Update status with results
        scoring_status[task_id] = {
            "status": "completed",
            "ticker": ticker,
            "started_at": scoring_status[task_id]["started_at"],
            "completed_at": datetime.now().isoformat(),
            "final_score": result['final_score'],
            "vr_score": result['vr_score'],
            "hr_score": result['hr_score'],
            "synergy_score": result['synergy_score'],
            "confidence": result['confidence'],
            "result_file": result['result_json_path']
        }
        
        logger.info(
            "background_scoring_completed",
            ticker=ticker,
            task_id=task_id,
            final_score=result['final_score']
        )
        
    except Exception as e:
        logger.error("background_scoring_failed", ticker=ticker, error=str(e))
        
        scoring_status[task_id] = {
            "status": "failed",
            "ticker": ticker,
            "error": str(e),
            "started_at": scoring_status.get(task_id, {}).get("started_at"),
            "failed_at": datetime.now().isoformat()
        }


# ============================================================================
# SCORE COMPANY (Background)
# ============================================================================

@router.post("/score/{ticker}")
async def score_company(
    ticker: str,
    sector: str = Query(..., description="Company sector (e.g., Technology)"),
    background_tasks: BackgroundTasks = None
):
    """
    Score a single company using the Integration Service (Background).
    
    Returns immediately with task_id. Check status with GET /scoring/status/{task_id}
    
    Pipeline:
    - Fetches CS1/CS2 data
    - Collects Glassdoor/Board
    - Runs all calculators (VR, HR, Synergy, Org-AI-R, CI)
    - Saves to JSON file
    
    Takes 2-5 minutes to complete.
    """
    
    # Generate task ID
    import uuid
    task_id = str(uuid.uuid4())
    
    # Initialize status
    scoring_status[task_id] = {
        "status": "queued",
        "ticker": ticker.upper(),
        "sector": sector,
        "queued_at": datetime.now().isoformat()
    }
    
    # Add to background tasks
    if background_tasks:
        background_tasks.add_task(run_scoring_task, ticker.upper(), sector, task_id)
    else:
        # Fallback: import threading
        import threading
        thread = threading.Thread(target=run_scoring_task, args=(ticker.upper(), sector, task_id))
        thread.daemon = True
        thread.start()
    
    logger.info("scoring_queued", ticker=ticker, task_id=task_id)
    
    return {
        "status": "queued",
        "task_id": task_id,
        "ticker": ticker.upper(),
        "sector": sector,
        "message": f"Scoring started for {ticker}. Check status with GET /scoring/status/{task_id}",
        "estimated_time": "2-5 minutes"
    }


# ============================================================================
# CHECK SCORING STATUS
# ============================================================================

@router.get("/status/{task_id}")
async def get_scoring_status(task_id: str):
    """
    Check status of a background scoring task.
    
    Returns:
        - status: "queued", "running", "completed", or "failed"
        - progress info
        - results (if completed)
    """
    
    if task_id not in scoring_status:
        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} not found"
        )
    
    return scoring_status[task_id]


# ============================================================================
# SCORE COMPANY (Synchronous - with longer timeout)
# ============================================================================

@router.post("/score-sync/{ticker}")
async def score_company_sync(
    ticker: str,
    sector: str = Query(..., description="Company sector")
):
    """
    Score company synchronously (waits for completion).
    
    WARNING: This can take 2-5 minutes and may timeout!
    Use POST /score/{ticker} (background) instead for better UX.

    Scoring runs in a worker thread so the event loop stays free to serve
    nested HTTP calls from ScoringIntegrationService → localhost:8000 (avoids
    deadlock / read-timeout when a single worker blocks on sync score_company).
    """
    
    try:
        from src.scoring.integration_service import ScoringIntegrationService
        
        logger.info("sync_scoring_started", ticker=ticker)
        
        # Run integration service
        service = ScoringIntegrationService(api_base_url="http://localhost:8000")
        result = await asyncio.to_thread(
            service.score_company,
            ticker.upper(),
            sector,
        )
        
        logger.info("sync_scoring_completed", ticker=ticker, score=result['final_score'])
        
        return {
            "status": "success",
            "ticker": ticker.upper(),
            "final_score": result['final_score'],
            "vr_score": result['vr_score'],
            "hr_score": result['hr_score'],
            "synergy_score": result['synergy_score'],
            "position_factor": result['position_factor'],
            "talent_concentration": result['talent_concentration'],
            "confidence": result['confidence'],
            "dimension_scores": result['dimension_scores'],
            "result_file": result['result_json_path']
        }
        
    except Exception as e:
        logger.error("sync_scoring_failed", ticker=ticker, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Scoring failed: {str(e)}"
        )


# ============================================================================
# GET EXISTING RESULTS
# ============================================================================

@router.get("/results/{ticker}")
async def get_scoring_results(ticker: str):
    """Get existing scoring results for a company from JSON file."""
    
    results_dir = Path("results")
    result_file = results_dir / f"{ticker.upper()}_org_air_result.json"
    
    if not result_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No results found for {ticker}. Score the company first."
        )
    
    try:
        with open(result_file) as f:
            data = json.load(f)
        
        return data
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error reading results: {str(e)}"
        )


@router.get("/results")
async def list_all_results():
    """List all available scoring results."""
    
    results_dir = Path("results")
    results = []
    
    if not results_dir.exists():
        return {"count": 0, "results": []}
    
    for file in results_dir.glob("*_org_air_result.json"):
        try:
            with open(file) as f:
                data = json.load(f)
                results.append({
                    "ticker": data['ticker'],
                    "company_name": data.get('company_name', data['ticker']),
                    "final_score": data['final_score'],
                    "vr_score": data['vr_score'],
                    "hr_score": data['hr_score'],
                    "scored_at": data.get('scored_at'),
                    "file": str(file.name)
                })
        except Exception as e:
            logger.warning("failed_to_load_result", file=str(file), error=str(e))
            continue
    
    return {
        "count": len(results),
        "results": sorted(results, key=lambda x: x['final_score'], reverse=True)
    }


# ============================================================================
# DELETE RESULT
# ============================================================================

@router.delete("/results/{ticker}")
async def delete_scoring_result(ticker: str):
    """Delete scoring result file for a company."""
    
    results_dir = Path("results")
    result_file = results_dir / f"{ticker.upper()}_org_air_result.json"
    
    if not result_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No results found for {ticker}"
        )
    
    try:
        result_file.unlink()
        
        return {
            "status": "deleted",
            "ticker": ticker.upper(),
            "message": f"Results deleted for {ticker}"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting results: {str(e)}"
        )