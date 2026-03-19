"""
Airflow DAG: org_air_scoring_pipeline
Orchestrates the full Org-AI-R scoring pipeline for 5 target companies.

Pipeline per company:
  1. fetch_company        - CS1 API: company metadata
  2. fetch_cs2_evidence   - CS2 API: external signals
  3. fetch_sec_evidence   - CS2 API: SEC document chunks (Item 1, 1A, 7)
  4. collect_glassdoor    - Culture signals collection
  5. collect_board        - Board governance collection
  6. build_evidence       - Convert raw data → EvidenceScore objects
  7. map_dimensions       - EvidenceMapper → 7 dimension scores
  8. calculate_scores     - VR → PF → HR → Synergy → Org-AI-R → CI
  9. persist_assessment   - Save to Snowflake
 10. generate_result_json - Write result JSON file
"""

import sys
import os
from datetime import datetime,timedelta


for path in ["/opt/airflow", "/opt/airflow/src", "/opt/airflow/app"]:
    if path not in sys.path:
        sys.path.insert(0, path)

from airflow.decorators import dag, task


_FALLBACK_COMPANIES = [
    {"ticker": "NVDA", "sector": "Technology", "name": "NVIDIA Corporation"},
    {"ticker": "JPM", "sector": "Financial Services", "name": "JPMorgan Chase & Co."},
    {"ticker": "WMT", "sector": "Retail", "name": "Walmart Inc."},
    {"ticker": "GE", "sector": "Industrials", "name": "General Electric Company"},
    {"ticker": "DG", "sector": "Retail", "name": "Dollar General Corporation"},
]


def _load_companies() -> list:
    """Load all active companies from Snowflake at DAG parse time."""
    try:
        from app.services.snowflake import db
        rows = db.execute_query(
            "SELECT ticker, name FROM companies WHERE is_deleted = FALSE AND ticker IS NOT NULL"
        )
        return [{"ticker": r["ticker"], "name": r.get("name", r["ticker"]), "sector": "Unknown"} for r in rows]
    except Exception:
        return _FALLBACK_COMPANIES


COMPANIES = _load_companies()

# API base URL (within Docker network, the api service is at http://api:8000)
API_BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000")


@dag(
    dag_id="org_air_scoring_pipeline",
    description="Full Org-AI-R scoring pipeline: CS1/CS2 → Evidence → Dimensions → VR → HR → Synergy → Org-AI-R",
    schedule=None,
    start_date=datetime(2026, 2, 17),
    catchup=False,
    tags=["scoring", "org-air", "case-study-3"],
    default_args={
        "owner": "pe-orgair",
        "retries": 1,
        "execution_timeout": timedelta(minutes=10),
    },
)
def org_air_scoring_pipeline():

    @task
    def fetch_company(ticker: str) -> dict:
        """Fetch company metadata from CS1 API."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        return svc.fetch_company(ticker)

    @task
    def fetch_cs2_evidence(ticker: str) -> dict:
        """Fetch CS2 external signals."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        return svc.fetch_cs2_evidence(ticker)

    @task
    def fetch_sec_evidence(ticker: str) -> dict:
        """Fetch SEC document chunks (Item 1, 1A, 7) from CS2 API."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        return svc.fetch_sec_evidence(ticker)

    @task
    def collect_glassdoor(ticker: str) -> dict:
        """Collect Glassdoor culture signals."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        return svc.collect_glassdoor(ticker)

    @task
    def collect_board(ticker: str) -> dict:
        """Collect board governance signals."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        return svc.collect_board(ticker)

    @task
    def build_evidence(
        cs2_data: dict,
        culture_data: dict,
        board_data: dict,
        sec_data: dict,
    ) -> list:
        """Convert raw API data into EvidenceScore-compatible dicts."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        evidence_scores = svc.build_evidence_scores(
            cs2_data, culture_data, board_data, sec_data
        )
        # Serialize for XCom (EvidenceScore → dict)
        return [
            {
                "source": es.source.value,
                "raw_score": float(es.raw_score),
                "confidence": float(es.confidence),
                "evidence_count": es.evidence_count,
                "metadata": es.metadata,
            }
            for es in evidence_scores
        ]

    @task
    def map_dimensions(evidence_list: list) -> dict:
        """Map evidence to 7 dimension scores using EvidenceMapper."""
        from decimal import Decimal

        from src.scoring.evidence_mapper import (
            EvidenceMapper,
            EvidenceScore,
            SignalSource,
        )

        # Reconstruct EvidenceScore objects from XCom dicts
        evidence_scores = [
            EvidenceScore(
                source=SignalSource(e["source"]),
                raw_score=Decimal(str(e["raw_score"])),
                confidence=Decimal(str(e["confidence"])),
                evidence_count=e["evidence_count"],
                metadata=e.get("metadata", {}),
            )
            for e in evidence_list
        ]

        mapper = EvidenceMapper()
        dim_scores = mapper.map_evidence_to_dimensions(evidence_scores)

        # Serialize DimensionScore → dict for XCom
        return {
            dim.value: ds.to_dict()
            for dim, ds in dim_scores.items()
        }

    @task
    def calculate_scores(
        ticker: str,
        sector: str,
        dimension_scores_dict: dict,
        cs2_data: dict,
        culture_data: dict,
    ) -> dict:
        """Run full scoring chain: VR → PF → HR → Synergy → Org-AI-R → CI."""
        from decimal import Decimal

        from src.scoring.evidence_mapper import Dimension, DimensionScore, SignalSource
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)

        # Reconstruct DimensionScore objects from serialized dicts
        dim_scores = {}
        for dim_name, ds_dict in dimension_scores_dict.items():
            dim_scores[dim_name] = DimensionScore(
                dimension=Dimension(dim_name),
                score=Decimal(str(ds_dict["score"])),
                contributing_sources=[
                    SignalSource(s) for s in ds_dict.get("contributing_sources", [])
                ],
                total_weight=Decimal(str(ds_dict.get("total_weight", 0))),
                confidence=Decimal(str(ds_dict.get("confidence", 0.5))),
            )

        return svc.calculate_all_scores(
            ticker=ticker,
            sector=sector,
            dimension_scores=dim_scores,
            cs2_data=cs2_data,
            culture_data=culture_data,
        )

    @task
    def persist_to_snowflake(
        ticker: str,
        company_data: dict,
        results: dict,
    ) -> str:
        """Persist assessment + dimension scores to Snowflake."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        assessment_id = svc.persist_assessment(
            ticker=ticker,
            company_id=company_data.get("id"),
            results=results,
        )
        return assessment_id or ""

    @task
    def generate_result_json(
        ticker: str,
        results: dict,
        company_name: str,
        assessment_id: str,
    ) -> str:
        """Write complete Org-AI-R result JSON."""
        from src.scoring.integration_service import ScoringIntegrationService

        svc = ScoringIntegrationService(api_base_url=API_BASE_URL)
        return svc.generate_result_json(
            ticker=ticker,
            results=results,
            company_name=company_name,
            assessment_id=assessment_id or None,
        )

    # ------------------------------------------------------------------ #
    # DAG Wiring: iterate over 5 companies
    # ------------------------------------------------------------------ #

    for company in COMPANIES:
        ticker = company["ticker"]
        sector = company["sector"]
        name = company["name"]

        # Phase 1: Parallel data collection (5 tasks in parallel)
        comp = fetch_company.override(task_id=f"fetch_company_{ticker}")(ticker)
        cs2 = fetch_cs2_evidence.override(task_id=f"fetch_cs2_{ticker}")(ticker)
        sec = fetch_sec_evidence.override(task_id=f"fetch_sec_{ticker}")(ticker)
        culture = collect_glassdoor.override(task_id=f"collect_glassdoor_{ticker}")(
            ticker
        )
        board = collect_board.override(task_id=f"collect_board_{ticker}")(ticker)

        # Phase 2: Evidence processing (sequential)
        evidence = build_evidence.override(task_id=f"build_evidence_{ticker}")(
            cs2, culture, board, sec
        )
        dims = map_dimensions.override(task_id=f"map_dimensions_{ticker}")(evidence)

        # Phase 3: Scoring (needs CS2 + culture for TC calculation)
        scores = calculate_scores.override(task_id=f"calculate_scores_{ticker}")(
            ticker, sector, dims, cs2, culture
        )

        # Phase 4: Parallel persistence
        assessment_id = persist_to_snowflake.override(
            task_id=f"persist_{ticker}"
        )(ticker, comp, scores)

        generate_result_json.override(task_id=f"result_json_{ticker}")(
            ticker, scores, name, assessment_id
        )


# Instantiate the DAG
org_air_scoring_pipeline()
