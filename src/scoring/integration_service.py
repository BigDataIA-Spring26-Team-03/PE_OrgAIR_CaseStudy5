# src/scoring/integration_service.py

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import structlog

from src.scoring.config import ScoringConfig, DIMENSION_NAMES
from src.scoring.evidence_mapper import (
    Dimension,
    DimensionScore,
    EvidenceMapper,
    EvidenceScore,
    SignalSource,
)
from src.scoring.rubric_scorer import RubricScorer, concatenate_evidence_chunks
from src.scoring.vr_calculator import VRCalculator
from src.scoring.hr_calculator import HRCalculator
from src.scoring.position_factor import PositionFactorCalculator
from src.scoring.synergy_calculator import SynergyCalculator
from src.scoring.org_air_calculator import OrgAIRCalculator
from src.scoring.confidence import ConfidenceCalculator
from src.scoring.talent_concentration import (
    TalentConcentrationCalculator,
    JobAnalysis,
)

logger = structlog.get_logger()

# CS2 signal category → EvidenceMapper SignalSource
CATEGORY_TO_SOURCE = {
    "technology_hiring": SignalSource.TECHNOLOGY_HIRING,
    "innovation_activity": SignalSource.INNOVATION_ACTIVITY,
    "digital_presence": SignalSource.DIGITAL_PRESENCE,
    "leadership_signals": SignalSource.LEADERSHIP_SIGNALS,
}

# SEC document section → SignalSource + primary rubric dimension
SEC_SECTION_MAP = {
    "Item 1 (Business)": {
        "source": SignalSource.SEC_ITEM_1,
        "dimensions": ["use_case_portfolio", "technology_stack"],
    },
    "Item 1A (Risk)": {
        "source": SignalSource.SEC_ITEM_1A,
        "dimensions": ["ai_governance", "data_infrastructure"],
    },
    "Item 7 (MD&A)": {
        "source": SignalSource.SEC_ITEM_7,
        "dimensions": ["leadership", "use_case_portfolio", "data_infrastructure"],
    },
}

# Default alignment factors per sector (VR-HR strategic alignment)
SECTOR_ALIGNMENT = {
    "technology": 1.10,
    "financial_services": 1.05,
    "financial": 1.05,
    "consumer_staples": 0.95,
    "consumer": 0.95,
    "retail": 0.95,
    "industrials": 0.90,
    "manufacturing": 0.90,
}

# Default timing factors per sector (market AI adoption timing)
SECTOR_TIMING = {
    "technology": 1.15,
    "financial_services": 1.10,
    "financial": 1.10,
    "consumer_staples": 0.95,
    "consumer": 0.95,
    "retail": 0.95,
    "industrials": 0.90,
    "manufacturing": 0.90,
}


class ScoringIntegrationService:
    """Full Org-AI-R scoring pipeline orchestrator."""

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base = api_base_url.rstrip("/")

        # Scoring calculators
        self.evidence_mapper = EvidenceMapper()
        self.rubric_scorer = RubricScorer()
        self.vr_calc = VRCalculator()
        self.hr_calc = HRCalculator(use_database=False)
        self.pf_calc = PositionFactorCalculator()
        self.synergy_calc = SynergyCalculator()
        self.org_air_calc = OrgAIRCalculator()
        self.confidence_calc = ConfidenceCalculator()
        self.tc_calc = TalentConcentrationCalculator()

        logger.info(
            "ScoringIntegrationService initialized",
            api_base=self.api_base,
        )

    # ------------------------------------------------------------------ #
    # Phase 1: Data Collection (CS1 + CS2 APIs)
    # ------------------------------------------------------------------ #

    def fetch_company(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch company metadata from CS1 API.
        GET /api/v1/companies → find by ticker.
        Returns dict with: id, name, ticker, industry_id, sector.
        """
        url = f"{self.api_base}/api/v1/companies"
        logger.info("fetch_company", ticker=ticker, url=url)

        resp = requests.get(url, params={"limit": 100}, timeout=120)
        resp.raise_for_status()
        companies = resp.json()

        for company in companies:
            if (company.get("ticker") or "").upper() == ticker.upper():
                logger.info(
                    "company_found",
                    ticker=ticker,
                    company_id=company.get("id"),
                    name=company.get("name"),
                )
                return company

        logger.warning("company_not_found", ticker=ticker)
        return self.register_company(ticker)

    # Maps yfinance sector strings → industry table name (fuzzy)
    _SECTOR_TO_INDUSTRY = {
        "Technology":             "Business Services",
        "Communication Services": "Business Services",
        "Financial Services":     "Financial Services",
        "Consumer Defensive":     "Retail",
        "Consumer Cyclical":      "Retail",
        "Industrials":            "Manufacturing",
        "Basic Materials":        "Manufacturing",
        "Healthcare":             "Healthcare Services",
        "Energy":                 "Manufacturing",
        "Real Estate":            "Business Services",
        "Utilities":              "Business Services",
    }

    def _resolve_industry_id(self, sector: str) -> str | None:
        """Return an industry_id from the DB that best matches the yfinance sector."""
        from app.services.snowflake import db
        industry_name = self._SECTOR_TO_INDUSTRY.get(sector)
        if industry_name:
            rows = db.execute_query(
                "SELECT id FROM industries WHERE name = %(name)s LIMIT 1",
                {"name": industry_name},
            )
            if rows:
                return str(rows[0].get("id") or rows[0].get("ID"))
        # Fallback: first industry in the table
        rows = db.execute_query("SELECT id FROM industries LIMIT 1")
        if rows:
            return str(rows[0].get("id") or rows[0].get("ID"))
        return None

    def register_company(self, ticker: str, sector: str = "Unknown") -> Dict[str, Any]:
        """Auto-register an unknown ticker in Snowflake using yfinance metadata."""
        from app.services.snowflake import db
        ticker = ticker.upper()
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            name = info.get("longName") or info.get("shortName") or ticker
            sector = info.get("sector") or sector
        except Exception as exc:
            logger.warning("yfinance_lookup_failed", ticker=ticker, error=str(exc))
            name = ticker
        company_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, ticker))
        industry_id = self._resolve_industry_id(sector)
        try:
            db.execute_update(
                """
                INSERT INTO companies (id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at)
                VALUES (%(id)s, %(name)s, %(ticker)s, %(industry_id)s, 0.0, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """,
                {"id": company_id, "name": name, "ticker": ticker, "industry_id": industry_id},
            )
            logger.info("company_registered", ticker=ticker, name=name, company_id=company_id, industry_id=industry_id)
        except Exception as exc:
            logger.warning("company_insert_failed", ticker=ticker, error=str(exc))
        return {"id": company_id, "ticker": ticker, "name": name, "sector": sector}

    def fetch_cs2_evidence(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch CS2 external signals from CS2 API.
        GET /api/v1/signals/company/{ticker}.
        Returns dict with: ticker, company_id, signal_count, signals[].
        """
        url = f"{self.api_base}/api/v1/signals/company/{ticker}"
        logger.info("fetch_cs2_evidence", ticker=ticker, url=url)

        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            logger.info(
                "cs2_evidence_fetched",
                ticker=ticker,
                signal_count=data.get("signal_count", 0),
            )
            return data
        except requests.RequestException as e:
            logger.warning("cs2_evidence_fetch_failed", ticker=ticker, error=str(e))
            return {"ticker": ticker, "signals": [], "signal_count": 0}

    def collect_glassdoor(self, ticker: str) -> Dict[str, Any]:
        logger.info("collect_glassdoor", ticker=ticker)

        # Step 1: Try existing data from Snowflake first
        try:
            get_url = f"{self.api_base}/api/v1/culture-signals/ticker/{ticker}"
            resp = requests.get(get_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data:
                records = data if isinstance(data, list) else [data]

                # Filter out fake/empty rows (confidence < 0.3 or review_count == 0)
                real_records = [
                    r for r in records
                    if float(r.get("confidence") or 0) >= 0.3
                    and int(r.get("review_count") or 0) > 0
                ]

                # Use best record (highest confidence among real ones)
                record = max(real_records, key=lambda r: float(r.get("confidence") or 0)) \
                        if real_records else None

                if record:
                    score = float(record.get("overall_score") or 50)
                    avg_rating = float(record.get("avg_rating") or 3.0)
                    rating_score  = (avg_rating / 5.0) * 100
                    blended_score = round((score * 0.5) + (rating_score * 0.5), 2)
                    logger.info("glassdoor_from_cache",
                                ticker=ticker,
                                raw_score=score,
                                avg_rating=avg_rating,
                                blended_score=blended_score)
                    return {
                        "culture_score":          blended_score,
                        "overall_score":          blended_score,
                        "innovation_score":       float(record.get("innovation_score") or 50),
                        "data_driven_score":      float(record.get("data_driven_score") or 50),
                        "change_readiness_score": float(record.get("change_readiness_score") or 50),
                        "ai_awareness_score":     float(record.get("ai_awareness_score") or 50),
                        "review_count":           int(record.get("review_count") or 1),
                        "avg_rating":             float(record.get("avg_rating") or 3.0),
                        "confidence":             float(record.get("confidence") or 0.7),
                        "current_employee_ratio": float(record.get("current_employee_ratio") or 0.5),
                    }
                else:
                    logger.warning("glassdoor_no_real_records", ticker=ticker,
                                total_records=len(records))

        except requests.RequestException as e:
            logger.warning("glassdoor_get_failed", ticker=ticker, error=str(e))

        # Step 2: No real existing data — try live collection
        logger.info("glassdoor_no_cache_collecting", ticker=ticker)
        try:
            post_url = f"{self.api_base}/api/v1/culture-signals/collect/{ticker}"
            resp = requests.post(post_url, params={"use_cache": True}, timeout=30)

            if resp.status_code == 429:
                logger.warning("glassdoor_quota_exhausted", ticker=ticker)
                return {}

            resp.raise_for_status()
            data = resp.json()
            logger.info("glassdoor_collected_fresh", ticker=ticker,
                        culture_score=data.get("culture_score"))
            return data
        except requests.RequestException as e:
            logger.warning("glassdoor_collect_failed", ticker=ticker, error=str(e))
            return {}


    def collect_board(self, ticker: str) -> Dict[str, Any]:
        """
        First tries to read existing board data from Snowflake via GET.
        Only triggers full collection pipeline if no data exists.
        """
        logger.info("collect_board", ticker=ticker)

        # Step 1: Try existing data from Snowflake first
        try:
            get_url = f"{self.api_base}/api/v1/board-governance/ticker/{ticker}"
            resp = requests.get(get_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data:
                record = data[0] if isinstance(data, list) else data
                gov_score = record.get("governance_score")
                if gov_score is not None:
                    logger.info("board_from_cache",
                                ticker=ticker, governance_score=gov_score)
                    return {
                        "governance_score":         float(gov_score),
                        "confidence":               float(record.get("confidence") or 0.7),
                        "has_tech_committee":       bool(record.get("has_tech_committee") or False),
                        "has_ai_expertise":         bool(record.get("has_ai_expertise") or False),
                        "has_data_officer":         bool(record.get("has_data_officer") or False),
                        "has_independent_majority": bool(record.get("has_independent_majority") or False),
                        "has_risk_tech_oversight":  bool(record.get("has_risk_tech_oversight") or False),
                        "has_ai_strategy":          bool(record.get("has_ai_strategy") or False),
                        "member_count":             1,
                    }
        except requests.RequestException as e:
            logger.warning("board_get_failed", ticker=ticker, error=str(e))

        # Step 2: No existing data — trigger full collection pipeline
        logger.info("board_no_existing_data_collecting", ticker=ticker)
        try:
            post_url = f"{self.api_base}/api/v1/board-governance/collect/{ticker}"
            resp = requests.post(post_url, params={"use_cache": True}, timeout=300)
            resp.raise_for_status()
            data = resp.json()
            logger.info("board_collected_fresh", ticker=ticker,
                        governance_score=data.get("governance_score"))
            return {
                "governance_score":         float(data.get("governance_score") or 0),
                "confidence":               float(data.get("confidence") or 0.7),
                "has_tech_committee":       bool(data.get("has_tech_committee") or False),
                "has_ai_expertise":         bool(data.get("has_ai_expertise") or False),
                "has_data_officer":         bool(data.get("has_data_officer") or False),
                "has_independent_majority": bool(data.get("has_independent_majority") or False),
                "has_risk_tech_oversight":  bool(data.get("has_risk_tech_oversight") or False),
                "has_ai_strategy":          bool(data.get("has_ai_strategy") or False),
                "member_count":             int(data.get("member_count") or 0),
            }
        except requests.RequestException as e:
            logger.warning("board_collect_failed", ticker=ticker, error=str(e))
            return {}
    
    def fetch_sec_evidence(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch SEC document chunks from CS2 API.
        Only fetches documents with status=chunked to avoid empty chunk lookups.
        Returns dict keyed by section name with list of chunk texts.
        E.g.: {"Item 1 (Business)": ["chunk1...", "chunk2..."], ...}
        """
        logger.info("fetch_sec_evidence", ticker=ticker)

        sec_sections: Dict[str, List[str]] = {}

        try:
            # Step 1: List ONLY chunked documents for this ticker
            url = f"{self.api_base}/api/v1/documents"
            resp = requests.get(
                url,
                params={"ticker": ticker, "status": "chunked", "limit": 20},
                timeout=120,
            )
            resp.raise_for_status()
            doc_data = resp.json()
            documents = (
                doc_data.get("items", doc_data)
                if isinstance(doc_data, dict)
                else doc_data
            )

            if not documents:
                logger.warning("no_chunked_sec_documents", ticker=ticker)
                return sec_sections

            logger.info(
                "sec_documents_found",
                ticker=ticker,
                count=len(documents),
                filing_types=[d.get("filing_type") for d in documents],
            )

            # Step 2: For each chunked document, fetch all chunks grouped by section
            for doc in documents:
                doc_id = doc.get("id")
                filing_type = doc.get("filing_type", "unknown")
                if not doc_id:
                    continue

                chunks_url = f"{self.api_base}/api/v1/documents/{doc_id}/chunks"
                try:
                    chunks_resp = requests.get(
                        chunks_url, params={"limit": 500}, timeout=120
                    )
                    chunks_resp.raise_for_status()
                    chunks_data = chunks_resp.json()
                    items = (
                        chunks_data.get("items", chunks_data)
                        if isinstance(chunks_data, dict)
                        else chunks_data
                    )

                    doc_sections = set()
                    for chunk in items:
                        section = chunk.get("section", "")
                        content = chunk.get("content", "")
                        if section and content:
                            sec_sections.setdefault(section, []).append(content)
                            doc_sections.add(section)

                    logger.info(
                        "doc_chunks_fetched",
                        doc_id=doc_id,
                        filing_type=filing_type,
                        sections=list(doc_sections),
                        chunk_count=len(items),
                    )

                except requests.RequestException as e:
                    logger.warning(
                        "chunk_fetch_failed",
                        doc_id=doc_id,
                        filing_type=filing_type,
                        error=str(e),
                    )
                    continue

            logger.info(
                "sec_evidence_fetched",
                ticker=ticker,
                sections=list(sec_sections.keys()),
                total_chunks=sum(len(v) for v in sec_sections.values()),
            )

        except requests.RequestException as e:
            logger.warning("sec_evidence_fetch_failed", ticker=ticker, error=str(e))

        return sec_sections

    def _score_sec_sections(
        self, sec_data: Dict[str, List[str]]
    ) -> List[EvidenceScore]:
        """
        Run RubricScorer on SEC document chunks to produce EvidenceScores.
        Each recognized section (Item 1, 1A, 7) becomes an EvidenceScore
        with a rubric-derived raw_score.
        """
        evidence_scores: List[EvidenceScore] = []

        for section_name, mapping in SEC_SECTION_MAP.items():
            chunks = sec_data.get(section_name, [])
            if not chunks:
                continue

            # Concatenate all chunks for this section
            full_text = concatenate_evidence_chunks(chunks)
            source = mapping["source"]
            primary_dim = mapping["dimensions"][0]

            # Score against the primary dimension's rubric
            rubric_result = self.rubric_scorer.score_dimension(
                dimension=primary_dim,
                evidence_text=full_text,
            )

            # Confidence based on amount of text evidence
            chunk_confidence = min(0.6 + len(chunks) * 0.05, 0.95)

            evidence_scores.append(
                EvidenceScore(
                    source=source,
                    raw_score=Decimal(str(max(0, min(100, float(rubric_result.score))))),
                    confidence=Decimal(str(round(chunk_confidence, 3))),
                    evidence_count=len(chunks),
                    metadata={
                        "section": section_name,
                        "rubric_level": rubric_result.level.label,
                        "keyword_matches": rubric_result.keyword_match_count,
                        "matched_keywords": rubric_result.matched_keywords[:10],
                    },
                )
            )

            logger.info(
                "sec_section_scored",
                section=section_name,
                score=float(rubric_result.score),
                level=rubric_result.level.label,
                keywords_matched=rubric_result.keyword_match_count,
                chunks=len(chunks),
            )

        return evidence_scores

    # ------------------------------------------------------------------ #
    # Phase 2: Evidence Processing
    # ------------------------------------------------------------------ #

    def build_evidence_scores(
        self,
        cs2_data: Dict[str, Any],
        culture_data: Dict[str, Any],
        board_data: Dict[str, Any],
        sec_data: Optional[Dict[str, List[str]]] = None,
    ) -> List[EvidenceScore]:
        """
        Convert raw API responses into EvidenceScore objects.
        Maps each CS2 category, Glassdoor, and Board data into the
        EvidenceMapper's expected input format.
        """
        evidence_scores: List[EvidenceScore] = []

        # --- CS2 External Signals ---
        signals = cs2_data.get("signals", [])
        # Group signals by category for aggregation
        category_groups: Dict[str, List[Dict]] = {}
        for sig in signals:
            cat = sig.get("category", "")
            category_groups.setdefault(cat, []).append(sig)

        for category, sigs in category_groups.items():
            source = CATEGORY_TO_SOURCE.get(category)
            if not source:
                logger.debug("skipping_unknown_category", category=category)
                continue

            # Aggregate: average normalized_score, count as evidence_count
            scores = [
                float(s.get("normalized_score", 50))
                for s in sigs
                if s.get("normalized_score") is not None
            ]
            avg_score = sum(scores) / len(scores) if scores else 50.0
            avg_score = max(0.0, min(100.0, avg_score))

            evidence_scores.append(
                EvidenceScore(
                    source=source,
                    raw_score=Decimal(str(round(avg_score, 2))),
                    confidence=Decimal(str(min(0.5 + len(sigs) * 0.05, 0.95))),
                    evidence_count=len(sigs),
                    metadata={"signal_count": len(sigs), "category": category},
                )
            )

        # --- Glassdoor Culture ---
        culture_score = (
            culture_data.get("culture_score")
            or culture_data.get("overall_score")
        )
        if culture_score is not None:
            evidence_scores.append(
                EvidenceScore(
                    source=SignalSource.GLASSDOOR_REVIEWS,
                    raw_score=Decimal(str(max(0, min(100, float(culture_score))))),
                    confidence=Decimal(
                        str(min(float(culture_data.get("confidence", 0.7)), 1.0))
                    ),
                    evidence_count=int(culture_data.get("review_count", 1)),
                    metadata={
                        "innovation_score": culture_data.get("innovation_score"),
                        "data_driven_score": culture_data.get("data_driven_score"),
                        "ai_awareness_score": culture_data.get("ai_awareness_score"),
                        "change_readiness_score": culture_data.get("change_readiness_score"),
                    },
                )
            )

        # --- Board Composition ---
        gov_score = board_data.get("governance_score")
        if gov_score is not None:
            evidence_scores.append(
                EvidenceScore(
                    source=SignalSource.BOARD_COMPOSITION,
                    raw_score=Decimal(str(max(0, min(100, float(gov_score))))),
                    confidence=Decimal(
                        str(min(float(board_data.get("confidence", 0.7)), 1.0))
                    ),
                    evidence_count=int(board_data.get("member_count", 1)),
                    metadata={
                        "has_tech_committee": board_data.get("has_tech_committee"),
                        "has_ai_expertise": board_data.get("has_ai_expertise"),
                        "has_data_officer": board_data.get("has_data_officer"),
                    },
                )
            )

        # --- SEC Filing Sections (Item 1, 1A, 7) ---
        if sec_data:
            sec_evidence = self._score_sec_sections(sec_data)
            evidence_scores.extend(sec_evidence)

        logger.info(
            "evidence_scores_built",
            total_evidence=len(evidence_scores),
            sources=[e.source.value for e in evidence_scores],
        )
        return evidence_scores

    def map_to_dimensions(
        self, evidence_scores: List[EvidenceScore]
    ) -> Dict[str, DimensionScore]:
        """
        Run EvidenceMapper to produce 7 dimension scores.
        Returns dict keyed by dimension name string.
        """
        dim_scores = self.evidence_mapper.map_evidence_to_dimensions(evidence_scores)

        result = {}
        for dim, ds in dim_scores.items():
            result[dim.value] = ds

        logger.info(
            "dimensions_mapped",
            dimensions={
                name: float(ds.score) for name, ds in result.items()
            },
        )
        return result

    # ------------------------------------------------------------------ #
    # Phase 3: Scoring Chain
    # ------------------------------------------------------------------ #

    def _calculate_talent_concentration(
        self,
        cs2_data: Dict[str, Any],
        culture_data: Dict[str, Any],
    ) -> float:
        """Calculate talent concentration from CS2 job signals + Glassdoor."""
        signals = cs2_data.get("signals", [])
        job_signals = [
            s for s in signals
            if s.get("category") == "technology_hiring"
        ]

        # Build JobAnalysis from CS2 job signals
        job_analysis = self.tc_calc.analyze_job_postings(job_signals)

        # Glassdoor individual mentions
        individual_mentions = int(culture_data.get("individual_mentions", 0))
        review_count = int(culture_data.get("review_count", 1))

        tc = self.tc_calc.calculate_tc(
            job_analysis=job_analysis,
            glassdoor_individual_mentions=individual_mentions,
            glassdoor_review_count=max(review_count, 1),
        )

        logger.info(
            "talent_concentration_calculated",
            tc=float(tc),
            total_jobs=job_analysis.total_ai_jobs,
            senior_jobs=job_analysis.senior_ai_jobs,
        )
        return float(tc)

    def calculate_all_scores(
        self,
        ticker: str,
        sector: str,
        dimension_scores: Dict[str, DimensionScore],
        cs2_data: Dict[str, Any],
        culture_data: Dict[str, Any],
        evidence_scores: Optional[List[EvidenceScore]] = None,
    ) -> Dict[str, Any]:
        """
        Full scoring chain:
        TC → VR → Position Factor → HR → Synergy → Org-AI-R → Confidence
        """
        # --- Step 1: Talent Concentration ---
        tc = self._calculate_talent_concentration(cs2_data, culture_data)

        # --- Step 2: V^R ---
        dim_score_list = [
            float(dimension_scores[name].score)
            for name in DIMENSION_NAMES
        ]
        vr_result = self.vr_calc.calculate(dim_score_list, tc)
        vr_score = float(vr_result.vr_score)

        logger.info("vr_calculated", ticker=ticker, vr_score=vr_score)

        # --- Step 3: Position Factor ---
        sector_normalized = sector.lower().replace(" ", "_")
        try:
            pf_result = self.pf_calc.calculate_with_realtime(
                vr_score=vr_score,
                ticker=ticker,
                sector=sector_normalized,
            )
        except Exception as e:
            logger.warning(
                "position_factor_realtime_failed",
                ticker=ticker,
                error=str(e),
            )
            # Fallback: use default market cap percentile
            pf_result = self.pf_calc.calculate(
                vr_score=vr_score,
                sector=sector_normalized,
                market_cap_percentile=0.5,
            )
        pf_value = float(pf_result.position_factor)

        logger.info("position_factor_calculated", ticker=ticker, pf=pf_value)

        # --- Step 4: H^R ---
        hr_result = self.hr_calc.calculate(
            sector=sector_normalized,
            position_factor=pf_value,
        )
        hr_score = float(hr_result.hr_score)

        logger.info("hr_calculated", ticker=ticker, hr_score=hr_score)

        # --- Step 5: Synergy ---
        alignment = SECTOR_ALIGNMENT.get(sector_normalized, 1.0)
        timing = SECTOR_TIMING.get(sector_normalized, 1.0)

        synergy_result = self.synergy_calc.calculate(
            vr_score=vr_score,
            hr_score=hr_score,
            alignment_factor=alignment,
            timing_factor=timing,
        )
        synergy_score = float(synergy_result.synergy_score)

        logger.info("synergy_calculated", ticker=ticker, synergy_score=synergy_score)

        # --- Step 6: Org-AI-R ---
        org_air_result = self.org_air_calc.calculate(
            vr_score=vr_score,
            hr_score=hr_score,
            synergy_score=synergy_score,
        )
        final_score = float(org_air_result.org_air_score)

        logger.info("org_air_calculated", ticker=ticker, final_score=final_score)

        # --- Step 7: Confidence ---
        # Use actual evidence item count (chunks, signals, reviews) — not double-counted
        # contributing_sources, which inflates when the same source maps to multiple dimensions.
        if evidence_scores:
            evidence_count = max(
                1,
                sum(e.evidence_count for e in evidence_scores),
            )
        else:
            # Fallback: count unique source mentions across dimensions (can double-count)
            evidence_count = max(
                1,
                sum(len(ds.contributing_sources) for ds in dimension_scores.values()),
            )

        ci = self.confidence_calc.calculate(
            score=final_score,
            score_type="org_air",
            evidence_count=evidence_count,
        )

        logger.info(
            "confidence_calculated",
            ticker=ticker,
            ci_lower=float(ci.ci_lower),
            ci_upper=float(ci.ci_upper),
            sem=float(ci.sem),
        )

        # --- Assemble results ---
        return {
            "ticker": ticker,
            "sector": sector,
            "final_score": final_score,
            "vr_score": vr_score,
            "hr_score": hr_score,
            "synergy_score": synergy_score,
            "position_factor": pf_value,
            "talent_concentration": tc,
            "confidence": ci.to_dict(),
            "dimension_scores": {
                name: ds.to_dict() for name, ds in dimension_scores.items()
            },
            "evidence_count": evidence_count,
            "vr_details": vr_result.to_dict(),
            "hr_details": hr_result.to_dict(),
            "pf_details": pf_result.to_dict(),
            "synergy_details": synergy_result.to_dict(),
            "org_air_details": org_air_result.to_dict(),
        }

    # ------------------------------------------------------------------ #
    # Phase 4: Persistence
    # ------------------------------------------------------------------ #

    def persist_assessment(
        self,
        ticker: str,
        company_id: Optional[str],
        results: Dict[str, Any],
    ) -> Optional[str]:
        if not company_id:
            logger.warning("skip_persist_no_company_id", ticker=ticker)
            return None

        try:
            import os
            import snowflake.connector
            from uuid import uuid4

            # Connect directly — no app.config dependency
            conn = snowflake.connector.connect(
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
                warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
                role="ACCOUNTADMIN",
            )

            assessment_id = str(uuid4())
            now = datetime.now(timezone.utc)
            cursor = conn.cursor()

            try:
                # Single INSERT with all 11 columns
                cursor.execute("""
                    INSERT INTO assessments (
                        id, company_id, assessment_type, assessment_date,
                        status, primary_assessor, secondary_assessor,
                        v_r_score, confidence_lower, confidence_upper,
                        created_at
                    ) VALUES (
                        %(id)s, %(company_id)s, %(assessment_type)s, %(assessment_date)s,
                        %(status)s, %(primary_assessor)s, %(secondary_assessor)s,
                        %(v_r_score)s, %(confidence_lower)s, %(confidence_upper)s,
                        %(created_at)s
                    )
                """, {
                    "id": assessment_id,
                    "company_id": company_id,
                    "assessment_type": "screening",
                    "assessment_date": now.date(),
                    "status": "approved",
                    "primary_assessor": "OrgAIR_Pipeline_v1",
                    "secondary_assessor": None,
                    "v_r_score": results["final_score"],
                    "confidence_lower": results["confidence"]["ci_lower"],
                    "confidence_upper": results["confidence"]["ci_upper"],
                    "created_at": now,
                })

                # Insert 7 dimension scores
                dim_enum_map = {
                    "data_infrastructure": "DATA_INFRASTRUCTURE",
                    "ai_governance": "AI_GOVERNANCE",
                    "technology_stack": "TECHNOLOGY_STACK",
                    "talent": "TALENT_SKILLS",
                    "leadership": "LEADERSHIP_VISION",
                    "use_case_portfolio": "USE_CASE_PORTFOLIO",
                    "culture": "CULTURE_CHANGE",
                }

                for dim_name, dim_data in results.get("dimension_scores", {}).items():
                    if not isinstance(dim_data, dict):
                        continue
                    cursor.execute("""
                        INSERT INTO dimension_scores (
                            id, assessment_id, dimension, score,
                            confidence, evidence_count, created_at
                        ) VALUES (
                            %(id)s, %(assessment_id)s, %(dimension)s, %(score)s,
                            %(confidence)s, %(evidence_count)s, %(created_at)s
                        )
                    """, {
                        "id": str(uuid4()),
                        "assessment_id": assessment_id,
                        "dimension": dim_enum_map.get(dim_name, dim_name.upper()),
                        "score": dim_data.get("score", 50.0),
                        "confidence": dim_data.get("confidence", 0.5),
                        "evidence_count": len(dim_data.get("contributing_sources", [])),
                        "created_at": now,
                    })

                conn.commit()
                logger.info("assessment_persisted", ticker=ticker,
                        assessment_id=assessment_id,
                        final_score=results["final_score"])
                return assessment_id

            except Exception as e:
                conn.rollback()
                raise
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            logger.error("persist_failed", ticker=ticker, error=str(e), exc_info=True)
            return None

    def generate_result_json(
        self,
        ticker: str,
        results: Dict[str, Any],
        company_name: str = "",
        assessment_id: Optional[str] = None,
    ) -> str:
        """
        Write complete Org-AI-R result JSON to results/{TICKER}_org_air_result.json.
        Returns the output file path.
        """
        output = {
            "ticker": ticker,
            "company_name": company_name or ticker,
            "sector": results["sector"],
            "assessment_id": assessment_id,
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "final_score": results["final_score"],
            "vr_score": results["vr_score"],
            "hr_score": results["hr_score"],
            "synergy_score": results["synergy_score"],
            "position_factor": results["position_factor"],
            "talent_concentration": results["talent_concentration"],
            "confidence": {
                "ci_lower": results["confidence"]["ci_lower"],
                "ci_upper": results["confidence"]["ci_upper"],
                "sem": results["confidence"]["sem"],
                "reliability": results["confidence"]["reliability"],
                "evidence_count": results["evidence_count"],
            },
            "dimension_scores": results["dimension_scores"],
            "scoring_parameters": {
                "alpha": float(ScoringConfig.ALPHA),
                "beta": float(ScoringConfig.BETA),
                "lambda": float(ScoringConfig.LAMBDA_PENALTY),
                "delta": float(HRCalculator.DELTA_POSITION),
            },
        }

        results_dir = Path(__file__).resolve().parent.parent.parent / "results"
        results_dir.mkdir(exist_ok=True)
        filepath = results_dir / f"{ticker}_org_air_result.json"

        with open(filepath, "w") as f:
            json.dump(output, f, indent=2, default=str)

        logger.info("result_json_generated", ticker=ticker, path=str(filepath))
        return str(filepath)

    # ------------------------------------------------------------------ #
    # Full Pipeline Orchestrator
    # ------------------------------------------------------------------ #

    def score_company(self, ticker: str, sector: str) -> Dict[str, Any]:
        """
        End-to-end Org-AI-R scoring pipeline for one company.

        Steps:
          1. Fetch company from CS1
          2. Fetch CS2 evidence (external signals)
          3. Fetch SEC document chunks (Item 1, 1A, 7)
          4. Collect Glassdoor culture data
          5. Collect Board governance data
          6. Build EvidenceScore objects (CS2 + SEC + Glassdoor + Board)
          7. Map evidence → 7 dimensions
          8. Calculate VR → PF → HR → Synergy → Org-AI-R → Confidence
          9. Persist assessment to Snowflake
         10. Generate result JSON
        """
        logger.info("pipeline_start", ticker=ticker, sector=sector)

        # Phase 1: Data Collection
        company_data = self.fetch_company(ticker)
        cs2_data = self.fetch_cs2_evidence(ticker)
        sec_data = self.fetch_sec_evidence(ticker)
        culture_data = self.collect_glassdoor(ticker)
        board_data = self.collect_board(ticker)

        # Phase 2: Evidence Processing
        evidence_scores = self.build_evidence_scores(
            cs2_data, culture_data, board_data, sec_data
        )
        dimension_scores = self.map_to_dimensions(evidence_scores)

        # Phase 3: Scoring Chain
        results = self.calculate_all_scores(
            ticker=ticker,
            sector=sector,
            dimension_scores=dimension_scores,
            cs2_data=cs2_data,
            culture_data=culture_data,
            evidence_scores=evidence_scores,
        )

        # Phase 4: Persistence
        company_id = company_data.get("id")
        assessment_id = self.persist_assessment(ticker, company_id, results)
        filepath = self.generate_result_json(
            ticker=ticker,
            results=results,
            company_name=company_data.get("name", ticker),
            assessment_id=assessment_id,
        )

        results["assessment_id"] = assessment_id
        results["result_json_path"] = filepath

        logger.info(
            "pipeline_complete",
            ticker=ticker,
            final_score=results["final_score"],
            assessment_id=assessment_id,
        )
        return results
