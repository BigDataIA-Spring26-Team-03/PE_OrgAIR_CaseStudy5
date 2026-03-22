# src/services/integration/cs2_client.py
from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SourceType(str, enum.Enum):
    """
    Evidence source types produced by CS2 collectors.

    """
    # SEC filing sections (from document_chunks_sec.section)
    SEC_10K_ITEM_1      = "sec_10k_item_1"      # Item 1 (Business)
    SEC_10K_ITEM_1A     = "sec_10k_item_1a"     # Item 1A (Risk Factors)
    SEC_10K_ITEM_7      = "sec_10k_item_7"      # Item 7 (MD&A)

    # External signals (from external_signals.signal_type)
    JOB_POSTING_LINKEDIN    = "job_posting_linkedin"
    JOB_POSTING_INDEED      = "job_posting_indeed"
    PATENT_USPTO            = "patent_uspto"
    PRESS_RELEASE           = "press_release"
    TECH_STACK_SIGNAL       = "tech_stack_signal"  
    GLASSDOOR_REVIEW        = "glassdoor_review"
    BOARD_PROXY_DEF14A      = "board_proxy_def14a"

    @classmethod
    def from_raw(cls, raw: str) -> Optional["SourceType"]:
        if not raw:
            return None

        # Direct enum value match first (e.g. "sec_10k_item_1")
        try:
            return cls(raw.lower().strip())
        except ValueError:
            pass

        # Map Snowflake section names → SourceType
        SECTION_MAP: Dict[str, "SourceType"] = {
            # ── SEC section strings (document_chunks_sec.section, lowercased) ──
            "item 1 (business)":    cls.SEC_10K_ITEM_1,
            "item 1":               cls.SEC_10K_ITEM_1,
            "item 1a (risk)":       cls.SEC_10K_ITEM_1A,
            "item 1a":              cls.SEC_10K_ITEM_1A,
            "item 7 (md&a)":        cls.SEC_10K_ITEM_7,
            "item 7":               cls.SEC_10K_ITEM_7,
            "item 2 (md&a)":        cls.SEC_10K_ITEM_7,

            # ── external_signals.category strings ──
            "technology_hiring":    cls.JOB_POSTING_LINKEDIN,  
            "innovation_activity":  cls.PATENT_USPTO,
            "leadership_signals":   cls.PRESS_RELEASE,
            "digital_presence":     cls.TECH_STACK_SIGNAL,    

            "culture_signals":      cls.GLASSDOOR_REVIEW,      
            "governance_signals":   cls.BOARD_PROXY_DEF14A, 
            # Backward-compatible aliases (CS3 JSON label strings)
            "glassdoor_reviews":    cls.GLASSDOOR_REVIEW,
            "board_composition":    cls.BOARD_PROXY_DEF14A,
        }

        normalized = raw.strip().lower()
        if normalized in SECTION_MAP:
            return SECTION_MAP[normalized]

        logger.warning("cs2_unknown_source_type", extra={"raw": raw})
        return None


class SignalCategory(str, enum.Enum):
    """
    Signal categories assigned by CS2 collectors.
    These match your external_signals table signal_type values and
    the contributing_sources in your CS3 scoring JSON.
    """
    TECHNOLOGY_HIRING   = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE    = "digital_presence"
    LEADERSHIP_SIGNALS  = "leadership_signals"
    CULTURE_SIGNALS     = "culture_signals"
    GOVERNANCE_SIGNALS  = "governance_signals"

    GLASSDOOR_REVIEWS   = "glassdoor_reviews"    # your CS3 JSON uses this exact string
    BOARD_COMPOSITION   = "board_composition"    # your CS3 JSON uses this exact string

    @classmethod
    def from_raw(cls, raw: str) -> Optional["SignalCategory"]:
        """Safe parser for signal category strings."""
        if not raw:
            return None
        try:
            return cls(raw.lower().strip())
        except ValueError:
            logger.warning("cs2_unknown_signal_category", extra={"raw": raw})
            return None


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ExtractedEntity:
    """
    A structured entity extracted from evidence text by CS2 NLP pipeline.
    entity_type examples: "ai_investment", "technology", "person", "dollar_amount"
    """
    entity_type: str
    text: str
    char_start: int
    char_end: int
    confidence: float
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CS2Evidence:
    """
    A single evidence item from CS2.

    Maps from two sources in your actual database:
    1. document_chunks_sec → SEC filing chunks
    2. external_signals    → job postings, patents, glassdoor, board signals

    CHANGE 3: added filing_type and section fields that actually exist
    in your document_chunks_sec table and are useful for CS4 citations.
    """
    evidence_id: str
    company_id: str
    source_type: SourceType
    signal_category: SignalCategory
    content: str
    extracted_at: datetime
    confidence: float                        # 0.0 – 1.0

    # Optional metadata
    fiscal_year: Optional[int] = None
    source_url: Optional[str] = None
    page_number: Optional[int] = None

    # CHANGE 3: these fields exist in your actual CS2 data
    filing_type: Optional[str] = None       # "10-K", "10-Q", "8-K"
    section: Optional[str] = None          # "Item 1 (Business)", "Item 7 (MD&A)"
    chunk_index: Optional[int] = None      # position within document

    extracted_entities: List[ExtractedEntity] = field(default_factory=list)

    # Indexing status — written back via mark_indexed()
    indexed_in_cs4: bool = False
    indexed_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CS2Client:
    """
    Async HTTP client for the CS2 Evidence Collection API.

    In your app, CS2 evidence is served from the SAME FastAPI instance as CS1/CS3
    (localhost:8000). The endpoints are:
        GET  /api/v1/evidence?company_id=NVDA
        POST /api/v1/evidence/mark-indexed

    If those endpoints don't exist yet, see the fallback note in get_evidence().

    Usage:
        async with CS2Client() as client:
            evidence = await client.get_evidence("NVDA")
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: float = 60.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "CS2Client":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_evidence(
        self,
        company_id: str,
        source_types: Optional[List[SourceType]] = None,
        signal_categories: Optional[List[SignalCategory]] = None,
        min_confidence: float = 0.0,
        indexed: Optional[bool] = None,
        since: Optional[datetime] = None,
        limit: int = 500,
    ) -> List[CS2Evidence]:
        """
        Fetch evidence for a company from all 4 sources concurrently:
        - /api/v1/evidence       → 4 external signals (hiring, patents, digital, leadership)
        - /api/v1/documents      → SEC 10-K chunks (documents.py)
        - /api/v1/board-governance/ticker/{ticker} → board governance (board.py)
        - /api/v1/culture-signals/reviews/ticker/{ticker} → Glassdoor reviews (culture.py)
        """
        ext_task  = self._fetch_external_evidence(company_id, min_confidence, indexed, since, limit)
        sec_task  = self._fetch_sec_evidence(company_id, limit)
        board_task = self._fetch_board_evidence(company_id)
        gd_task   = self._fetch_glassdoor_evidence(company_id, limit)

        results = await asyncio.gather(ext_task, sec_task, board_task, gd_task, return_exceptions=True)

        all_evidence: List[CS2Evidence] = []
        labels = ("external", "sec", "board", "glassdoor")
        for label, result in zip(labels, results):
            if isinstance(result, BaseException):
                logger.warning("cs2_source_failed source=%s company=%s error=%s", label, company_id, result)
            else:
                all_evidence.extend(result)

        # Apply filters if requested
        if source_types:
            allowed_st = {st.value for st in source_types}
            all_evidence = [e for e in all_evidence if e.source_type and e.source_type.value in allowed_st]
        if signal_categories:
            allowed_sc = {sc.value for sc in signal_categories}
            all_evidence = [e for e in all_evidence if e.signal_category and e.signal_category.value in allowed_sc]

        return all_evidence[:limit]

    # ------------------------------------------------------------------
    # Source-specific fetch helpers
    # ------------------------------------------------------------------

    async def _fetch_external_evidence(
        self,
        company_id: str,
        min_confidence: float,
        indexed: Optional[bool],
        since: Optional[datetime],
        limit: int,
    ) -> List[CS2Evidence]:
        """Fetch 4 external signal types from /api/v1/evidence."""
        params: Dict[str, Any] = {"company_id": company_id, "limit": limit}
        if min_confidence > 0.0:
            params["min_confidence"] = min_confidence
        if indexed is not None:
            params["indexed"] = indexed
        if since is not None:
            params["since"] = since.isoformat()

        client = self._get_client()
        response = await client.get("/api/v1/evidence", params=params)
        response.raise_for_status()
        return [self._map_evidence(item) for item in response.json()]

    async def _fetch_sec_evidence(self, company_id: str, limit: int) -> List[CS2Evidence]:
        """Fetch SEC 10-K chunks from documents.py router."""
        client = self._get_client()
        ticker = company_id.upper()

        # Step 1: get list of chunked documents for this ticker
        doc_resp = await client.get(
            "/api/v1/documents",
            params={"ticker": ticker, "status": "chunked", "limit": 50},
        )
        if doc_resp.status_code != 200:
            return []
        docs = doc_resp.json()
        if not docs:
            return []

        # Step 2: fetch chunks for each document in parallel
        async def _get_chunks(doc: dict) -> List[CS2Evidence]:
            chunk_resp = await client.get(
                f"/api/v1/documents/{doc['id']}/chunks",
                params={"limit": 500},
            )
            if chunk_resp.status_code != 200:
                return []
            items = []
            filing_date = doc.get("filing_date") or ""
            try:
                fy = int(filing_date[:4]) if filing_date and len(filing_date) >= 4 else None
            except (ValueError, TypeError):
                fy = None
            for chunk in chunk_resp.json():
                section = chunk.get("section") or ""
                source_type = SourceType.from_raw(section) or SourceType.SEC_10K_ITEM_1
                items.append(CS2Evidence(
                    evidence_id=str(chunk.get("id", "")),
                    company_id=company_id,
                    source_type=source_type,
                    signal_category=SignalCategory.DIGITAL_PRESENCE,
                    content=str(chunk.get("content") or ""),
                    extracted_at=datetime.now(),
                    confidence=0.85,
                    fiscal_year=fy,
                    source_url=doc.get("source_url"),
                    filing_type=doc.get("filing_type"),
                    section=section,
                    chunk_index=chunk.get("chunk_index"),
                ))
            return items

        chunk_lists = await asyncio.gather(*[_get_chunks(doc) for doc in docs], return_exceptions=True)
        evidence: List[CS2Evidence] = []
        for result in chunk_lists:
            if isinstance(result, list):
                evidence.extend(result)
        return evidence[:limit]

    async def _fetch_board_evidence(self, company_id: str) -> List[CS2Evidence]:
        """Fetch board governance evidence from board.py router."""
        client = self._get_client()
        resp = await client.get(f"/api/v1/board-governance/ticker/{company_id.upper()}")
        if resp.status_code != 200:
            return []
        data = resp.json()
        if not data:
            return []
        # May return a list or a single object
        items = data if isinstance(data, list) else [data]
        evidence: List[CS2Evidence] = []
        for item in items:
            raw_evidence = item.get("evidence") or []
            if isinstance(raw_evidence, list):
                content = " | ".join(str(e) for e in raw_evidence if e)
            else:
                content = str(raw_evidence)
            if not content.strip():
                continue
            evidence.append(CS2Evidence(
                evidence_id=str(item.get("id", "")),
                company_id=company_id,
                source_type=SourceType.BOARD_PROXY_DEF14A,
                signal_category=SignalCategory.GOVERNANCE_SIGNALS,
                content=content,
                extracted_at=datetime.now(),
                confidence=float(item.get("confidence", 0.7)),
            ))
        return evidence

    async def _fetch_glassdoor_evidence(self, company_id: str, limit: int) -> List[CS2Evidence]:
        """Fetch Glassdoor reviews from culture.py router."""
        client = self._get_client()
        resp = await client.get(f"/api/v1/culture-signals/reviews/ticker/{company_id.upper()}")
        if resp.status_code != 200:
            return []
        reviews = resp.json()
        if not reviews:
            return []
        evidence: List[CS2Evidence] = []
        for r in reviews[:limit]:
            title = r.get("title") or ""
            pros  = r.get("pros") or ""
            cons  = r.get("cons") or ""
            content = f"{title}: {pros} / {cons}".strip(": /").strip()
            if not content:
                continue
            rd = r.get("review_date") or ""
            try:
                fy = int(str(rd)[:4]) if rd and len(str(rd)) >= 4 else None
            except (ValueError, TypeError):
                fy = None
            evidence.append(CS2Evidence(
                evidence_id=str(r.get("id", "")),
                company_id=company_id,
                source_type=SourceType.GLASSDOOR_REVIEW,
                signal_category=SignalCategory.CULTURE_SIGNALS,
                content=content,
                extracted_at=datetime.now(),
                confidence=float(r.get("culture_score") or 0.7),
                fiscal_year=fy,
            ))
        return evidence

    async def mark_indexed(self, evidence_ids: List[str]) -> int:
        """
        Notify CS2 that these items have been indexed in CS4's ChromaDB.
        Prevents re-indexing the same chunks on the next pipeline run.
        Returns count of records updated.
        """
        if not evidence_ids:
            return 0

        client = self._get_client()
        response = await client.post(
            "/api/v1/evidence/mark-indexed",
            json={"evidence_ids": evidence_ids},
        )
        response.raise_for_status()
        updated: int = response.json().get("updated_count", 0)

        logger.info(
            "cs2_mark_indexed",
            extra={"updated_count": updated, "requested": len(evidence_ids)},
        )
        return updated

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _map_evidence(self, data: dict) -> CS2Evidence:
        """
        Map a raw evidence dict to CS2Evidence.

        """
        def _get(*keys: str, default: Any = None) -> Any:
            """Try multiple key names, return first non-None value."""
            for k in keys:
                v = data.get(k)
                if v is not None:
                    return v
            return default

        # Parse source type — try both formats
        raw_source = _get("source_type", "signal_type", "section", default="")
        source_type = SourceType.from_raw(str(raw_source))

        # Parse signal category
        raw_signal = _get("signal_category", "signal_type", default="")
        signal_category = SignalCategory.from_raw(str(raw_signal))

        # If still None, derive signal from source type as fallback
        if signal_category is None and source_type is not None:
            signal_category = self._derive_signal_from_source(source_type)

        # Parse extracted_at timestamp
        raw_ts = _get("extracted_at", "created_at", "filing_date", default="")
        try:
            extracted_at = datetime.fromisoformat(str(raw_ts)) if raw_ts else datetime.now()
        except (ValueError, TypeError):
            extracted_at = datetime.now()

        # Parse indexed_at timestamp
        indexed_at_raw = data.get("indexed_at")
        indexed_at: Optional[datetime] = None
        if indexed_at_raw:
            try:
                indexed_at = datetime.fromisoformat(indexed_at_raw)
            except (ValueError, TypeError):
                pass

        # Build extracted entities
        raw_entities: List[dict] = data.get("extracted_entities") or []
        extracted_entities = [self._map_entity(e) for e in raw_entities]

        return CS2Evidence(
            # CHANGE 5: try both "evidence_id" and "id" as the primary key
            evidence_id=str(_get("evidence_id", "id", default="")),
            company_id=str(_get("company_id", "ticker", default="")),
            source_type=source_type or SourceType.SEC_10K_ITEM_1,  # safe default
            signal_category=signal_category or SignalCategory.DIGITAL_PRESENCE,
            # CHANGE 5: try both "content" and "chunk_text" (your Snowflake column name)
            content=str(_get("content", "chunk_text", default="")),
            extracted_at=extracted_at,
            confidence=float(_get("confidence", default=0.8)),
            fiscal_year=data.get("fiscal_year"),
            source_url=data.get("source_url"),
            page_number=data.get("page_number"),
            # CHANGE 3: capture filing_type, section, chunk_index from SEC chunks
            filing_type=_get("filing_type", default=None),
            section=_get("section", default=None),
            chunk_index=data.get("chunk_index"),
            extracted_entities=extracted_entities,
            indexed_in_cs4=bool(data.get("indexed_in_cs4", False)),
            indexed_at=indexed_at,
        )

    @staticmethod
    def _derive_signal_from_source(source_type: SourceType) -> SignalCategory:

        mapping = {
            SourceType.SEC_10K_ITEM_1:      SignalCategory.DIGITAL_PRESENCE,
            SourceType.SEC_10K_ITEM_1A:     SignalCategory.GOVERNANCE_SIGNALS,
            SourceType.SEC_10K_ITEM_7:      SignalCategory.LEADERSHIP_SIGNALS,
            SourceType.JOB_POSTING_LINKEDIN: SignalCategory.TECHNOLOGY_HIRING,
            SourceType.JOB_POSTING_INDEED:  SignalCategory.TECHNOLOGY_HIRING,
            SourceType.PATENT_USPTO:        SignalCategory.INNOVATION_ACTIVITY,
            SourceType.GLASSDOOR_REVIEW:    SignalCategory.CULTURE_SIGNALS,
            SourceType.BOARD_PROXY_DEF14A:  SignalCategory.GOVERNANCE_SIGNALS,
        }
        return mapping.get(source_type, SignalCategory.DIGITAL_PRESENCE)

    @staticmethod
    def _map_entity(data: dict) -> ExtractedEntity:
        """Map a raw entity dict to ExtractedEntity."""
        return ExtractedEntity(
            entity_type=data.get("entity_type", "unknown"),
            text=data.get("text", ""),
            char_start=int(data.get("char_start", 0)),
            char_end=int(data.get("char_end", 0)),
            confidence=float(data.get("confidence", 0.0)),
            attributes=data.get("attributes") or {},
        )

    def _get_client(self) -> httpx.AsyncClient:
        """Return the active async client, raising if not in context manager."""
        if self._client is None:
            raise RuntimeError(
                "CS2Client must be used as an async context manager: "
                "`async with CS2Client() as client:`"
            )
        return self._client