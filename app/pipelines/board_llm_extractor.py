# app/pipelines/board_llm_extractor.py
# Stage 3-4: LLM-powered director extraction + merge/deduplicate.

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

EXTRACTIONS_DIR = Path("data/board_extractions")
EXTRACTIONS_DIR.mkdir(parents=True, exist_ok=True)


# ── Pydantic schemas for structured LLM output ──────────────────────


class DirectorExtraction(BaseModel):
    name: str = Field(description="Full name of the director")
    title: Optional[str] = Field(default=None, description="Role/title if mentioned")
    committees: List[str] = Field(default_factory=list, description="Committee memberships")
    is_independent: Optional[bool] = Field(default=None, description="Whether independent")
    tenure_since_year: Optional[int] = Field(default=None, description="Year appointed/joined board")
    bio: Optional[str] = Field(default=None, description="Biographical summary (2-3 sentences)")
    evidence: List[str] = Field(default_factory=list, description="Supporting text snippets")


class ChunkExtraction(BaseModel):
    directors: List[DirectorExtraction] = Field(default_factory=list)


# ── System prompt for LLM extraction ────────────────────────────────

SYSTEM_PROMPT = """You are extracting board director information from SEC DEF 14A proxy statements.

Rules:
- Only extract actual board directors or director nominees (not executives who are not on the board, employees, or company names)
- Extract biographical details, committee memberships, independence status, and tenure
- For bio: write a 2-3 sentence summary of the person's professional background
- For committees: use the full committee name (e.g., "Audit Committee", "Compensation Committee")
- For is_independent: set to true only if the text explicitly says "independent" about this director
- For tenure_since_year: extract the year they joined the board (look for "since", "appointed", "elected", "joined" followed by a year)
- If information is not present in the chunk, use null — do NOT guess
- Ignore compensation tables, ownership tables, and financial data
- Precision over recall: only extract what you're confident about
"""


# ── Stage 3: LLM extraction ────────────────────────────────────────


def extract_from_chunks(
    chunks: List[Dict], ticker: str, use_cache: bool = True
) -> List[ChunkExtraction]:
    """Extract director info from all chunks using LLM.

    Caches results to data/board_extractions/{ticker}.json.
    """
    cache_file = EXTRACTIONS_DIR / f"{ticker}.json"

    if use_cache and cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            extractions = [ChunkExtraction(**e) for e in cached.get("extractions", [])]
            if extractions:
                logger.info(f"Loaded {len(extractions)} cached extractions for {ticker}")
                return extractions
        except Exception as e:
            logger.warning(f"Error loading extraction cache for {ticker}: {e}")

    from langchain_openai import ChatOpenAI
    from langchain_core.prompts import ChatPromptTemplate

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    structured_llm = llm.with_structured_output(ChunkExtraction)

    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "Extract board director information from this proxy statement section:\n\n{text}"),
    ])

    chain = prompt | structured_llm

    extractions: List[ChunkExtraction] = []

    for chunk in chunks:
        chunk_text = chunk.get("chunk_text", "")
        chunk_idx = chunk.get("chunk_index", 0)

        if not chunk_text.strip():
            continue

        try:
            result = chain.invoke({"text": chunk_text})
            if result and result.directors:
                logger.info(
                    f"  Chunk {chunk_idx}: extracted {len(result.directors)} directors"
                )
                extractions.append(result)
            else:
                logger.debug(f"  Chunk {chunk_idx}: no directors found")
        except Exception as e:
            logger.warning(f"  Chunk {chunk_idx}: LLM extraction failed: {e}")

    # Cache extractions
    cache_data = {
        "ticker": ticker,
        "extracted_at": datetime.now().isoformat(),
        "chunk_count": len(chunks),
        "extraction_count": len(extractions),
        "extractions": [e.model_dump() for e in extractions],
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, indent=2)
    logger.info(f"Cached {len(extractions)} extractions for {ticker}")

    return extractions


# ── Stage 4: Merge + deduplicate ────────────────────────────────────


def merge_extractions(all_extractions: List[ChunkExtraction]) -> List[Dict]:
    """Combine per-chunk director extractions, dedupe by name."""
    by_name: Dict[str, Dict] = {}

    for extraction in all_extractions:
        for d in extraction.directors:
            key = _normalize_name(d.name)
            if not key or len(key) < 3:
                continue
            if key in by_name:
                _merge_into(by_name[key], d)
            else:
                by_name[key] = d.model_dump()

    members = list(by_name.values())[:20]  # cap at 20

    # Convert tenure_since_year to tenure_years
    current_year = datetime.now().year
    for m in members:
        since = m.pop("tenure_since_year", None)
        if since and isinstance(since, int) and 1950 < since <= current_year:
            m["tenure_years"] = float(current_year - since)
        else:
            m["tenure_years"] = 0.0

        # Ensure defaults
        m.setdefault("title", "Director")
        m["title"] = m["title"] or "Director"
        m.setdefault("bio", "")
        m["bio"] = m["bio"] or ""
        m.setdefault("is_independent", False)
        m["is_independent"] = m["is_independent"] or False
        m.setdefault("committees", [])
        m.pop("evidence", None)

    logger.info(f"Merged to {len(members)} unique directors")
    return members


def _normalize_name(name: str) -> str:
    """Lowercase, strip suffixes, collapse whitespace."""
    name = name.lower().strip()
    for suffix in [", jr.", ", sr.", " jr.", " sr.", " jr", " sr", " iii", " ii", " iv", ", ph.d.", ", m.d."]:
        name = name.replace(suffix, "")
    # Remove titles
    for prefix in ["dr. ", "mr. ", "ms. ", "mrs. ", "gen. ", "adm. "]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    return re.sub(r"\s+", " ", name).strip()


def _merge_into(existing: Dict, new: DirectorExtraction) -> None:
    """Merge new extraction into existing record."""
    # Bio: keep longest
    if new.bio and len(new.bio) > len(existing.get("bio") or ""):
        existing["bio"] = new.bio

    # Title: prefer non-"Director" title
    if new.title and new.title != "Director" and (not existing.get("title") or existing["title"] == "Director"):
        existing["title"] = new.title

    # Committees: union
    existing_comms = set(c.lower() for c in existing.get("committees", []))
    for c in new.committees:
        if c.lower() not in existing_comms:
            existing.setdefault("committees", []).append(c)
            existing_comms.add(c.lower())

    # Independence: True overrides None/False
    if new.is_independent is True:
        existing["is_independent"] = True

    # Tenure: prefer explicit year
    if new.tenure_since_year and not existing.get("tenure_since_year"):
        existing["tenure_since_year"] = new.tenure_since_year

    # Evidence: accumulate
    existing.setdefault("evidence", []).extend(new.evidence or [])


# ── Quality gate ────────────────────────────────────────────────────


def quality_check(members: List[Dict]) -> tuple[bool, str]:
    """Validate extracted board members meet quality thresholds."""
    if len(members) < 5:
        return False, f"too_few_members={len(members)}"
    bios = [m.get("bio") or "" for m in members]
    good = sum(1 for b in bios if len(b) >= 50)
    if good / max(1, len(bios)) < 0.5:
        return False, f"low_bio_quality={good}/{len(bios)}"
    return True, "ok"
