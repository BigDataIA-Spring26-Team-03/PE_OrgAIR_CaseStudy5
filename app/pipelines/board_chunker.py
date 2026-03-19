# app/pipelines/board_chunker.py
# Stage 2: Chunk DEF 14A proxy text for LLM extraction.

from __future__ import annotations

import re
import logging
from typing import List, Dict

from app.pipelines.document_chunker_s3 import sentence_aware_split

logger = logging.getLogger(__name__)

# Chunk parameters tuned for proxy statements
MAX_WORDS = 2000
OVERLAP_WORDS = 200


def chunk_proxy_text(raw_text: str) -> List[Dict]:
    """Split proxy text into overlapping chunks, filtering garbage.

    Returns list of dicts with chunk_index and chunk_text.
    """
    if not raw_text or not raw_text.strip():
        return []

    chunks = sentence_aware_split(raw_text, max_words=MAX_WORDS, overlap_words=OVERLAP_WORDS)

    filtered = []
    for i, chunk in enumerate(chunks):
        if _is_garbage_chunk(chunk):
            continue
        filtered.append({"chunk_index": i, "chunk_text": chunk})

    logger.info(f"Chunked proxy text: {len(chunks)} total -> {len(filtered)} after filtering")
    return filtered


def _is_garbage_chunk(text: str) -> bool:
    """Filter TOC, numeric tables, compensation sections."""
    if not text or len(text.strip()) < 50:
        return True

    # Low alpha ratio -> numeric-heavy table
    alpha = sum(1 for c in text if c.isalpha())
    if len(text) > 0 and alpha / len(text) < 0.4:
        return True

    lower = text.lower()
    garbage_signals = [
        "table of contents",
        "beneficial ownership",
        "compensation discussion and analysis",
        "pay versus performance",
        "summary compensation table",
        "outstanding equity awards",
        "option exercises and stock vested",
        "nonqualified deferred compensation",
        "potential payments upon termination",
        "ceo pay ratio",
    ]
    # Only filter if the garbage signal appears in the first 200 chars
    # (section header), not deep in the text
    first_200 = lower[:200]
    if any(sig in first_200 for sig in garbage_signals):
        return True

    return False
