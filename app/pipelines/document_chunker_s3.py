from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService


# GRADING STANDARDS

MIN_WORDS = 500
MAX_WORDS = 1000
OVERLAP_WORDS = 75  # 50–100 allowed



# HELPER FUNCTIONS


def row_get(row: Dict[str, Any], *keys: str) -> Any:
    """Get first non-None value from dict keys"""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def normalize_ws(text: str) -> str:
    """Normalize whitespace"""
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def word_count(text: str) -> int:
    """Count words in text"""
    return len(text.split())


def filing_type_norm(filing_type: str) -> str:
    """Normalize filing type"""
    return filing_type.upper().strip().replace(" ", "").replace("-", "")


def take_overlap_words(text: str, overlap: int) -> str:
    """Take last N words for overlap"""
    words = text.split()
    if not words:
        return ""
    return " ".join(words[-overlap:])



# SENTENCE SPLITTING


_SENTENCE_SPLIT_RE = re.compile(r"(?<=[\.!?])\s+(?=[A-Z0-9])")


def split_sentences(text: str) -> List[str]:
    """Split text into sentences"""
    text = text.strip()
    if not text:
        return []
    parts = _SENTENCE_SPLIT_RE.split(text)
    return [p.strip() for p in parts if p.strip()]


def sentence_aware_split(text: str, max_words: int, overlap_words: int) -> List[str]:
    """
    Split long text by sentence boundaries.
    Falls back to word splitting if sentences are too long.
    """
    sents = split_sentences(text)
    
    # Fallback: split by words if no sentences
    if not sents:
        words = text.split()
        out: List[str] = []
        step = max(1, max_words - overlap_words)
        i = 0
        while i < len(words):
            out.append(" ".join(words[i : i + max_words]).strip())
            i += step
        return out

    out: List[str] = []
    buf: List[str] = []
    buf_w = 0

    def flush() -> None:
        nonlocal buf, buf_w
        if buf:
            out.append(" ".join(buf).strip())
        buf = []
        buf_w = 0

    for s in sents:
        w = word_count(s)
        
        # If single sentence > max, split it by words
        if w > max_words:
            flush()
            words = s.split()
            step = max(1, max_words - overlap_words)
            i = 0
            while i < len(words):
                out.append(" ".join(words[i : i + max_words]).strip())
                i += step
            continue

        # Add to buffer if fits
        if buf_w + w <= max_words:
            buf.append(s)
            buf_w += w
        else:
            flush()
            buf.append(s)
            buf_w = w

    flush()

    # Add overlap between chunks
    if overlap_words > 0 and len(out) > 1:
        overlapped: List[str] = []
        prev = ""
        for c in out:
            if prev:
                ov = take_overlap_words(prev, overlap_words)
                if ov:
                    merged = (ov + " " + c).strip()
                    overlapped.append(merged if word_count(merged) <= max_words else c)
                else:
                    overlapped.append(c)
            else:
                overlapped.append(c)
            prev = c
        out = overlapped

    return out



# SECTION DETECTION


@dataclass(frozen=True)
class SectionSlice:
    """Represents a section of the document"""
    section: str
    start: int
    end: int
    text: str


def _dedupe_hits(hits: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    """Remove duplicate section hits that are too close"""
    hits.sort(key=lambda x: x[0])
    out: List[Tuple[int, str]] = []
    last = -10_000
    for pos, label in hits:
        if pos - last < 250:
            continue
        out.append((pos, label))
        last = pos
    return out


def find_section_boundaries(text: str, filing_type: str) -> List[Tuple[int, str]]:
    """Find section boundaries in SEC filings"""
    t = "\n" + text + "\n"
    ft = filing_type_norm(filing_type)
    patterns: List[Tuple[str, str]] = []

    if ft == "10K":
        patterns = [
            (r"\n\s*ITEM\s+1\s*[\.\:\-]\s+", "Item 1"),
            (r"\n\s*ITEM\s+1A\s*[\.\:\-]\s+", "Item 1A"),
            (r"\n\s*ITEM\s+1B\s*[\.\:\-]\s+", "Item 1B"),
            (r"\n\s*ITEM\s+7\s*[\.\:\-]\s+", "Item 7"),
            (r"\n\s*ITEM\s+7A\s*[\.\:\-]\s+", "Item 7A"),
        ]
    elif ft == "10Q":
        patterns = [
            (r"\n\s*ITEM\s+1A\s*[\.\:\-]\s+", "Item 1A"),
            (r"\n\s*ITEM\s+2\s*[\.\:\-]\s+", "Item 2"),
        ]
    elif ft == "8K":
        patterns = [
            (r"\n\s*ITEM\s+1\.01\s*[\.\:\-]\s+", "Item 1.01"),
            (r"\n\s*ITEM\s+2\.02\s*[\.\:\-]\s+", "Item 2.02"),
            (r"\n\s*ITEM\s+5\.02\s*[\.\:\-]\s+", "Item 5.02"),
            (r"\n\s*ITEM\s+7\.01\s*[\.\:\-]\s+", "Item 7.01"),
            (r"\n\s*ITEM\s+8\.01\s*[\.\:\-]\s+", "Item 8.01"),
        ]
    elif ft == "DEF14A":
        patterns = [
            (r"\n\s*EXECUTIVE\s+COMPENSATION\s*\n", "Executive Compensation"),
            (r"\n\s*COMPENSATION\s+DISCUSSION\s+AND\s+ANALYSIS\s*\n", "CD&A"),
            (r"\n\s*DIRECTOR\s+COMPENSATION\s*\n", "Director Compensation"),
            (r"\n\s*NAMED\s+EXECUTIVE\s+OFFICERS?\s*\n", "Named Executive Officers"),
        ]

    hits: List[Tuple[int, str]] = []
    for pat, label in patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            hits.append((m.start(), label))

    return _dedupe_hits(hits)


def slice_sections(text: str, filing_type: str) -> List[SectionSlice]:
    """Split document into sections"""
    boundaries = find_section_boundaries(text, filing_type)
    
    if not boundaries:
        return [SectionSlice(section="Unknown", start=0, end=len(text), text=text)]

    slices: List[SectionSlice] = []
    first_pos = boundaries[0][0]

    # Add intro section if substantial content before first section
    if first_pos > 900:
        intro = text[:first_pos].strip()
        if intro:
            slices.append(SectionSlice(section="Intro", start=0, end=first_pos, text=intro))

    # Extract each section
    for i, (pos, label) in enumerate(boundaries):
        start = max(pos - 1, 0)
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(text)
        sec_text = text[start:end].strip()
        if sec_text:
            slices.append(SectionSlice(section=label, start=start, end=end, text=sec_text))

    return slices if slices else [SectionSlice(section="Unknown", start=0, end=len(text), text=text)]



# SEMANTIC BLOCK DETECTION


def is_noise_block(b: str) -> bool:
    """Detect table-like or low-content blocks"""
    b = b.strip()
    if not b:
        return True

    wc = word_count(b)
    if wc < 10:
        return True

    # Table-ish: many short lines
    lines = b.splitlines()
    if len(lines) >= 10:
        short_lines = sum(1 for ln in lines if len(ln.split()) <= 6)
        if short_lines / max(len(lines), 1) > 0.65:
            return True

    # Low alpha ratio (mostly numbers/symbols)
    letters = sum(ch.isalpha() for ch in b)
    if wc < 50 and letters / max(len(b), 1) < 0.10:
        return True

    return False


def split_semantic_blocks(text: str) -> List[str]:
    """Split text into semantic blocks (paragraphs)"""
    parts = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]

    blocks: List[str] = []
    carry: List[str] = []

    for p in parts:
        if is_noise_block(p):
            carry.append(p)
            continue

        if carry:
            blocks.append("\n".join(carry + [p]).strip())
            carry = []
        else:
            blocks.append(p)

    if carry:
        if blocks:
            blocks[-1] = (blocks[-1] + "\n" + "\n".join(carry)).strip()
        else:
            blocks = ["\n".join(carry).strip()]

    # Merge tiny blocks to ~150 words
    merged: List[str] = []
    buf: List[str] = []
    buf_w = 0

    for b in blocks:
        w = word_count(b)
        if buf_w + w < 150:
            buf.append(b)
            buf_w += w
            continue

        if buf:
            merged.append("\n\n".join(buf).strip())
            buf = []
            buf_w = 0

        merged.append(b)

    if buf:
        merged.append("\n\n".join(buf).strip())

    return [m for m in merged if m.strip()]



# CHUNK BUILDER (500-1000 words)


def build_chunks_for_section(sec_text: str) -> List[str]:
    """Build 500-1000 word chunks from section text"""
    blocks = split_semantic_blocks(sec_text)
    if not blocks:
        return []

    # Expand huge blocks first
    expanded: List[str] = []
    for b in blocks:
        if word_count(b) > MAX_WORDS:
            expanded.extend(sentence_aware_split(b, MAX_WORDS, 0))
        else:
            expanded.append(b)

    chunks: List[str] = []
    buf: List[str] = []
    buf_w = 0

    def flush() -> None:
        nonlocal buf, buf_w
        if not buf:
            return
        c = "\n\n".join(buf).strip()
        if c:
            chunks.append(c)
        buf = []
        buf_w = 0

    for b in expanded:
        w = word_count(b)

        # If single block is in range and buffer empty, keep it
        if not buf and MIN_WORDS <= w <= MAX_WORDS:
            chunks.append(b.strip())
            continue

        if buf_w + w <= MAX_WORDS:
            buf.append(b)
            buf_w += w
        else:
            flush()
            buf.append(b)
            buf_w = w
            if buf_w > MAX_WORDS:
                flush()

    flush()

    # Merge small chunks
    if len(chunks) > 1:
        merged: List[str] = []
        i = 0
        while i < len(chunks):
            c = chunks[i]
            wc = word_count(c)

            if wc >= MIN_WORDS:
                merged.append(c)
                i += 1
                continue

            # Try merge forward
            if i + 1 < len(chunks):
                combo = (c + "\n\n" + chunks[i + 1]).strip()
                if word_count(combo) <= MAX_WORDS:
                    merged.append(combo)
                    i += 2
                    continue

            # Try merge backward
            if merged:
                combo = (merged[-1] + "\n\n" + c).strip()
                if word_count(combo) <= MAX_WORDS:
                    merged[-1] = combo
                    i += 1
                    continue

            merged.append(c)
            i += 1

        chunks = merged

    # Add overlap
    if OVERLAP_WORDS > 0 and len(chunks) > 1:
        out: List[str] = []
        prev = ""
        for c in chunks:
            if prev:
                ov = take_overlap_words(prev, OVERLAP_WORDS)
                if ov:
                    mc = (ov + " " + c).strip()
                    out.append(mc if word_count(mc) <= MAX_WORDS else c)
                else:
                    out.append(c)
            else:
                out.append(c)
            prev = c
        chunks = out

    # Final guard: force split if still > MAX
    final: List[str] = []
    for c in chunks:
        if word_count(c) <= MAX_WORDS:
            final.append(c)
        else:
            final.extend(sentence_aware_split(c, MAX_WORDS, OVERLAP_WORDS))

    return [normalize_ws(x) for x in final if normalize_ws(x)]


def find_char_span(doc_text: str, chunk_text: str) -> Tuple[Optional[int], Optional[int]]:
    """Find character offsets of chunk in document"""
    idx = doc_text.find(chunk_text)
    if idx == -1:
        return None, None
    return idx, idx + len(chunk_text)



# CHUNK ROW MODEL


@dataclass(frozen=True)
class ChunkRow:
    """Represents a chunk to be inserted into database"""
    id: str
    document_id: str
    chunk_index: int
    content: str
    section: str
    start_char: Optional[int]
    end_char: Optional[int]
    word_count: int



# CHUNKER PIPELINE


class DocumentChunkerPipeline:
    """
    Document chunker pipeline.
    
    Process:
    1. Fetch documents with status='cleaned'
    2. Load cleaned text from S3 (processed/*.txt.gz)
    3. Split into sections (Item 1, 1A, 7, etc.)
    4. Create 500-1000 word chunks with 75-word overlap
    5. Insert chunks into document_chunks_sec table
    6. Update document status to 'chunked'
    """
    
    def __init__(
        self,
        sf: Optional[SnowflakeService] = None,
        s3: Optional[S3Storage] = None,
    ) -> None:
        self.sf = sf or SnowflakeService()
        self.s3 = s3 or S3Storage()

    def fetch_cleaned_documents(self, limit: int) -> List[Dict[str, Any]]:
        """Fetch documents ready for chunking"""
        return self.sf.execute_query(
            """
            SELECT id, ticker, filing_type, s3_key
            FROM documents_sec
            WHERE status='cleaned'
              AND s3_key IS NOT NULL
            ORDER BY ticker, filing_type
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )

    def existing_chunk_count(self, doc_id: str) -> int:
        """Check if document already has chunks"""
        rows = self.sf.execute_query(
            "SELECT COUNT(*) AS cnt FROM document_chunks_sec WHERE document_id=%(id)s",
            {"id": doc_id},
        )
        if not rows:
            return 0
        v = rows[0].get("CNT") if "CNT" in rows[0] else rows[0].get("cnt")
        return int(v or 0)

    def insert_chunks_batch(self, rows: List[ChunkRow]) -> None:
        """Insert chunks in batch (avoid parameter explosion)"""
        if not rows:
            return

        values_sql: List[str] = []
        params: Dict[str, Any] = {}

        for i, ch in enumerate(rows):
            values_sql.append(
                f"(%(id{i})s, %(document_id{i})s, %(chunk_index{i})s, %(content{i})s, "
                f"%(section{i})s, %(start_char{i})s, %(end_char{i})s, %(word_count{i})s)"
            )
            params[f"id{i}"] = ch.id
            params[f"document_id{i}"] = ch.document_id
            params[f"chunk_index{i}"] = ch.chunk_index
            params[f"content{i}"] = ch.content
            params[f"section{i}"] = ch.section or "Unknown"
            params[f"start_char{i}"] = ch.start_char
            params[f"end_char{i}"] = ch.end_char
            params[f"word_count{i}"] = ch.word_count

        sql = f"""
        INSERT INTO document_chunks_sec
            (id, document_id, chunk_index, content, section, start_char, end_char, word_count)
        VALUES
            {", ".join(values_sql)}
        """
        self.sf.execute_update(sql, params)

    def mark_chunked(self, doc_id: str, chunk_count: int) -> None:
        """Update document status to chunked"""
        self.sf.execute_update(
            """
            UPDATE documents_sec
            SET status='chunked',
                chunk_count=%(cnt)s,
                processed_at=CURRENT_TIMESTAMP(),
                error_message=NULL
            WHERE id=%(id)s
            """,
            {"id": doc_id, "cnt": chunk_count},
        )

    def mark_error(self, doc_id: str, msg: str) -> None:
        """Mark document as failed"""
        self.sf.execute_update(
            """
            UPDATE documents_sec
            SET status='chunk_error',
                error_message=%(msg)s
            WHERE id=%(id)s
            """,
            {"id": doc_id, "msg": msg},
        )

    def run(self, limit: int = 200) -> None:
        """Run chunking pipeline"""
        docs = self.fetch_cleaned_documents(limit=limit)
        
        if not docs:
            print("No documents with status='cleaned' to chunk.")
            return

        print(f"\n{'='*70}")
        print(f"DOCUMENT CHUNKER PIPELINE")
        print(f"{'='*70}")
        print(f"Documents to chunk: {len(docs)}")
        print(f"Target: 500-1000 words, 75-word overlap")
        print()

        scanned = skipped = failed = 0
        inserted_chunks = 0

        for idx, d in enumerate(docs, 1):
            doc_id = str(row_get(d, "id", "ID"))
            ticker = str(row_get(d, "ticker", "TICKER") or "").upper()
            filing_type = str(row_get(d, "filing_type", "FILING_TYPE") or "")
            processed_key = str(row_get(d, "s3_key", "S3_KEY") or "").strip()

            scanned += 1
            print(f"\n[{idx}/{len(docs)}] {ticker} {filing_type}")
            print(f"   Doc ID: {doc_id}")

            try:
                # Check if already chunked
                existing = self.existing_chunk_count(doc_id)
                if existing > 0:
                    skipped += 1
                    self.mark_chunked(doc_id, chunk_count=existing)
                    print(f"   ↪Already chunked: {existing} chunks")
                    continue

                if not processed_key:
                    raise ValueError("s3_key is NULL (must point to processed text)")

                # Validate s3_key points to processed artifact
                if not processed_key.startswith("processed/"):
                    raise ValueError(
                        f"s3_key must point to processed/* but got: {processed_key}"
                    )

                t0 = time.time()

                # Load cleaned text from S3
                doc_text = normalize_ws(self.s3.read_text_auto(processed_key))
                if not doc_text.strip():
                    raise ValueError("processed text is empty")

                print(f"   Text: {len(doc_text):,} chars")

                # Split into sections
                sections = slice_sections(doc_text, filing_type)
                print(f"   Sections: {[s.section for s in sections]}")

                # Build chunks
                chunk_rows: List[ChunkRow] = []
                chunk_idx = 0

                for sec in sections:
                    sec_label = (sec.section or "Unknown").strip() or "Unknown"
                    sec_chunks = build_chunks_for_section(sec.text)

                    for c in sec_chunks:
                        c = normalize_ws(c)
                        wc = word_count(c)
                        if wc == 0:
                            continue

                        # Hard guard: if still > MAX, split again
                        if wc > MAX_WORDS:
                            for sub in sentence_aware_split(c, MAX_WORDS, OVERLAP_WORDS):
                                sub = normalize_ws(sub)
                                swc = word_count(sub)
                                if swc == 0:
                                    continue
                                s, e = find_char_span(doc_text, sub)
                                chunk_rows.append(
                                    ChunkRow(
                                        id=str(uuid4()),
                                        document_id=doc_id,
                                        chunk_index=chunk_idx,
                                        content=sub,
                                        section=sec_label,
                                        start_char=s,
                                        end_char=e,
                                        word_count=swc,
                                    )
                                )
                                chunk_idx += 1
                            continue

                        s, e = find_char_span(doc_text, c)
                        chunk_rows.append(
                            ChunkRow(
                                id=str(uuid4()),
                                document_id=doc_id,
                                chunk_index=chunk_idx,
                                content=c,
                                section=sec_label,
                                start_char=s,
                                end_char=e,
                                word_count=wc,
                            )
                        )
                        chunk_idx += 1

                if not chunk_rows:
                    raise ValueError("No chunks produced")

                # Show chunk statistics
                word_counts = [ch.word_count for ch in chunk_rows]
                avg_words = sum(word_counts) / len(word_counts)
                print(f"    Chunks: {len(chunk_rows)} (avg {avg_words:.0f} words)")

                # Batch insert
                BATCH_SIZE = 75
                for i in range(0, len(chunk_rows), BATCH_SIZE):
                    self.insert_chunks_batch(chunk_rows[i : i + BATCH_SIZE])

                self.mark_chunked(doc_id, chunk_count=len(chunk_rows))

                elapsed = time.time() - t0
                inserted_chunks += len(chunk_rows)
                print(f"    Success in {elapsed:.1f}s")

            except Exception as e:
                failed += 1
                error_msg = f"chunk_failed: {type(e).__name__}: {str(e)[:200]}"
                self.mark_error(doc_id, error_msg)
                print(f"   FAILED: {e}")

        # Summary
        print(f"\n{'='*70}")
        print(f" CHUNKING SUMMARY")
        print(f"{'='*70}")
        print(f"Docs scanned:      {scanned}")
        print(f"Docs chunked:      {scanned - skipped - failed}")
        print(f"Docs skipped:      {skipped}")
        print(f"Docs failed:       {failed}")
        print(f"Chunks inserted:   {inserted_chunks:,}")
        if scanned - skipped - failed > 0:
            print(f"Avg chunks/doc:    {inserted_chunks / (scanned - skipped - failed):.1f}")
        print(f"{'='*70}\n")


# MAIN ENTRY POINT


def main(limit: int = 200) -> None:
    """
    Run document chunker pipeline.
    
    Args:
        limit: Max documents to process (default 200 for all 140 docs)
    """
    pipeline = DocumentChunkerPipeline()
    pipeline.run(limit=limit)


if __name__ == "__main__":
    import sys
    
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    main(limit=limit)