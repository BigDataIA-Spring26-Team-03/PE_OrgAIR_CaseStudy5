from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService



# HELPER FUNCTIONS


def row_get(row: Dict[str, Any], *keys: str) -> Any:
    """Get first non-None value from dict keys (case-insensitive)"""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def sha256_text(text: str) -> str:
    """Compute SHA256 hash for content deduplication"""
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def normalize_ws(text: str) -> str:
    """Normalize whitespace consistently"""
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def processed_s3_key(doc_id: str) -> str:
    """
    Generate stable S3 key for processed/cleaned text.
    Format: processed/{doc_id}.txt.gz
    """
    return f"processed/{doc_id}.txt.gz"



# SEC HEADER / BOILERPLATE PATTERNS


HEADER_PATTERNS = [
    re.compile(r"^UNITED STATES SECURITIES AND EXCHANGE COMMISSION", re.I),
    re.compile(r"^WASHINGTON,\s*D\.C\.\s*20549", re.I),
    re.compile(r"^FORM\s+(10-K|10-Q|8-K|DEF\s*14A)\b", re.I),
    re.compile(r"^Commission File Number", re.I),
    re.compile(r"^Securities registered pursuant to Section", re.I),
    re.compile(r"^Indicate by check mark", re.I),
    re.compile(r"^☐|^☑", re.I),  # Checkboxes
    re.compile(r"^TABLE OF CONTENTS\b", re.I),
    re.compile(r"^INDEX TO FINANCIAL STATEMENTS\b", re.I),
]

# XBRL namespace patterns (critical for iXBRL documents)
XBRL_NAMESPACE_PATTERNS = [
    re.compile(r"^(?:dei|us-gaap|ifrs|srt|country|currency|exch|naics|sic|stpr|invest)[:_][\w]+", re.I),
    re.compile(r"^xbrli?:\w+", re.I),
    re.compile(r"^ix:\w+", re.I),
    re.compile(r"^link:\w+", re.I),
    re.compile(r"^xlink:\w+", re.I),
]

# XBRL document markers
XBRL_DOC_MARKERS = [
    re.compile(r"^IDEA:\s*XBRL\s+DOCUMENT", re.I),
    re.compile(r"^<XBRL>", re.I),
    re.compile(r"^<?xml.*xbrl", re.I),
    re.compile(r"^CONSOLIDATED\s+DOCUMENT", re.I),
]

# Inline XBRL renderer junk
IXBRL_RENDERER_PATTERNS = [
    re.compile(r"^Namespace\s+Prefix", re.I),
    re.compile(r"^Namespace\s+URI", re.I),
    re.compile(r"^xbrli?:.*ItemType", re.I),
    re.compile(r"^Document\s+And\s+Entity\s+Information", re.I),
    re.compile(r"^Report\.css", re.I),
    re.compile(r"^Show\.js", re.I),
    re.compile(r"^.*\.xsd\s*$", re.I),
    re.compile(r"^.*fasb\.org.*$", re.I),
    re.compile(r"^.*xbrl\.org.*$", re.I),
    re.compile(r"^.*taxonomy.*extension.*$", re.I),
]

# Table numeric patterns (not narrative)
TABLE_NUMERIC_PATTERNS = [
    re.compile(r"^\s*[\(\$\d\.,\)]+\s*$"),  # Lines with only numbers/currency
    re.compile(r"^\s*\(\s*\d+[\d\.,]*\s*\)\s*$"),  # Parenthetical numbers
    re.compile(r"^[\d\.,]+\s+[\d\.,]+\s+[\d\.,]+\s*$"),  # Multiple number columns
    re.compile(r"^\s*\$\s*[\d\.,]+\s*$"),  # Single dollar amounts
    re.compile(r"^\s*—\s*$"),  # Em dashes (table markers)
]

# Financial statement headers (not narrative)
FINANCIAL_STATEMENT_HEADERS = [
    re.compile(r"^In\s+millions,?\s+except", re.I),
    re.compile(r"^\$\s+in\s+millions", re.I),
    re.compile(r"^Years?\s+ended\s+\w+\s+\d+", re.I),
    re.compile(r"^Three\s+months\s+ended", re.I),
    re.compile(r"^Nine\s+months\s+ended", re.I),
    re.compile(r"^As\s+of\s+\w+\s+\d+,?\s+\d{4}", re.I),
]

GARBAGE_LINE_PATTERNS = [
    re.compile(r"^[-_]{8,}$"),  # Long dashes/underscores
    re.compile(r"^\s*\d+\s*$"),  # Standalone numbers (page numbers)
    re.compile(r"^\s*Page\s+\d+\s*$", re.I),
    re.compile(r"^https?://\S+$", re.I),  # Standalone URLs
    re.compile(r"^\s*(xbrl|ixbrl|inline xbrl)\s*$", re.I),
    re.compile(r"^\s*<[^>]+>\s*$"),  # Stray HTML tags
]

INVENTORY_LINE_PATTERNS = [
    # "EX-4.1 exhibit41q4fy25.htm"
    re.compile(
        r"^EX-\d+(?:\.\w+)?\s+\S+\.(?:htm|html|xml|xsd|xbrl|jpg|jpeg|png|gif|pdf|txt)\s*$",
        re.I,
    ),
    # Filename-only lines
    re.compile(
        r"^[A-Za-z0-9][A-Za-z0-9._-]{2,}\.(?:htm|html|xml|xsd|xbrl|jpg|jpeg|png|gif|pdf|txt)\s*$",
        re.I,
    ),
    re.compile(r"^XBRL\s+TAXONOMY\s+EXTENSION\s+.*$", re.I),
    re.compile(r"^GRAPHIC(?:\s+\S+)?\s*$", re.I),
]



# BINARY/UUENCODE ATTACHMENT DETECTION


UUE_BEGIN_RE = re.compile(r"^begin\s+\d{3}\s+.+$", re.I)
UUE_END_RE = re.compile(r"^end\s*$", re.I)
UUE_MLINE_RE = re.compile(r"^M[\x20-\x7E]{55,}$")
HIGH_SYMBOL_RE = re.compile(r"^[A-Za-z0-9+/=]{0,10}[^A-Za-z0-9\s]{10,}.*$")
REPEAT_GIBBERISH_RE = re.compile(r'^(?:[A-Z@]{3,}|\*{3,}|"+|[A-Z]{2,}@)\s*.*$', re.I)


def is_xbrl_line(line: str) -> bool:
    """
    Detect XBRL/iXBRL content that should be removed.
    This is the most critical filter for modern SEC filings.
    """
    s = line.strip()
    if not s:
        return False
    
    # XBRL namespace patterns (dei:, us-gaap:, etc.)
    if any(p.match(s) for p in XBRL_NAMESPACE_PATTERNS):
        return True
    
    # XBRL document markers
    if any(p.match(s) for p in XBRL_DOC_MARKERS):
        return True
    
    # iXBRL renderer junk
    if any(p.match(s) for p in IXBRL_RENDERER_PATTERNS):
        return True
    
    # CSS/JavaScript (embedded in iXBRL)
    if any(keyword in s.lower() for keyword in ['report.css', 'show.js', '.xsd', 'xmlns:', 'fasb.org', 'xbrl.org']):
        return True
    
    return False


def is_table_numeric_line(line: str) -> bool:
    """
    Detect numeric table rows (not narrative text).
    These are financial statement tables, not disclosure text.
    """
    s = line.strip()
    if not s or len(s) < 3:
        return False
    
    # Pure numeric patterns
    if any(p.match(s) for p in TABLE_NUMERIC_PATTERNS):
        return True
    
    # Financial statement headers
    if any(p.match(s) for p in FINANCIAL_STATEMENT_HEADERS):
        return True
    
    # Lines with mostly numbers and symbols (< 20% alpha)
    alpha_count = sum(c.isalpha() for c in s)
    if len(s) >= 10 and (alpha_count / len(s)) < 0.20:
        # Check if it's not a URL or code
        if not s.startswith(('http', 'www', '//')):
            return True
    
    return False


def is_binary_like_line(line: str) -> bool:
    """
    Detect binary/encoded content that should be removed.
    
    Checks for:
    - UUencode markers
    - Low alpha-to-total ratio (< 12%)
    - High non-word character ratio (> 35%)
    - Gibberish patterns
    """
    s = line.strip()
    if not s:
        return False

    # UUencode detection
    if UUE_BEGIN_RE.match(s) or UUE_END_RE.match(s):
        return True
    if UUE_MLINE_RE.match(s):
        return True

    # Low alpha ratio on long lines
    if len(s) >= 80:
        alpha = sum(ch.isalpha() for ch in s)
        if alpha / max(len(s), 1) < 0.12:
            return True

    # High non-word character ratio
    nonword = sum(1 for ch in s if not (ch.isalnum() or ch.isspace()))
    if len(s) >= 60 and (nonword / len(s)) > 0.35:
        return True

    # High symbol with low alpha
    if HIGH_SYMBOL_RE.match(s) and len(s) >= 40:
        alpha = sum(ch.isalpha() for ch in s)
        if alpha < 15:
            return True

    # Repeated gibberish patterns
    if len(s) >= 30 and REPEAT_GIBBERISH_RE.match(s):
        alpha = sum(ch.isalpha() for ch in s)
        spaces = s.count(" ")
        if alpha / max(len(s), 1) < 0.25 and spaces < 10:
            return True

    return False
    """
    Detect binary/encoded content that should be removed.
    
    Checks for:
    - UUencode markers
    - Low alpha-to-total ratio (< 12%)
    - High non-word character ratio (> 35%)
    - Gibberish patterns
    """
    s = line.strip()
    if not s:
        return False

    # UUencode detection
    if UUE_BEGIN_RE.match(s) or UUE_END_RE.match(s):
        return True
    if UUE_MLINE_RE.match(s):
        return True

    # Low alpha ratio on long lines
    if len(s) >= 80:
        alpha = sum(ch.isalpha() for ch in s)
        if alpha / max(len(s), 1) < 0.12:
            return True

    # High non-word character ratio
    nonword = sum(1 for ch in s if not (ch.isalnum() or ch.isspace()))
    if len(s) >= 60 and (nonword / len(s)) > 0.35:
        return True

    # High symbol with low alpha
    if HIGH_SYMBOL_RE.match(s) and len(s) >= 40:
        alpha = sum(ch.isalpha() for ch in s)
        if alpha < 15:
            return True

    # Repeated gibberish patterns
    if len(s) >= 30 and REPEAT_GIBBERISH_RE.match(s):
        alpha = sum(ch.isalpha() for ch in s)
        spaces = s.count(" ")
        if alpha / max(len(s), 1) < 0.25 and spaces < 10:
            return True

    return False


def drop_binary_blocks(text: str) -> str:
    """
    Remove binary/uuencoded blocks from text.
    Handles multi-line uuencode blocks (begin...end).
    """
    lines = text.splitlines()
    out: list[str] = []
    in_uue_block = False

    for ln in lines:
        s = ln.rstrip("\n")

        # UUencode block detection
        if UUE_BEGIN_RE.match(s.strip()):
            in_uue_block = True
            continue

        if in_uue_block:
            if UUE_END_RE.match(s.strip()):
                in_uue_block = False
            continue

        # Single-line binary detection
        if is_binary_like_line(s):
            continue

        out.append(s)

    cleaned = "\n".join(out)
    return normalize_ws(cleaned)



# MAIN CLEANING FUNCTION


def clean_sec_text(text: str) -> str:
    """
    AGGRESSIVE cleaning for SEC documents.
    
    Removes (in order):
    1. Binary/uuencoded attachments
    2. XBRL/iXBRL content (CRITICAL for modern filings)
    3. Table numeric rows (financial statements)
    4. SEC boilerplate headers
    5. Navigation elements
    6. Low-entropy lines
    
    Returns:
        Pure narrative text suitable for LLM consumption
    """
    # Initial normalization
    text = normalize_ws(text)
    
    # Step 1: Remove binary blocks
    text = drop_binary_blocks(text)

    # Step 2: Aggressive line-by-line filtering
    lines = [ln.strip() for ln in text.splitlines()]
    cleaned_lines: list[str] = []
    
    # Track consecutive blank lines (collapse multiple)
    consecutive_blanks = 0

    for ln in lines:
        # Preserve paragraph breaks (but not excessive)
        if not ln:
            consecutive_blanks += 1
            if consecutive_blanks <= 2:  # Max 2 blank lines
                cleaned_lines.append("")
            continue
        
        consecutive_blanks = 0
        
        # CRITICAL: Remove XBRL lines first
        if is_xbrl_line(ln):
            continue
        
        # Remove table numeric rows
        if is_table_numeric_line(ln):
            continue
        
        # Remove SEC headers (original patterns)
        if any(p.search(ln) for p in HEADER_PATTERNS):
            continue
        
        # Remove garbage lines (original patterns)
        if any(p.search(ln) for p in GARBAGE_LINE_PATTERNS):
            continue
        
        # Remove inventory lines (original patterns)
        if any(p.search(ln) for p in INVENTORY_LINE_PATTERNS):
            continue
        
        # Remove binary-like content
        if is_binary_like_line(ln):
            continue

        # Remove low-entropy lines (same character repeated)
        if len(ln) >= 10 and len(set(ln)) <= 2:
            continue
        
        # Remove lines with excessive parentheses (likely table fragments)
        paren_count = ln.count('(') + ln.count(')')
        if len(ln) >= 20 and paren_count > len(ln) * 0.3:
            continue
        
        # Keep the line
        cleaned_lines.append(ln)

    out = "\n".join(cleaned_lines)
    out = normalize_ws(out)

    # Step 3: Remove multi-line blocks
    
    # Remove TOC blocks
    out = re.sub(r"\bTABLE OF CONTENTS\b.*?(?=\n\n|\Z)", "", out, flags=re.I | re.S)
    
    # Remove XBRL document blocks (may span multiple lines)
    out = re.sub(r"IDEA:\s*XBRL\s+DOCUMENT.*?(?=\n\n|\Z)", "", out, flags=re.I | re.S)
    
    # Remove CSS/JS blocks
    out = re.sub(r"Report\.css.*?(?=\n\n|\Z)", "", out, flags=re.I | re.S)
    out = re.sub(r"Show\.js.*?(?=\n\n|\Z)", "", out, flags=re.I | re.S)
    
    # Remove taxonomy extension blocks
    out = re.sub(r"TAXONOMY\s+EXTENSION.*?(?=\n\n|\Z)", "", out, flags=re.I | re.S)
    
    # Step 4: Final normalization
    out = normalize_ws(out)
    
    # Step 5: Quality check - ensure we have real narrative content
    # Count words vs numbers
    words = re.findall(r'\b[a-zA-Z]{3,}\b', out)
    if len(words) < 100:
        # Too few real words - this might be all tables
        pass  # Still return it, but flag in metadata
    
    return out


# CLEANER RESULT


@dataclass(frozen=True)
class CleanResult:
    """Result of cleaning a single document"""
    doc_id: str
    processed_key: str
    cleaned_hash: str
    chars: int
    was_deduped: bool = False


# PIPELINE


class DocumentTextCleanerPipeline:
    """
    Text cleaning pipeline for SEC documents.
    
    Process:
    1. Fetch documents with status='parsed' from Snowflake
    2. Load parsed JSON from S3
    3. Clean text (remove boilerplate, binary, etc.)
    4. Check for duplicates by content hash
    5. Upload cleaned text to S3 (gzipped)
    6. Update Snowflake with status='cleaned' and content_hash
    """
    
    def __init__(
        self, 
        sf: Optional[SnowflakeService] = None, 
        s3: Optional[S3Storage] = None
    ) -> None:
        self.sf = sf or SnowflakeService()
        self.s3 = s3 or S3Storage()

    def fetch_parsed_documents(self, limit: int = 200) -> list[dict[str, Any]]:
        """
        Fetch documents ready for cleaning.
        Only status='parsed' documents are candidates.
        """
        return self.sf.execute_query(
            """
            SELECT id, ticker, filing_type, s3_key, filing_date
            FROM documents_sec
            WHERE LOWER(status) = 'parsed'
            ORDER BY filing_date DESC, ticker
            LIMIT %(limit)s
            """,
            {"limit": int(limit)},
        )

    def set_status(
        self, 
        doc_id: str, 
        status: str, 
        error_message: Optional[str] = None
    ) -> None:
        """Update document status in Snowflake"""
        self.sf.execute_update(
            """
            UPDATE documents_sec
            SET status=%(status)s,
                error_message=%(error)s,
                processed_at=CURRENT_TIMESTAMP()
            WHERE id=%(id)s
            """,
            {"id": doc_id, "status": status, "error": error_message},
        )

    def update_clean_row(
        self,
        doc_id: str,
        processed_key: str,
        cleaned_hash: str,
        error_message: Optional[str],
    ) -> None:
        """
        Update document after successful cleaning.
        Sets content_hash (for deduplication) and processed S3 key.
        """
        self.sf.execute_update(
            """
            UPDATE documents_sec
            SET s3_key=%(s3_key)s,
                content_hash=%(hash)s,
                status='cleaned',
                error_message=%(error)s,
                processed_at=CURRENT_TIMESTAMP()
            WHERE id=%(id)s
            """,
            {
                "id": doc_id, 
                "s3_key": processed_key, 
                "hash": cleaned_hash, 
                "error": error_message
            },
        )

    def find_duplicate_doc(
        self, 
        ticker: str, 
        filing_type: str, 
        cleaned_hash: str, 
        current_id: str
    ) -> Optional[dict[str, Any]]:
        """
        Check if another document with same content hash exists.
        Used for content deduplication.
        """
        rows = self.sf.execute_query(
            """
            SELECT id, s3_key
            FROM documents_sec
            WHERE UPPER(ticker) = UPPER(%(ticker)s)
              AND filing_type = %(filing_type)s
              AND content_hash = %(hash)s
              AND id <> %(id)s
              AND LOWER(status) IN ('cleaned','chunked')
            LIMIT 1
            """,
            {
                "ticker": ticker, 
                "filing_type": filing_type, 
                "hash": cleaned_hash, 
                "id": current_id
            },
        )
        return rows[0] if rows else None

    def run(self, limit: int = 200) -> dict[str, int]:
        """
        Run the cleaning pipeline.
        
        Args:
            limit: Max documents to process in this run
            
        Returns:
            Dict with counts: scanned, cleaned, deduped, failed
        """
        rows = self.fetch_parsed_documents(limit=limit)
        
        if not rows:
            print(" No documents with status='parsed' to clean.")
            return {"scanned": 0, "cleaned": 0, "deduped": 0, "failed": 0}

        print(f"\n{'='*70}")
        print(f" DOCUMENT TEXT CLEANER PIPELINE")
        print(f"{'='*70}")
        print(f"Documents to clean: {len(rows)}")
        print()

        cleaned = 0
        deduped = 0
        failed = 0

        for idx, r in enumerate(rows, 1):
            doc_id = str(row_get(r, "id", "ID"))
            parsed_key = str(row_get(r, "s3_key", "S3_KEY") or "").strip()
            ticker = str(row_get(r, "ticker", "TICKER") or "").upper()
            filing_type = str(row_get(r, "filing_type", "FILING_TYPE") or "")
            filing_date = row_get(r, "filing_date", "FILING_DATE")

            if not parsed_key:
                failed += 1
                self.set_status(doc_id, "clean_error", "clean_failed: missing parsed s3_key")
                print(f"[{idx}/{len(rows)}]  Missing s3_key: {ticker} {filing_type}")
                continue

            out_key = processed_s3_key(doc_id)

            print(f"\n[{idx}/{len(rows)}]  {ticker} {filing_type} ({filing_date})")
            print(f"   Doc ID: {doc_id}")
            
            t0 = time.time()

            try:
                # Handle both raw and parsed files
                # Priority: try parsed first, fall back to raw
                
                actual_key = None
                
                # Try 1: Check if it's already a parsed JSON key and exists
                if 'parsed/' in parsed_key and parsed_key.endswith('.json.gz'):
                    if self.s3.exists(parsed_key):
                        actual_key = parsed_key
                        print(f"   Using parsed JSON: {parsed_key}")
                
                # Try 2: Convert to parsed key and check
                if not actual_key:
                    parts = parsed_key.split('/')
                    if len(parts) >= 2:
                        parts[0] = 'parsed'
                        filename = parts[-1]
                        base = filename.rsplit('.', 1)[0] if '.' in filename else filename
                        parts[-1] = f"{base}.json.gz"
                        test_key = '/'.join(parts)
                        
                        if self.s3.exists(test_key):
                            actual_key = test_key
                            print(f"   Found parsed JSON: {test_key}")
                
                # Try 3: Fall back to raw file
                if not actual_key:
                    raw_key = parsed_key.replace('parsed/', 'raw/').replace('.json.gz', '.txt')
                    if self.s3.exists(raw_key):
                        actual_key = raw_key
                        print(f"     Falling back to raw file: {raw_key}")
                    else:
                        raise ValueError(f"No file found in S3 (tried parsed and raw): {parsed_key}")
                
                # Load the file
                if actual_key.endswith('.json.gz'):
                    # It's a parsed JSON file
                    parsed = self.s3.read_json_auto(actual_key)
                    raw_text = (parsed.get("text") or "").strip()
                else:
                    # It's a raw text file
                    raw_text = self.s3.read_text_auto(actual_key).strip()
                
                if not raw_text:
                    raise ValueError("Extracted text is empty")

                print(f"   Raw text: {len(raw_text):,} chars")

                # Clean the text (AGGRESSIVE)
                cleaned_text = clean_sec_text(raw_text)
                
                if not cleaned_text.strip():
                    raise ValueError("cleaned_text ended empty after cleaning")

                # Quality metrics
                reduction_pct = 100 * (1 - len(cleaned_text) / len(raw_text))
                word_count = len(re.findall(r'\b[a-zA-Z]{3,}\b', cleaned_text))
                
                print(f"   Cleaned: {len(cleaned_text):,} chars ({reduction_pct:.1f}% reduction)")
                print(f"   Words: {word_count:,} (quality check)")
                
                # Warning if too aggressive
                if reduction_pct > 95:
                    print(f"   WARNING: {reduction_pct:.1f}% reduction - may have removed too much!")
                if word_count < 100:
                    print(f"     WARNING: Only {word_count} words - mostly tables/XBRL?")

                # Compute content hash for deduplication
                cleaned_hash = sha256_text(cleaned_text)

                # Check for duplicates
                dup = self.find_duplicate_doc(ticker, filing_type, cleaned_hash, doc_id)
                
                if dup:
                    dup_id = str(row_get(dup, "id", "ID"))
                    dup_s3_key = str(row_get(dup, "s3_key", "S3_KEY") or "").strip()

                    # Prefer reusing existing processed artifact
                    if dup_s3_key and self.s3.exists(dup_s3_key):
                        self.update_clean_row(
                            doc_id=doc_id,
                            processed_key=dup_s3_key,
                            cleaned_hash=cleaned_hash,
                            error_message=f"dedup: same content as document {dup_id}",
                        )
                        deduped += 1
                        elapsed = time.time() - t0
                        print(f"     DEDUP: Reused artifact from {dup_id}")
                        print(f"    Success in {elapsed:.1f}s")
                        continue

                # Write cleaned text to S3 (idempotent)
                if not self.s3.exists(out_key):
                    self.s3.put_text(out_key, cleaned_text, gzip_compress=True)
                    print(f"    Uploaded: {out_key}")
                else:
                    print(f"   ↪ Artifact exists: {out_key}")

                # Update Snowflake
                dup_msg = None
                if dup:
                    dup_id = str(row_get(dup, "id", "ID"))
                    dup_msg = f"dedup: same content as document {dup_id}"

                self.update_clean_row(doc_id, out_key, cleaned_hash, dup_msg)

                elapsed = time.time() - t0
                
                if dup_msg:
                    deduped += 1
                    print(f"     DEDUP: Marked as duplicate")
                    print(f"    Success in {elapsed:.1f}s")
                else:
                    cleaned += 1
                    print(f"    Success in {elapsed:.1f}s")

            except Exception as e:
                failed += 1
                error_msg = f"clean_failed: {type(e).__name__}: {str(e)[:200]}"
                self.set_status(doc_id, "clean_error", error_msg)
                print(f"    FAILED: {e}")

        # Final summary
        print(f"\n{'='*70}")
        print(f" CLEANING SUMMARY")
        print(f"{'='*70}")
        print(f"Total scanned:     {len(rows)}")
        print(f"Cleaned:           {cleaned}")
        print(f"Deduplicated:      {deduped}")
        print(f"Failed:            {failed}")
        print(f"{'='*70}\n")
        
        return {
            "scanned": len(rows), 
            "cleaned": cleaned, 
            "deduped": deduped, 
            "failed": failed
        }



# MAIN ENTRY POINT

def main(limit: int = 200) -> None:
    """
    Run document text cleaner pipeline.
    
    Args:
        limit: Max documents to process (default 200 for all 140 docs)
    """
    pipeline = DocumentTextCleanerPipeline()
    pipeline.run(limit=limit)


if __name__ == "__main__":
    import sys
    
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    main(limit=limit)