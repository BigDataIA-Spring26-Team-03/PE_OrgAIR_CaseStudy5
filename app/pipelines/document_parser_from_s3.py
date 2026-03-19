from __future__ import annotations

import hashlib
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import pdfplumber
from bs4 import BeautifulSoup

from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService



# CONFIGURATION & GUARDRAILS


# Parser limits (prevent hangs on massive docs)
MAX_HTML_BYTES = 15_000_000
MAX_TABLES_SCAN = 800
MAX_TABLES_EMIT = 300
MAX_ROWS_PER_TABLE = 250
MAX_CELLS_PER_ROW = 50
PRINT_EVERY_N_TABLES = 100

# AI keyword patterns for frequency analysis
AI_KEYWORDS = [
    "artificial intelligence", "machine learning", "deep learning",
    "neural network", "natural language processing", "nlp",
    "computer vision", "generative ai", "gen ai", "large language model",
    "llm", "ai model", "automation", "predictive analytics"
]

# Technology terms to track
TECH_KEYWORDS = [
    "cloud", "data science", "big data", "analytics",
    "digital transformation", "api", "platform", "saas"
]



# UTILITY FUNCTIONS


def parsed_s3_key(raw_key: str) -> str:
    """Convert raw S3 key to parsed key: raw/ticker/... -> parsed/ticker/...json.gz"""
    parts = raw_key.split("/")
    if parts:
        parts[0] = "parsed"
    filename = parts[-1]
    base = filename.rsplit(".", 1)[0] if "." in filename else filename
    parts[-1] = f"{base}.json.gz"
    return "/".join(parts)


def normalize(text: str) -> str:
    """Normalize whitespace and remove null bytes"""
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def row_get(row: Dict[str, Any], *keys: str) -> Any:
    """Get first non-None value from dict keys (case-insensitive)"""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def looks_like_pdf(data: bytes) -> bool:
    """Check if file starts with PDF magic bytes"""
    return data[:4] == b"%PDF"


def looks_like_html(data: bytes) -> bool:
    """Check if file contains HTML-like content"""
    head = data[:8000].lower()
    return (
        b"<html" in head
        or b"<!doctype html" in head
        or b"</table" in head
        or b"<xbrl" in head
        or b"<ix:" in head
        or b"<div" in head
        or b"<p" in head
    )


def compute_hash(text: str) -> str:
    """Compute SHA256 hash for deduplication"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def count_keywords(text: str, keywords: List[str]) -> Dict[str, int]:
    """Count keyword frequencies (case-insensitive)"""
    text_lower = text.lower()
    counts = {}
    for keyword in keywords:
        pattern = re.compile(r'\b' + re.escape(keyword.lower()) + r'\b')
        count = len(pattern.findall(text_lower))
        if count > 0:
            counts[keyword] = count
    return counts



# SECTION EXTRACTORS BY FILING TYPE


class TenKExtractor:
    """Extract AI-relevant sections from 10-K (Annual Report)"""
    
    SECTION_PATTERNS = {
        "item_1_business": re.compile(
            r"(?:^|\n)\s*ITEM\s+1\.?\s*[:\-—]?\s*BUSINESS",
            re.IGNORECASE | re.MULTILINE
        ),
        "item_1a_risk": re.compile(
            r"(?:^|\n)\s*ITEM\s+1A\.?\s*[:\-—]?\s*RISK\s+FACTORS",
            re.IGNORECASE | re.MULTILINE
        ),
        "item_7_mda": re.compile(
            r"(?:^|\n)\s*ITEM\s+7\.?\s*[:\-—]?\s*MANAGEMENT'?S?\s+DISCUSSION",
            re.IGNORECASE | re.MULTILINE
        ),
        "item_8_financial": re.compile(
            r"(?:^|\n)\s*ITEM\s+8\.?\s*[:\-—]?\s*FINANCIAL\s+STATEMENTS",
            re.IGNORECASE | re.MULTILINE
        ),
    }
    
    def extract(self, text: str) -> Dict[str, Any]:
        sections = {}
        positions = []
        for section_name, pattern in self.SECTION_PATTERNS.items():
            match = pattern.search(text)
            if match:
                positions.append((match.start(), section_name))
        
        positions.sort()
        
        for i, (start_pos, section_name) in enumerate(positions):
            if i + 1 < len(positions):
                end_pos = positions[i + 1][0]
            else:
                end_pos = min(start_pos + 50000, len(text))
            
            section_text = text[start_pos:end_pos].strip()
            sections[section_name] = section_text
        
        ai_keyword_counts = count_keywords(text, AI_KEYWORDS)
        tech_keyword_counts = count_keywords(text, TECH_KEYWORDS)
        rd_mentions = re.findall(r'r[&\s]?d\s+(?:spending|expense|investment)[^\n]{0,100}', 
                                 text, re.IGNORECASE)
        
        return {
            "filing_type": "10-K",
            "sections": sections,
            "ai_keywords": ai_keyword_counts,
            "tech_keywords": tech_keyword_counts,
            "total_ai_mentions": sum(ai_keyword_counts.values()),
            "total_tech_mentions": sum(tech_keyword_counts.values()),
            "rd_mentions": rd_mentions[:5],
            "has_ai_risks": "item_1a_risk" in sections and any(
                kw in sections["item_1a_risk"].lower() 
                for kw in ["artificial intelligence", "machine learning", "ai"]
            ),
        }


class TenQExtractor:
    """Extract AI-relevant content from 10-Q (Quarterly Report)"""
    
    def extract(self, text: str) -> Dict[str, Any]:
        ai_keyword_counts = count_keywords(text, AI_KEYWORDS)
        tech_keyword_counts = count_keywords(text, TECH_KEYWORDS)
        
        initiative_patterns = [
            r'initiative[s]?[^\n]{0,150}',
            r'announced[^\n]{0,150}',
            r'progress[^\n]{0,150}',
            r'deploy[^\n]{0,150}',
        ]
        
        initiatives = []
        for pattern in initiative_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            initiatives.extend(matches[:3])
        
        rd_mentions = re.findall(
            r'r[&\s]?d\s+(?:spending|expense|investment)[^\n]{0,100}',
            text, re.IGNORECASE
        )
        
        headcount_mentions = re.findall(
            r'(?:employee|headcount|hire|hiring)[^\n]{0,100}',
            text, re.IGNORECASE
        )
        
        return {
            "filing_type": "10-Q",
            "ai_keywords": ai_keyword_counts,
            "tech_keywords": tech_keyword_counts,
            "total_ai_mentions": sum(ai_keyword_counts.values()),
            "total_tech_mentions": sum(tech_keyword_counts.values()),
            "initiative_mentions": initiatives[:10],
            "rd_mentions": rd_mentions[:3],
            "headcount_mentions": headcount_mentions[:5],
        }


class EightKExtractor:
    """Extract material events from 8-K (Current Report)"""
    
    LEADERSHIP_PATTERNS = [
        r'chief\s+(?:technology|data|digital|information|ai|analytics)\s+officer',
        r'cto|cdo|cio|cdao',
        r'vice\s+president[^\n]{0,100}(?:technology|data|ai|digital)',
        r'head\s+of\s+(?:ai|data|technology|digital)',
    ]
    
    ACQUISITION_PATTERNS = [
        r'acquire[d]?[^\n]{0,150}',
        r'merger[^\n]{0,150}',
        r'acquisition[^\n]{0,150}',
        r'purchase[d]?\s+(?:all|substantially)[^\n]{0,100}',
    ]
    
    PARTNERSHIP_PATTERNS = [
        r'partnership[^\n]{0,150}',
        r'alliance[^\n]{0,150}',
        r'collaboration[^\n]{0,150}',
        r'joint\s+venture[^\n]{0,150}',
    ]
    
    def extract(self, text: str) -> Dict[str, Any]:
        leadership_signals = []
        for pattern in self.LEADERSHIP_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            leadership_signals.extend(matches)
        
        acquisition_signals = []
        for pattern in self.ACQUISITION_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            acquisition_signals.extend(matches)
        
        partnership_signals = []
        for pattern in self.PARTNERSHIP_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            partnership_signals.extend(matches)
        
        text_lower = text.lower()
        is_ai_related = any(kw in text_lower for kw in [
            "artificial intelligence", "machine learning", "ai", 
            "data science", "analytics"
        ])
        
        return {
            "filing_type": "8-K",
            "is_ai_related": is_ai_related,
            "leadership_changes": leadership_signals[:5],
            "acquisitions": acquisition_signals[:5],
            "partnerships": partnership_signals[:5],
            "has_leadership_signal": len(leadership_signals) > 0,
            "has_acquisition_signal": len(acquisition_signals) > 0,
            "has_partnership_signal": len(partnership_signals) > 0,
        }


class DEF14AExtractor:
    """Extract governance signals from DEF-14A (Proxy Statement)"""
    
    def extract(self, text: str) -> Dict[str, Any]:
        board_expertise_patterns = [
            r'technology',
            r'artificial\s+intelligence',
            r'machine\s+learning',
            r'data\s+science',
            r'digital',
            r'software',
            r'cybersecurity',
        ]
        
        board_expertise_score = 0
        board_mentions = []
        for pattern in board_expertise_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                board_expertise_score += len(matches)
                board_mentions.append(f"{pattern}: {len(matches)}")
        
        has_tech_committee = bool(re.search(
            r'technology\s+committee|digital\s+committee|innovation\s+committee',
            text, re.IGNORECASE
        ))
        
        comp_patterns = [
            r'compensation[^\n]{0,150}(?:digital|technology|ai|innovation)',
            r'performance\s+metric[s]?[^\n]{0,150}(?:digital|technology|ai)',
            r'bonus[^\n]{0,100}(?:digital|technology|ai)',
        ]
        
        comp_mentions = []
        for pattern in comp_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            comp_mentions.extend(matches[:3])
        
        has_ai_expert = bool(re.search(
            r'artificial\s+intelligence|machine\s+learning|ai\s+expert',
            text, re.IGNORECASE
        ))
        
        return {
            "filing_type": "DEF-14A",
            "board_tech_expertise_score": board_expertise_score,
            "board_expertise_mentions": board_mentions,
            "has_tech_committee": has_tech_committee,
            "has_ai_expert": has_ai_expert,
            "exec_comp_tied_to_digital": len(comp_mentions) > 0,
            "comp_mentions": comp_mentions,
            "governance_signal_strength": "strong" if (
                has_tech_committee and has_ai_expert and board_expertise_score > 10
            ) else "moderate" if (
                has_tech_committee or has_ai_expert
            ) else "weak",
        }



# DOCUMENT PARSER


class DocumentParser:
    """SEC-safe parser with filing-type-specific extractors"""
    
    XBRL_LIKE_ATTR_RE = re.compile(r"xbrl|ix:", re.I)
    
    def __init__(self):
        self.extractors = {
            "10-K": TenKExtractor(),
            "10-Q": TenQExtractor(),
            "8-K": EightKExtractor(),
            "DEF 14A": DEF14AExtractor(),
        }
    
    def _strip_xbrl(self, soup: BeautifulSoup) -> int:
        removed = 0
        for tag in soup.find_all(
            ["xbrl", "ix:header", "ix:nonnumeric", "ix:nonfraction", 
             "ix:continuation", "ix:footnote"]
        ):
            tag.decompose()
            removed += 1
        
        for tag in soup.find_all(attrs={"contextref": True}):
            tag.decompose()
            removed += 1
        
        for tag in soup.find_all(attrs={"name": self.XBRL_LIKE_ATTR_RE}):
            tag.decompose()
            removed += 1
        
        for tag in soup.find_all(
            attrs={"class": re.compile(r"xbrl|ixbrl|inline-xbrl|inlinexbrl", re.I)}
        ):
            tag.decompose()
            removed += 1
        
        for tag in soup.find_all(
            attrs={"id": re.compile(r"xbrl|ixbrl|inline-xbrl|inlinexbrl", re.I)}
        ):
            tag.decompose()
            removed += 1
        
        return removed
    
    def _make_soup_resilient(self, html: str) -> Tuple[BeautifulSoup, str]:
        try:
            soup = BeautifulSoup(html, "lxml")
            return soup, "lxml"
        except Exception:
            soup = BeautifulSoup(html, "html.parser")
            return soup, "html.parser"
    
    def parse_html(self, data: bytes) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        if len(data) > MAX_HTML_BYTES:
            data = data[:MAX_HTML_BYTES]
        
        html = data.decode("utf-8", errors="ignore").replace("\x00", " ")
        soup, builder = self._make_soup_resilient(html)
        
        for tag in soup(["script", "style", "noscript", "meta", "link"]):
            tag.decompose()
        
        xbrl_removed = self._strip_xbrl(soup)
        text = normalize(soup.get_text("\n"))
        
        tables: List[Dict[str, Any]] = []
        all_tables = soup.find_all("table")
        total_tables_found = len(all_tables)
        tables_to_scan = all_tables[:MAX_TABLES_SCAN]
        
        for t_index, table in enumerate(tables_to_scan):
            if (t_index + 1) % PRINT_EVERY_N_TABLES == 0:
                print(f"   ...Scanned {t_index + 1}/{min(total_tables_found, MAX_TABLES_SCAN)} tables")
            
            if len(tables) >= MAX_TABLES_EMIT:
                break
            
            rows: List[List[str]] = []
            for tr in table.find_all("tr")[:MAX_ROWS_PER_TABLE]:
                tds = tr.find_all("td")[:MAX_CELLS_PER_ROW]
                if not tds:
                    continue
                cells = [normalize(td.get_text(" ", strip=True)) for td in tds]
                if any(cells):
                    rows.append(cells)
            
            if rows:
                tables.append({"table_index": t_index, "rows": rows})
        
        meta = {
            "builder": builder,
            "html_bytes_used": len(data),
            "xbrl_nodes_removed": xbrl_removed,
            "total_tables_found": total_tables_found,
            "tables_emitted": len(tables),
        }
        
        return text, tables, meta
    
    def parse_pdf(self, data: bytes) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        text_parts: List[str] = []
        tables: List[Dict[str, Any]] = []
        
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as f:
            f.write(data)
            f.flush()
            
            try:
                with pdfplumber.open(f.name) as pdf:
                    for i, page in enumerate(pdf.pages):
                        page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
                        if page_text:
                            text_parts.append(page_text)
                        
                        for table in (page.extract_tables() or [])[:50]:
                            if table:
                                tables.append({"page": i + 1, "rows": table})
            except Exception as e:
                print(f"   ⚠️  pdfplumber failed: {e}, trying PyMuPDF...")
            
            used_fallback = False
            try:
                doc = fitz.open(f.name)
                pymu_text = [p.get_text("text") for p in doc]
                doc.close()
                
                if not text_parts or len("".join(pymu_text)) > len("".join(text_parts)):
                    text_parts = pymu_text
                    used_fallback = True
            except Exception as e:
                print(f"   ⚠️  PyMuPDF also failed: {e}")
        
        meta = {
            "pdf_tables_count": len(tables),
            "pymupdf_fallback_used": used_fallback,
        }
        
        return normalize("\n".join(text_parts)), tables, meta
    
    def extract_filing_specific_content(
        self, 
        text: str, 
        filing_type: str
    ) -> Dict[str, Any]:
        extractor = self.extractors.get(filing_type)
        if extractor:
            return extractor.extract(text)
        else:
            return {
                "filing_type": filing_type,
                "ai_keywords": count_keywords(text, AI_KEYWORDS),
                "total_ai_mentions": sum(count_keywords(text, AI_KEYWORDS).values()),
            }



# S3 PIPELINE


class DocumentParserS3Pipeline:
    """Main pipeline for parsing downloaded documents with AI-readiness focus"""
    
    def __init__(self) -> None:
        self.sf = SnowflakeService()
        self.s3 = S3Storage()
        self.parser = DocumentParser()
    
    def run(self, limit: int = 200, force_reparse: bool = False) -> None:
        status_filter = "'downloaded', 'error'" if force_reparse else "'downloaded'"
        query = f"""
            SELECT id, ticker, filing_type, filing_date, s3_key
            FROM documents_sec
            WHERE status IN ({status_filter})
            ORDER BY filing_date DESC, ticker
            LIMIT %(limit)s
        """
        
        rows = self.sf.execute_query(query, {"limit": limit})
        
        if not rows:
            print(f" No documents with status={status_filter} to parse.")
            return
        
        print(f"\n{'='*70}")
        print(f"SEC DOCUMENT PARSER - AI-READINESS FOCUSED")
        print(f"{'='*70}")
        print(f"Documents to process: {len(rows)}")
        print(f"Force reparse: {force_reparse}")
        print()
        
        parsed_count = 0
        skipped_existing = 0
        failed = 0
        stats_by_type = {}
        
        for idx, r in enumerate(rows, 1):
            doc_id = row_get(r, "id", "ID")
            raw_key = row_get(r, "s3_key", "S3_KEY")
            ticker = (row_get(r, "ticker", "TICKER") or "").upper()
            filing_type = row_get(r, "filing_type", "FILING_TYPE") or ""
            filing_date = row_get(r, "filing_date", "FILING_DATE")
            
            if not doc_id or not raw_key:
                print(f" Malformed row (missing id/s3_key): {r}")
                failed += 1
                continue
            
            out_key = parsed_s3_key(str(raw_key))
            
            # Idempotent check
            if not force_reparse and self.s3.exists(out_key):
                # Update database to point to parsed file
                self.sf.execute_update(
                    """
                    UPDATE documents_sec 
                    SET status='parsed', 
                        s3_key=%(parsed_key)s,
                        error_message=NULL 
                    WHERE id=%(id)s
                    """,
                    {"id": doc_id, "parsed_key": out_key},
                )
                skipped_existing += 1
                print(f"[{idx}/{len(rows)}] ↪SKIP: {ticker} {filing_type}")
                continue
            
            print(f"\n[{idx}/{len(rows)}] {ticker} {filing_type} ({filing_date})")
            
            t0 = time.time()
            
            try:
                # Download from S3
                data = self.s3.get_bytes(str(raw_key))
                print(f"   Downloaded: {len(data):,} bytes")
                
                # Determine parser type
                if str(raw_key).lower().endswith(".pdf") or looks_like_pdf(data):
                    text, tables, meta = self.parser.parse_pdf(data)
                    parser_type = "pdf"
                else:
                    text, tables, meta = self.parser.parse_html(data)
                    parser_type = "html"
                
                if not text:
                    raise ValueError("No text extracted from document")
                
                print(f"   Text: {len(text):,} chars | Tables: {len(tables)}")
                
                # Extract filing-specific AI-relevant content
                ai_content = self.parser.extract_filing_specific_content(text, filing_type)
                
                # Show key extractions
                if filing_type == "10-K":
                    print(f"   AI mentions: {ai_content.get('total_ai_mentions', 0)}")
                    print(f"   Sections: {list(ai_content.get('sections', {}).keys())}")
                elif filing_type == "10-Q":
                    print(f"   AI mentions: {ai_content.get('total_ai_mentions', 0)}")
                elif filing_type == "8-K":
                    signals = []
                    if ai_content.get('has_leadership_signal'):
                        signals.append('Leadership')
                    if ai_content.get('has_acquisition_signal'):
                        signals.append('M&A')
                    if ai_content.get('has_partnership_signal'):
                        signals.append('Partnership')
                    print(f"   Signals: {', '.join(signals) if signals else 'None'}")
                elif filing_type == "DEF 14A":
                    print(f"   Governance: {ai_content.get('governance_signal_strength', 'unknown')}")
                    print(f"   Board tech score: {ai_content.get('board_tech_expertise_score', 0)}")
                
                # Prepare payload
                payload = {
                    "document_id": doc_id,
                    "ticker": ticker,
                    "filing_type": filing_type,
                    "filing_date": str(filing_date) if filing_date else None,
                    "raw_s3_key": raw_key,
                    "parsed_s3_key": out_key,
                    "parsed_at": datetime.now(timezone.utc).isoformat(),
                    "parser_type": parser_type,
                    "meta": meta,
                    "text_length": len(text),
                    "text": text,
                    "tables": tables,
                    "ai_content": ai_content,
                }
                
                # Upload to S3
                self.s3.put_json_gz(out_key, payload)
                print(f"   Uploaded: {out_key}")
                
                
                self.sf.execute_update(
                    """
                    UPDATE documents_sec 
                    SET status='parsed',
                        s3_key=%(parsed_key)s,
                        error_message=NULL,
                        processed_at=CURRENT_TIMESTAMP()
                    WHERE id=%(id)s
                    """,
                    {"id": doc_id, "parsed_key": out_key},
                )
                
                elapsed = time.time() - t0
                parsed_count += 1
                
                if filing_type not in stats_by_type:
                    stats_by_type[filing_type] = 0
                stats_by_type[filing_type] += 1
                
                print(f"    Success in {elapsed:.1f}s")
                
            except Exception as e:
                failed += 1
                error_msg = f"parse_failed: {type(e).__name__}: {str(e)[:200]}"
                
                self.sf.execute_update(
                    "UPDATE documents_sec SET status='error', error_message=%(err)s WHERE id=%(id)s",
                    {"id": doc_id, "err": error_msg},
                )
                
                print(f"    FAILED: {e}")
        
        # Final summary
        print(f"\n{'='*70}")
        print(f" PARSING SUMMARY")
        print(f"{'='*70}")
        print(f"Total scanned:     {len(rows)}")
        print(f"Parsed:            {parsed_count}")
        print(f"Skipped (exists):  {skipped_existing}")
        print(f"Failed:            {failed}")
        print(f"\nBy Filing Type:")
        for ftype, count in sorted(stats_by_type.items()):
            print(f"  {ftype:12s}: {count}")
        print(f"{'='*70}\n")


# ========================================
# MAIN ENTRY POINT
# ========================================

def main(limit: int = 200, force_reparse: bool = False) -> None:
    pipeline = DocumentParserS3Pipeline()
    pipeline.run(limit=limit, force_reparse=force_reparse)


if __name__ == "__main__":
    import sys
    
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    force = "--force" in sys.argv
    
    main(limit=limit, force_reparse=force)