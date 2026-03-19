from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from sec_edgar_downloader import Downloader

from app.services.snowflake import SnowflakeService

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DOCUMENTS_TABLE = "documents_sec"
CHUNKS_TABLE = "document_chunks_sec"

AFTER_DATE = os.getenv("SEC_AFTER_DATE", "2021-01-01")
SEC_SLEEP = float(os.getenv("SEC_SLEEP_SECONDS", "0.75"))

FILING_COUNTS: Dict[str, int] = {
    "10-K": 2,
    "8-K": 2,
    "10-Q": 4,
}

# Fallback section name used when no heading is detected for a filing type.
# Guarantees the section column is never null/unknown for any ticker.
FILING_TYPE_DEFAULT_SECTION: Dict[str, str] = {
    "10-K": "Item 1 (Business)",
    "10-Q": "Item 1",
    "8-K": "Item 8.01",
    "DEF 14A": "Proxy Statement",
}

CANONICAL = {
    "10-K": {
        "business":     "Item 1 (Business)",
        "risk_factors": "Item 1A (Risk)",
        "mda":          "Item 7 (MD&A)",
    },
    "8-K": {
        "8.01":  "Item 8.01",
        "5.02":  "Item 5.02",
        "2.01":  "Item 2.01",
        "1.01":  "Item 1.01",
    },
    "10-Q": {
        "part1item1": "Item 1",
        "part1item2": "Item 7 (MD&A)",
        "part2item1": "Item 1A (Risk)",
    },
}

MISTRAL_HEADER_MAP: List[Tuple[str, str]] = [
    ("ITEM 1A",         "Item 1A (Risk)"),
    ("RISK FACTORS",    "Item 1A (Risk)"),
    ("ITEM 7",          "Item 7 (MD&A)"),
    ("MD&A",            "Item 7 (MD&A)"),
    ("MANAGEMENT",      "Item 7 (MD&A)"),
    ("ITEM 1",          "Item 1 (Business)"),
    ("BUSINESS",        "Item 1 (Business)"),
]

MIN_WORDS = 500
MAX_WORDS = 1000
OVERLAP_WORDS = 75
MIN_BLOCK_WORDS = 10
MAX_NUMERIC_RATIO = 0.65


class SECPipeline:
    def __init__(self) -> None:
        self._email = self._require_env("SEC_EDGAR_USER_AGENT_EMAIL")
        self._bucket = self._require_env("S3_BUCKET_NAME")
        self._region = os.getenv("AWS_REGION", "us-east-1")
        self._mistral_key = os.getenv("MISTRAL_API_KEY")

        try:
            from edgar import set_identity  # type: ignore
            set_identity(f"OrgAIR {self._email}")
        except Exception as exc:
            logger.warning("Could not set edgartools identity: %s", exc)

        self._db = SnowflakeService()
        self._s3 = boto3.client(
            "s3",
            region_name=self._region,
            config=Config(
                retries={"max_attempts": 10, "mode": "adaptive"},
                connect_timeout=60,
                read_timeout=300,
                tcp_keepalive=True,
            ),
        )

    def run(self, ticker: str) -> Dict[str, Any]:
        ticker = ticker.upper().strip()
        logger.info("=" * 60)
        logger.info("SEC PIPELINE: %s", ticker)
        logger.info("=" * 60)

        company_id = self._resolve_company_id(ticker)
        docs = self._download(ticker, company_id)
        total_chunks = 0
        errors: List[str] = []

        for doc in docs:
            try:
                sections = self._parse_doc(doc)
                if not sections:
                    logger.warning("No sections extracted for %s %s", ticker, doc["filing_type"])
                    continue

                parsed_s3_key = self._save_parsed_to_s3(
                    ticker, doc["filing_type"], doc["filing_date"], sections, doc["doc_id"]
                )
                self._update_doc_status(doc["doc_id"], "parsed", {"s3_key": parsed_s3_key})

                all_chunks: List[Dict[str, Any]] = []
                for section_name, text in sections.items():
                    if not text or len(text.split()) < MIN_BLOCK_WORDS:
                        continue
                    chunks = self._chunk_section(text, section_name, doc["doc_id"], doc["filing_type"])
                    all_chunks.extend(chunks)

                if all_chunks:
                    self._save_chunks(doc["doc_id"], all_chunks)
                    total_chunks += len(all_chunks)
                    total_word_count = sum(c["word_count"] for c in all_chunks)
                    self._update_doc_status(
                        doc["doc_id"], "chunked",
                        {
                            "chunk_count": len(all_chunks),
                            "word_count": total_word_count,
                            "processed_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    logger.info(
                        "  %s %s → %d chunks across %d sections",
                        ticker, doc["filing_type"], len(all_chunks), len(sections)
                    )

            except Exception as exc:
                msg = f"{ticker} {doc.get('filing_type')} {doc.get('doc_id')}: {exc}"
                logger.error("Error processing doc: %s", msg)
                errors.append(msg)
                self._update_doc_status(doc["doc_id"], "failed", {"error_message": str(exc)[:500]})

        result = {
            "ticker": ticker,
            "docs_processed": len(docs),
            "chunks_created": total_chunks,
            "errors": errors,
        }
        logger.info("Done: %s", result)
        return result

    def _download(self, ticker: str, company_id: Optional[str]) -> List[Dict[str, Any]]:
        download_root = Path("data/raw")
        download_root.mkdir(parents=True, exist_ok=True)

        dl = Downloader("OrgAIR", self._email, str(download_root))
        docs: List[Dict[str, Any]] = []
        run_date = date.today().isoformat()

        for filing_type, limit in FILING_COUNTS.items():
            logger.info("Downloading %s x%d for %s", filing_type, limit, ticker)
            try:
                dl.get(filing_type, ticker, limit=limit, after=AFTER_DATE)
            except Exception as exc:
                logger.error("SEC download failed for %s %s: %s", ticker, filing_type, exc)
                time.sleep(SEC_SLEEP)
                continue
            time.sleep(SEC_SLEEP)

            folders = self._get_download_folders(ticker, filing_type, limit)
            for folder in folders:
                main_file = self._pick_main_file(folder)
                if not main_file:
                    continue

                content_hash = self._sha256(main_file)
                existing = self._db.execute_query(
                    f"SELECT id, status, local_path, s3_key FROM {DOCUMENTS_TABLE} WHERE content_hash = %(h)s AND UPPER(ticker) = %(t)s LIMIT 1",
                    {"h": content_hash, "t": ticker},
                )
                if existing:
                    row = existing[0]
                    status = str(row.get("STATUS") or row.get("status", ""))
                    if status == "chunked":
                        logger.info("  Skipping (already chunked): %s", main_file.name)
                        continue
                    existing_id = str(row.get("ID") or row.get("id"))
                    existing_path = str(row.get("LOCAL_PATH") or row.get("local_path") or str(main_file))
                    logger.info("  Re-queuing for parse (status=%s): %s doc_id=%s", status, main_file.name, existing_id)
                    docs.append({
                        "doc_id": existing_id,
                        "ticker": ticker,
                        "filing_type": filing_type,
                        "filing_date": run_date,
                        "local_path": existing_path,
                        "s3_key": str(row.get("S3_KEY") or row.get("s3_key", "")),
                        "is_pdf": main_file.suffix.lower() == ".pdf",
                    })
                    continue

                filing_date = self._extract_date(folder)
                ft_norm = filing_type.upper().replace(" ", "").replace("-", "")
                s3_key = f"sec/{ticker}/{ft_norm}/{run_date}/{uuid4()}{main_file.suffix}"

                uploaded = self._s3_upload(main_file, s3_key)
                if not uploaded:
                    continue

                source_url = self._build_source_url(folder, main_file)
                doc_id = str(uuid4())

                self._db.execute_update(
                    f"""
                    INSERT INTO {DOCUMENTS_TABLE}
                        (id, company_id, ticker, filing_type, filing_date,
                         source_url, local_path, s3_key, content_hash, status, created_at)
                    VALUES
                        (%(id)s, %(cid)s, %(ticker)s, %(ft)s, %(fdate)s,
                         %(url)s, %(lpath)s, %(s3key)s, %(hash)s, 'downloaded', %(now)s)
                    """,
                    {
                        "id": doc_id,
                        "cid": company_id,
                        "ticker": ticker,
                        "ft": filing_type,
                        "fdate": str(filing_date) if filing_date else None,
                        "url": source_url,
                        "lpath": str(main_file),
                        "s3key": s3_key,
                        "hash": content_hash,
                        "now": datetime.now(timezone.utc).isoformat(),
                    },
                )

                docs.append({
                    "doc_id": doc_id,
                    "ticker": ticker,
                    "filing_type": filing_type,
                    "filing_date": str(filing_date) if filing_date else run_date,
                    "local_path": str(main_file),
                    "s3_key": s3_key,
                    "is_pdf": main_file.suffix.lower() == ".pdf",
                })
                logger.info("  Stored %s %s doc_id=%s", filing_type, main_file.name, doc_id)

        return docs

    def _parse_doc(self, doc: Dict[str, Any]) -> Dict[str, str]:
        filing_type = doc["filing_type"]
        ticker = doc["ticker"]
        local_path = doc["local_path"]
        is_pdf = doc.get("is_pdf", False)

        if is_pdf:
            logger.info("  PDF detected — using Mistral OCR for %s", local_path)
            sections = self._parse_with_mistral_ocr(local_path, filing_type)
            return self._ensure_sections(sections, filing_type, local_path)

        try:
            sections = self._parse_with_edgartools(ticker, filing_type, local_path)
            if sections:
                if filing_type == "10-K":
                    expected = ["Item 1 (Business)", "Item 1A (Risk)", "Item 7 (MD&A)"]
                    missing = [s for s in expected if s not in sections]
                    if missing:
                        logger.info("  edgartools missing %s — supplementing from local HTML", missing)
                        bs_sections = self._parse_with_beautifulsoup(local_path, filing_type)
                        for s in missing:
                            if s in bs_sections:
                                sections[s] = bs_sections[s]
                                logger.info("  Supplemented %s from BeautifulSoup", s)
                return self._ensure_sections(sections, filing_type, local_path)
        except Exception as exc:
            logger.warning("edgartools failed for %s %s: %s — trying HTML parser", ticker, filing_type, exc)

        sections = self._parse_with_beautifulsoup(local_path, filing_type)
        return self._ensure_sections(sections, filing_type, local_path)

    def _ensure_sections(
        self, sections: Dict[str, str], filing_type: str, local_path: str
    ) -> Dict[str, str]:
        """
        If parsing produced no sections at all, fall back to storing the full
        document text under the filing type's default section name.
        This guarantees the section column is never null or 'unknown'.
        """
        if sections:
            return sections

        logger.warning(
            "No sections detected for %s %s — storing full text under default section",
            filing_type, local_path
        )
        default_section = FILING_TYPE_DEFAULT_SECTION.get(filing_type, filing_type)
        try:
            with open(local_path, "rb") as f:
                raw = f.read()
            primary = self._extract_primary_document(raw)
            from bs4 import BeautifulSoup  # type: ignore
            soup = BeautifulSoup(primary, "lxml")
            for tag in soup(["script", "style", "head"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            if len(text.split()) >= MIN_BLOCK_WORDS:
                return {default_section: text}
        except Exception as exc:
            logger.error("Fallback full-text extraction failed for %s: %s", local_path, exc)

        return {}

    def _parse_with_edgartools(
        self, ticker: str, filing_type: str, local_path: str
    ) -> Dict[str, str]:
        from edgar import Company  # type: ignore

        company = Company(ticker)
        sections: Dict[str, str] = {}

        if filing_type == "10-K":
            filings = company.get_filings(form="10-K")
            if not filings:
                return {}
            tenk = filings.latest(1).obj()
            raw = {
                "business":     getattr(tenk, "business", None),
                "risk_factors": getattr(tenk, "risk_factors", None),
                "mda":          getattr(tenk, "mda", None),
            }
            for attr_key, canonical_name in CANONICAL["10-K"].items():
                text = raw.get(attr_key)
                if text:
                    text_str = str(text).strip()
                    if len(text_str.split()) >= MIN_BLOCK_WORDS:
                        sections[canonical_name] = text_str

        elif filing_type == "10-Q":
            filings = company.get_filings(form="10-Q")
            if not filings:
                return {}
            tenq = filings.latest(1).obj()
            for attr_key, canonical_name in CANONICAL["10-Q"].items():
                text = getattr(tenq, attr_key, None)
                if text:
                    text_str = str(text).strip()
                    if len(text_str.split()) >= MIN_BLOCK_WORDS:
                        sections[canonical_name] = text_str

        elif filing_type == "8-K":
            filings = company.get_filings(form="8-K")
            if not filings:
                return {}
            eightk = filings.latest(1).obj()
            items_raw = getattr(eightk, "items", None) or []

            if isinstance(items_raw, dict):
                items_dict = items_raw
            elif isinstance(items_raw, list):
                items_dict = {}
                for item in items_raw:
                    item_num = getattr(item, "item_number", None) or getattr(item, "number", None)
                    text = getattr(item, "text", None) or getattr(item, "content", None)
                    if item_num and text:
                        items_dict[str(item_num)] = text
                    elif isinstance(item, str):
                        items_dict[str(len(items_dict))] = item
            else:
                logger.warning("Unexpected 8-K items type: %s", type(items_raw))
                items_dict = {}

            logger.info("8-K items resolved: %s", list(items_dict.keys()))

            for item_num, canonical_name in CANONICAL["8-K"].items():
                text = items_dict.get(item_num)
                if text:
                    text_str = str(text).strip()
                    if len(text_str.split()) >= MIN_BLOCK_WORDS:
                        sections[canonical_name] = text_str

        # NOTE: return is outside the for loop for all branches
        return sections

    def _parse_with_mistral_ocr(self, local_path: str, filing_type: str) -> Dict[str, str]:
        if not self._mistral_key:
            logger.warning("MISTRAL_API_KEY not set — Mistral OCR fallback unavailable")
            return {}

        try:
            from mistralai import Mistral  # type: ignore

            client = Mistral(api_key=self._mistral_key)

            with open(local_path, "rb") as f:
                file_data = f.read()

            uploaded = client.files.upload(
                file={"file_name": Path(local_path).name, "content": file_data},
                purpose="ocr",
            )
            signed = client.files.get_signed_url(file_id=uploaded.id, expiry=1)
            ocr_result = client.ocr.process(
                model="mistral-ocr-latest",
                document={"type": "url", "url": signed.url},
            )
            full_markdown = "\n\n".join(
                page.markdown for page in ocr_result.pages if page.markdown
            )
            return self._extract_sections_from_markdown(full_markdown, filing_type)

        except Exception as exc:
            logger.error("Mistral OCR failed for %s: %s", local_path, exc)
            return {}

    def _extract_primary_document(self, raw: bytes) -> bytes:
        try:
            text = raw.decode("utf-8", errors="replace")
        except Exception:
            return raw

        start_marker = "<TEXT>"
        end_marker = "</TEXT>"
        start = text.find(start_marker)
        if start == -1:
            return raw

        end = text.find(end_marker, start)
        if end == -1:
            end = len(text)

        primary = text[start + len(start_marker):end]
        return primary.encode("utf-8")

    def _parse_with_beautifulsoup(self, local_path: str, filing_type: str) -> Dict[str, str]:
        from bs4 import BeautifulSoup  # type: ignore

        try:
            with open(local_path, "rb") as f:
                raw = f.read()

            primary_html = self._extract_primary_document(raw)
            soup = BeautifulSoup(primary_html, "lxml")

            for tag in soup(["script", "style", "head"]):
                tag.decompose()

            elements = soup.find_all(["p", "div", "span", "td", "tr", "h1", "h2", "h3", "h4"])

            sections: Dict[str, str] = {}
            current_section: Optional[str] = None
            current_lines: List[str] = []

            def flush() -> None:
                if not current_section or not current_lines:
                    return
                text = "\n\n".join(current_lines).strip()
                if len(text.split()) >= MIN_BLOCK_WORDS:
                    if current_section not in sections:
                        sections[current_section] = text
                    else:
                        sections[current_section] += "\n\n" + text

            for el in elements:
                raw_text = el.get_text(separator=" ", strip=True)
                if not raw_text or len(raw_text) < 5:
                    continue

                upper = raw_text.upper()[:120]

                if len(raw_text) < 200:
                    detected = self._detect_section_heading(upper, filing_type)
                    if detected:
                        flush()
                        current_section = detected
                        current_lines = []
                        continue

                if current_section and len(raw_text.split()) >= 5:
                    current_lines.append(raw_text)

            flush()
            logger.info("  BeautifulSoup extracted %d sections from %s", len(sections), Path(local_path).name)
            return sections

        except Exception as exc:
            logger.error("BeautifulSoup parse failed for %s: %s", local_path, exc)
            return {}

    def _detect_section_heading(self, upper_text: str, filing_type: str) -> Optional[str]:
        if filing_type == "10-K":
            if "ITEM 1A" in upper_text or "RISK FACTORS" in upper_text:
                return "Item 1A (Risk)"
            if "ITEM 7A" in upper_text:
                return None
            if "ITEM 7" in upper_text and ("MANAGEMENT" in upper_text or "MD&A" in upper_text or upper_text.strip().startswith("ITEM 7")):
                return "Item 7 (MD&A)"
            if "ITEM 1" in upper_text and "BUSINESS" in upper_text:
                return "Item 1 (Business)"
            if upper_text.strip() in ("ITEM 1.", "ITEM 1"):
                return "Item 1 (Business)"
        elif filing_type == "10-Q":
            if "ITEM 1A" in upper_text or "RISK FACTORS" in upper_text:
                return "Item 1A (Risk)"
            if "ITEM 2" in upper_text and ("MANAGEMENT" in upper_text or "MD&A" in upper_text):
                return "Item 7 (MD&A)"
            if "ITEM 1" in upper_text and "FINANCIAL" in upper_text:
                return "Item 1"
        elif filing_type == "8-K":
            for item_num, canonical in CANONICAL["8-K"].items():
                if f"ITEM {item_num.upper()}" in upper_text:
                    return canonical
        return None

    def _extract_sections_from_markdown(self, markdown: str, filing_type: str) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        if not markdown:
            return sections

        lines = markdown.split("\n")
        current_header: Optional[str] = None
        current_lines: List[str] = []

        def flush(header: Optional[str], body_lines: List[str]) -> None:
            if not header:
                return
            canonical = self._map_header_to_canonical(header, filing_type)
            if canonical:
                text = "\n".join(body_lines).strip()
                if len(text.split()) >= MIN_BLOCK_WORDS:
                    if canonical not in sections:
                        sections[canonical] = text
                    else:
                        sections[canonical] += "\n\n" + text

        for line in lines:
            if line.startswith("#"):
                flush(current_header, current_lines)
                current_header = line.lstrip("#").strip().upper()
                current_lines = []
            else:
                current_lines.append(line)

        flush(current_header, current_lines)
        return sections

    def _map_header_to_canonical(self, header_upper: str, filing_type: str) -> Optional[str]:
        if filing_type == "10-K":
            for pattern, canonical in MISTRAL_HEADER_MAP:
                if pattern in header_upper:
                    return canonical
            return None

        if filing_type == "8-K":
            for item_num, canonical in CANONICAL["8-K"].items():
                if item_num in header_upper:
                    return canonical
            return None

        if filing_type == "10-Q":
            if "ITEM 1A" in header_upper or "RISK" in header_upper:
                return "Item 1A (Risk)"
            if "ITEM 2" in header_upper or "MD&A" in header_upper or "MANAGEMENT" in header_upper:
                return "Item 7 (MD&A)"
            if "ITEM 1" in header_upper:
                return "Item 1"
            return None

        return None

    def _chunk_section(
        self, text: str, section: str, doc_id: str, filing_type: str
    ) -> List[Dict[str, Any]]:
        # Guarantee section is always a meaningful string, never None/empty
        if not section or not section.strip():
            section = FILING_TYPE_DEFAULT_SECTION.get(filing_type, filing_type)

        raw_blocks = [b.strip() for b in text.split("\n\n") if b.strip()]

        clean_blocks: List[str] = []
        for block in raw_blocks:
            words = block.split()
            if len(words) < MIN_BLOCK_WORDS:
                continue
            numeric_chars = sum(1 for c in block if c.isdigit() or c in ",.$()")
            if len(block) > 0 and numeric_chars / len(block) > MAX_NUMERIC_RATIO:
                continue
            clean_blocks.append(block)

        if not clean_blocks:
            return []

        chunks: List[Dict[str, Any]] = []
        current_words: List[str] = []
        char_offset = 0
        chunk_start = 0

        def make_chunk(words: List[str], start: int) -> Dict[str, Any]:
            content = " ".join(words)
            return {
                "id": str(uuid4()),
                "document_id": doc_id,
                "chunk_index": len(chunks),
                "content": content,
                "section": section,
                "word_count": len(words),
                "start_char": start,
                "end_char": start + len(content),
            }

        for block in clean_blocks:
            block_words = block.split()
            if len(current_words) + len(block_words) > MAX_WORDS and current_words:
                chunks.append(make_chunk(current_words, chunk_start))
                overlap = current_words[-OVERLAP_WORDS:] if len(current_words) > OVERLAP_WORDS else current_words[:]
                chunk_start = char_offset - len(" ".join(overlap))
                current_words = overlap
            current_words.extend(block_words)
            char_offset += len(block) + 2

        if len(current_words) >= MIN_BLOCK_WORDS:
            chunks.append(make_chunk(current_words, chunk_start))
        elif chunks and current_words:
            chunks[-1]["content"] += " " + " ".join(current_words)
            chunks[-1]["word_count"] += len(current_words)
            chunks[-1]["end_char"] = chunks[-1]["start_char"] + len(chunks[-1]["content"])

        final_chunks: List[Dict[str, Any]] = []
        for chunk in chunks:
            if chunk["word_count"] <= MAX_WORDS:
                final_chunks.append(chunk)
            else:
                split_chunks = self._split_oversized(chunk, doc_id, len(final_chunks))
                final_chunks.extend(split_chunks)

        for i, c in enumerate(final_chunks):
            c["chunk_index"] = i

        return final_chunks

    def _split_oversized(
        self, chunk: Dict[str, Any], doc_id: str, index_offset: int
    ) -> List[Dict[str, Any]]:
        sentences = chunk["content"].split(". ")
        result: List[Dict[str, Any]] = []
        current: List[str] = []

        for sentence in sentences:
            current.append(sentence)
            if len(" ".join(current).split()) >= MIN_WORDS:
                content = ". ".join(current).strip()
                result.append({
                    "id": str(uuid4()),
                    "document_id": doc_id,
                    "chunk_index": index_offset + len(result),
                    "content": content,
                    "section": chunk["section"],
                    "word_count": len(content.split()),
                    "start_char": chunk["start_char"],
                    "end_char": chunk["start_char"] + len(content),
                })
                current = []

        if current:
            content = ". ".join(current).strip()
            if result:
                result[-1]["content"] += " " + content
                result[-1]["word_count"] += len(content.split())
            else:
                result.append({
                    "id": str(uuid4()),
                    "document_id": doc_id,
                    "chunk_index": index_offset,
                    "content": content,
                    "section": chunk["section"],
                    "word_count": len(content.split()),
                    "start_char": chunk["start_char"],
                    "end_char": chunk["start_char"] + len(content),
                })

        return result

    def _save_parsed_to_s3(
        self, ticker: str, filing_type: str, filing_date: str,
        sections: Dict[str, str], doc_id: str,
    ) -> str:
        ft_norm = filing_type.upper().replace(" ", "").replace("-", "")
        s3_key = f"parsed/{ticker}/{ft_norm}/{filing_date}/{doc_id}.json.gz"
        payload = {
            "doc_id": doc_id,
            "ticker": ticker,
            "filing_type": filing_type,
            "sections": {k: v for k, v in sections.items()},
            "parsed_at": datetime.now(timezone.utc).isoformat(),
        }
        body = gzip.compress(json.dumps(payload).encode("utf-8"))
        self._s3.put_object(Bucket=self._bucket, Key=s3_key, Body=body, ContentType="application/gzip")
        return s3_key

    def _save_chunks(self, doc_id: str, chunks: List[Dict[str, Any]]) -> None:
        # Delete any stale rows for this document before inserting fresh chunks.
        # Prevents duplicates when a doc is re-queued for parsing.
        self._db.execute_update(
            f"DELETE FROM {CHUNKS_TABLE} WHERE document_id = %(doc_id)s",
            {"doc_id": doc_id},
        )

        for chunk in chunks:
            self._db.execute_update(
                f"""
                INSERT INTO {CHUNKS_TABLE}
                    (id, document_id, chunk_index, content, section,
                     word_count, start_char, end_char)
                VALUES
                    (%(id)s, %(doc_id)s, %(idx)s, %(content)s, %(section)s,
                     %(wc)s, %(sc)s, %(ec)s)
                """,
                {
                    "id": chunk["id"],
                    "doc_id": chunk["document_id"],
                    "idx": chunk["chunk_index"],
                    "content": chunk["content"],
                    "section": chunk["section"],
                    "wc": chunk["word_count"],
                    "sc": chunk.get("start_char", 0),
                    "ec": chunk.get("end_char", 0),
                },
            )

    def _update_doc_status(
        self, doc_id: str, status: str, extra: Optional[Dict[str, Any]] = None
    ) -> None:
        set_parts = ["status = %(status)s"]
        params: Dict[str, Any] = {"status": status, "id": doc_id}

        if extra:
            for k, v in extra.items():
                set_parts.append(f"{k} = %({k})s")
                params[k] = v

        self._db.execute_update(
            f"UPDATE {DOCUMENTS_TABLE} SET {', '.join(set_parts)} WHERE id = %(id)s",
            params,
        )

    def _resolve_company_id(self, ticker: str) -> str:
        rows = self._db.execute_query(
            "SELECT id FROM companies WHERE UPPER(ticker) = %(t)s AND is_deleted = FALSE LIMIT 1",
            {"t": ticker},
        )
        if rows:
            r = rows[0]
            return str(r.get("ID") or r.get("id"))

        logger.info("Company %s not found — auto-inserting into companies table", ticker)
        industry_rows = self._db.execute_query("SELECT id FROM industries LIMIT 1")
        if not industry_rows:
            raise RuntimeError("No industries in DB. Seed the industries table first.")
        ir = industry_rows[0]
        industry_id = str(ir.get("ID") or ir.get("id"))

        new_id = str(uuid4())
        self._db.execute_update(
            """
            INSERT INTO companies (id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at)
            VALUES (%(id)s, %(name)s, %(ticker)s, %(industry_id)s, 0.0, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """,
            {"id": new_id, "name": ticker, "ticker": ticker, "industry_id": industry_id},
        )
        logger.info("Auto-inserted company: %s → %s", ticker, new_id)
        return new_id

    def _get_download_folders(self, ticker: str, filing_type: str, limit: int) -> List[Path]:
        base = Path("data/raw/sec-edgar-filings") / ticker / filing_type
        if not base.exists():
            return []
        subdirs = [p for p in base.iterdir() if p.is_dir()]
        subdirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return subdirs[:limit]

    def _pick_main_file(self, folder: Path) -> Optional[Path]:
        candidates = list(folder.rglob("full-submission.txt"))
        if not candidates:
            candidates = (
                list(folder.rglob("*.txt"))
                + list(folder.rglob("*.html"))
                + list(folder.rglob("*.htm"))
                + list(folder.rglob("*.pdf"))
            )
        if not candidates:
            candidates = [p for p in folder.rglob("*") if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
        return candidates[0]

    def _sha256(self, path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for block in iter(lambda: f.read(1024 * 1024), b""):
                h.update(block)
        return h.hexdigest()

    def _s3_upload(self, local_path: Path, s3_key: str) -> bool:
        for attempt in range(1, 6):
            try:
                with local_path.open("rb") as f:
                    self._s3.put_object(
                        Bucket=self._bucket,
                        Key=s3_key,
                        Body=f.read(),
                        ContentType="text/plain",
                    )
                return True
            except (BotoCoreError, ClientError) as exc:
                wait = min(2 ** attempt, 30)
                logger.warning("S3 upload attempt %d failed: %s — retrying in %ds", attempt, exc, wait)
                time.sleep(wait)
        logger.error("S3 upload failed after 5 attempts: %s", s3_key)
        return False

    def _build_source_url(self, folder: Path, main_file: Path) -> Optional[str]:
        accession = folder.name
        parts = accession.split("-")
        if len(parts) < 3:
            return None
        cik = parts[0].lstrip("0") or "0"
        accession_nodashes = accession.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodashes}/{main_file.name}"

    def _extract_date(self, folder: Path) -> Optional[date]:
        parts = folder.name.split("-")
        if len(parts) < 3:
            return None
        try:
            yr = int(parts[1])
            yr = yr + 2000 if yr < 50 else yr + 1900
            return date(yr, 1, 1)
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _require_env(name: str) -> str:
        v = os.getenv(name)
        if not v:
            raise RuntimeError(f"Missing required env var: {name}")
        return v


def run_pipeline(tickers: Optional[List[str]] = None) -> None:
    if tickers:
        targets = [t.upper() for t in tickers]
    else:
        try:
            from app.services.snowflake import db
            rows = db.execute_query(
                "SELECT ticker FROM companies WHERE is_deleted = FALSE AND ticker IS NOT NULL"
            )
            targets = [r["ticker"] for r in rows]
            logger.info("Loaded %d active tickers from Snowflake", len(targets))
        except Exception as exc:
            logger.warning("Could not load tickers from Snowflake (%s), falling back to defaults", exc)
            targets = ["NVDA", "JPM", "WMT", "GE", "DG"]

    pipeline = SECPipeline()
    for ticker in targets:
        try:
            result = pipeline.run(ticker)
            logger.info("Result for %s: %s", ticker, result)
        except Exception as exc:
            logger.error("Pipeline failed for %s: %s", ticker, exc)


if __name__ == "__main__":
    import sys
    tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    run_pipeline(tickers)