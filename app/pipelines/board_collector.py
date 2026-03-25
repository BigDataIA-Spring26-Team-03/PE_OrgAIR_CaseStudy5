# app/pipelines/board_collector.py
# SEC EDGAR proxy-statement fetcher — clean text extraction for LLM pipeline.

from __future__ import annotations

import json
import re
import time
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from app.pipelines.document_chunker_s3 import normalize_ws

logger = logging.getLogger(__name__)


# SEC EDGAR CIK numbers for known companies (fast-path cache)
COMPANY_CIKS: Dict[str, str] = {
    "NVDA": "1045810",
    "JPM": "19617",
    "WMT": "104169",
    "GE": "40545",
    "DG": "29534",
}

# Runtime CIK cache populated via SEC EDGAR company_tickers.json
_CIK_CACHE: Dict[str, str] = {}


def lookup_cik(ticker: str) -> Optional[str]:
    """Return the SEC EDGAR CIK for any ticker, resolving dynamically if needed."""
    ticker = ticker.upper()
    if ticker in COMPANY_CIKS:
        return COMPANY_CIKS[ticker]
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=EDGAR_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        for entry in resp.json().values():
            if entry.get("ticker", "").upper() == ticker:
                cik = str(entry["cik_str"]).zfill(10)
                _CIK_CACHE[ticker] = cik
                return cik
    except Exception as exc:
        logger.warning(f"CIK lookup failed for {ticker}: {exc}")
    return None

# Polite EDGAR headers (required by SEC fair-access policy)
EDGAR_HEADERS = {
    "User-Agent": "PE-OrgAIR research@example.com",
    "Accept-Encoding": "gzip, deflate",
}


class BoardCompositionCollector:
    """Collect board composition data from SEC DEF 14A proxy statements.
    """

    def __init__(self, data_dir: str = "data/board"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir = Path("data/board_raw")
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect_board_data(
        self, ticker: str, use_cache: bool = True
    ) -> Dict:
        """Main entry point: fetch proxy, extract clean text + committees + strategy.
        """
        ticker = ticker.upper()

        if use_cache:
            cached = self.load_from_cache(ticker)
            if cached:
                return cached

        raw_result = self.collect_raw_text(ticker)
        if not raw_result.get("raw_text"):
            logger.warning(f"No proxy text obtained for {ticker}, returning empty")
            return {
                "raw_text": "",
                "members": [],
                "committees": [],
                "strategy_text": "",
                "source_meta": {},
            }

        raw_text = raw_result["raw_text"]
        text_lower = raw_text.lower()

        committees = self._extract_committees(text_lower)
        strategy_text = self._extract_strategy_text(text_lower)

        data = {
            "raw_text": raw_text,
            "members": [],  # populated by LLM extractor in stage 3
            "committees": committees,
            "strategy_text": strategy_text,
            "source_meta": raw_result.get("source_meta", {}),
        }

        self._cache_results(ticker, data)
        return data

    def collect_raw_text(self, ticker: str) -> Dict:
        """Fetch DEF 14A and return clean text + source metadata."""
        ticker = ticker.upper()

        # Check raw text cache
        raw_cache = self.raw_dir / f"{ticker}.json"
        if raw_cache.exists():
            try:
                with open(raw_cache, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                if cached.get("raw_text"):
                    logger.info(f"Loaded raw text from cache for {ticker}")
                    return cached
            except Exception as e:
                logger.warning(f"Error loading raw cache for {ticker}: {e}")

        proxy_html = self._fetch_latest_proxy(ticker)
        if not proxy_html:
            return {"ticker": ticker, "source_meta": {}, "raw_text": ""}

        # Strip HTML to clean text
        soup = BeautifulSoup(proxy_html, "html.parser")
        for tag in soup(["script", "style", "meta", "link"]):
            tag.decompose()
        raw_text = soup.get_text(separator="\n", strip=True)
        raw_text = normalize_ws(raw_text)

        result = {
            "ticker": ticker,
            "source_meta": {
                "cik": lookup_cik(ticker) or "",
                "filing_type": "DEF 14A",
                "collected_at": datetime.now().isoformat(),
            },
            "raw_text": raw_text,
        }

        # Cache raw text
        with open(raw_cache, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        logger.info(f"Cached raw text to {raw_cache}")

        return result

    # ------------------------------------------------------------------
    # EDGAR fetching
    # ------------------------------------------------------------------

    def _fetch_latest_proxy(self, ticker: str) -> Optional[str]:
        """Fetch the most recent DEF 14A filing HTML from EDGAR."""
        cik = lookup_cik(ticker)
        if not cik:
            logger.warning(f"No CIK found for ticker {ticker}")
            return None

        submissions_url = (
            f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        )

        try:
            logger.info(f"Fetching EDGAR submissions for {ticker} (CIK {cik})")
            resp = requests.get(submissions_url, headers=EDGAR_HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            filings = data.get("filings", {}).get("recent", {})
            forms = filings.get("form", [])
            accessions = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            for i, form in enumerate(forms):
                if form == "DEF 14A":
                    accession = accessions[i].replace("-", "")
                    doc = primary_docs[i]
                    filing_url = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{cik}/{accession}/{doc}"
                    )
                    logger.info(f"Found DEF 14A for {ticker}: {filing_url}")
                    time.sleep(0.2)
                    doc_resp = requests.get(
                        filing_url, headers=EDGAR_HEADERS, timeout=30
                    )
                    doc_resp.raise_for_status()
                    return doc_resp.text

            logger.warning(f"No DEF 14A found in recent filings for {ticker}")
            return None

        except requests.RequestException as e:
            logger.error(f"EDGAR request failed for {ticker}: {e}")
            return None

    # ------------------------------------------------------------------
    # Deterministic text extraction
    # ------------------------------------------------------------------

    def _extract_committees(self, full_text_lower: str) -> List[str]:
        """Extract board committee names from the proxy text."""
        committees: List[str] = []
        seen: set = set()

        committee_patterns = [
            r"(audit\s+committee)",
            r"(compensation\s+committee)",
            r"(nominating\s+(?:and\s+)?(?:corporate\s+)?governance\s+committee)",
            r"(technology\s+(?:and\s+)?(?:\w+\s+)?committee)",
            r"(risk\s+(?:management\s+)?committee)",
            r"(innovation\s+committee)",
            r"(digital\s+(?:\w+\s+)?committee)",
            r"(executive\s+committee)",
            r"(finance\s+committee)",
            r"(cyber(?:security)?\s+committee)",
            r"(data\s+(?:\w+\s+)?committee)",
        ]

        for pattern in committee_patterns:
            matches = re.findall(pattern, full_text_lower)
            for m in matches:
                name = m.strip().title()
                if name not in seen:
                    committees.append(name)
                    seen.add(name)

        logger.info(f"Extracted {len(committees)} committees")
        return committees

    def _extract_strategy_text(self, full_text_lower: str) -> str:
        """Extract strategy-related passages mentioning AI/ML/technology."""
        keywords = [
            "artificial intelligence",
            "machine learning",
            "ai strategy",
            "digital transformation",
            "technology strategy",
        ]

        passages: List[str] = []
        sentences = re.split(r"[.!?]+", full_text_lower)
        for sentence in sentences:
            if any(kw in sentence for kw in keywords):
                clean = sentence.strip()
                if 20 < len(clean) < 500:
                    passages.append(clean)

        return ". ".join(passages[:10])

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def _cache_results(self, ticker: str, data: Dict) -> None:
        """Save parsed board data to JSON cache."""
        cache_file = self.data_dir / f"{ticker}.json"
        payload = {
            "ticker": ticker,
            "source": "SEC EDGAR DEF 14A",
            "collected_at": datetime.now().isoformat(),
            "member_count": len(data.get("members", [])),
            **data,
        }
        # Don't cache the full raw_text in the board cache (it's in board_raw/)
        payload.pop("raw_text", None)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"Cached board data to {cache_file}")

    def load_from_cache(self, ticker: str) -> Optional[Dict]:
        """Load board data from cache if available."""
        cache_file = self.data_dir / f"{ticker.upper()}.json"
        if not cache_file.exists():
            return None
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Only use cache if it has members (i.e., LLM extraction already ran)
            if not data.get("members"):
                return None
            logger.info(f"Loaded board data from cache for {ticker}")
            return {
                "members": data.get("members", []),
                "committees": data.get("committees", []),
                "strategy_text": data.get("strategy_text", ""),
                "source_meta": data.get("source_meta", {}),
            }
        except Exception as e:
            logger.error(f"Error loading board cache for {ticker}: {e}")
            return None

    def cache_with_members(self, ticker: str, data: Dict) -> None:
        """Cache final board data including LLM-extracted members."""
        cache_file = self.data_dir / f"{ticker.upper()}.json"
        payload = {
            "ticker": ticker,
            "source": "SEC EDGAR DEF 14A + LLM extraction",
            "collected_at": datetime.now().isoformat(),
            "member_count": len(data.get("members", [])),
            "members": data.get("members", []),
            "committees": data.get("committees", []),
            "strategy_text": data.get("strategy_text", ""),
            "source_meta": data.get("source_meta", {}),
        }
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"Cached final board data with {payload['member_count']} members to {cache_file}")
