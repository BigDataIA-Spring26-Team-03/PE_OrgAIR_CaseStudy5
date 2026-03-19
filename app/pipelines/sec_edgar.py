from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import List, Dict, Optional
from uuid import uuid4

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, SSLError
from dotenv import load_dotenv
from sec_edgar_downloader import Downloader

from app.services.snowflake import SnowflakeService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]

DOCUMENTS_TABLE = "documents_sec"
CHUNKS_TABLE = "document_chunks_sec"

DEFAULT_FILING_TYPES = {
    "10-K": 3,
    "10-Q": 4,
    "8-K": 5,
    "DEF 14A": 2,
}

FILING_TYPES = list(DEFAULT_FILING_TYPES.keys())

SEC_REQUEST_SLEEP_SECONDS = float(os.getenv("SEC_SLEEP_SECONDS", "0.75"))
AFTER_DATE = os.getenv("SEC_AFTER_DATE", "2021-01-01")


def _get_default_industry_id(sf: SnowflakeService) -> str:
    rows = sf.execute_query("SELECT id FROM industries LIMIT 1")
    if not rows:
        raise RuntimeError("No industries found in database. Seed the industries table first.")
    r = rows[0]
    return str(r.get("ID") or r.get("id"))


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_all_download_folders(ticker: str, filing_type: str, limit: int) -> list[Path]:
    base = Path("data/raw") / "sec-edgar-filings" / ticker / filing_type
    if not base.exists():
        return []
    subdirs = [p for p in base.iterdir() if p.is_dir()]
    if not subdirs:
        return []
    subdirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return subdirs[:limit]


def pick_main_file(folder: Path) -> Optional[Path]:
    candidates = list(folder.rglob("full-submission.txt"))
    if not candidates:
        candidates = (
            list(folder.rglob("*.txt")) +
            list(folder.rglob("*.html")) +
            list(folder.rglob("*.htm"))
        )
    if not candidates:
        candidates = [p for p in folder.rglob("*") if p.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0]


def filing_type_for_paths(filing_type: str) -> str:
    t = filing_type.upper().strip()
    t = t.replace(" ", "")
    t = t.replace("-", "")
    return t


def build_sec_source_url(download_folder: Path, main_file: Path) -> Optional[str]:
    accession = download_folder.name
    parts = accession.split("-")
    if len(parts) < 3:
        return None
    cik = parts[0].lstrip("0") or "0"
    accession_nodashes = accession.replace("-", "")
    filename = main_file.name
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodashes}/{filename}"


def extract_filing_date_from_folder(folder: Path) -> Optional[date]:
    accession = folder.name
    parts = accession.split("-")
    if len(parts) < 3:
        return None
    try:
        year_str = parts[1]
        year = int(year_str)
        if year < 50:
            year += 2000
        else:
            year += 1900
        return date(year, 1, 1)
    except (ValueError, IndexError):
        return None


def upload_file_with_retry(
    s3_client,
    file_path: str,
    bucket: str,
    s3_key: str,
    transfer_config: TransferConfig,
    max_retries: int = 5,
) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            with open(file_path, "rb") as f:
                file_content = f.read()
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=file_content,
                ContentType="text/plain",
            )
            logger.debug(f"Upload successful on attempt {attempt}")
            return True

        except (SSLError, BotoCoreError, ClientError) as e:
            error_msg = str(e)
            is_retryable = (
                "SSL" in error_msg
                or "EOF occurred" in error_msg
                or "Connection was closed" in error_msg
                or "timeout" in error_msg.lower()
                or "timed out" in error_msg.lower()
            )
            if not is_retryable or attempt == max_retries:
                logger.error(f"S3 upload failed after {attempt} attempts: {error_msg[:150]}")
                return False
            wait_time = min(2 ** attempt, 30)
            logger.warning(f"Upload attempt {attempt}/{max_retries} failed, retrying in {wait_time}s...")
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}: {str(e)[:150]}")
            if attempt == max_retries:
                return False
            time.sleep(2 ** attempt)

    return False


def collect_for_tickers(
    tickers: List[str],
    filing_types: List[str],
    limit_per_type: int = 1,
    after: Optional[str] = None,
) -> None:
    target_tickers = [t.upper().strip() for t in tickers if t and t.strip()]
    filing_type_map = {ft: int(limit_per_type) for ft in filing_types}
    after_date = after or AFTER_DATE
    main(
        target_tickers=target_tickers,
        filing_type_map=filing_type_map,
        after_date=after_date,
    )


def main(
    target_tickers: Optional[List[str]] = None,
    filing_type_map: Optional[Dict[str, int]] = None,
    after_date: Optional[str] = None,
) -> None:
    tickers = target_tickers or DEFAULT_TARGET_TICKERS
    ftype_map = filing_type_map or DEFAULT_FILING_TYPES
    ftypes_list = list(ftype_map.keys())
    after = after_date or AFTER_DATE

    logger.info("=" * 60)
    logger.info("SEC EDGAR DOWNLOADER")
    logger.info("=" * 60)

    email = require_env("SEC_EDGAR_USER_AGENT_EMAIL")
    bucket = require_env("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "us-east-1")

    logger.info(f"Tickers: {tickers}")
    logger.info(f"Filing types: {ftypes_list}")
    logger.info(f"After Date: {after}")
    logger.info(f"Rate Limit: {SEC_REQUEST_SLEEP_SECONDS}s between requests")

    download_root = Path("data/raw")
    download_root.mkdir(parents=True, exist_ok=True)

    dl = Downloader("OrgAIR", email, str(download_root))

    transfer_config = TransferConfig(
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True,
        max_io_queue=1000,
    )

    s3 = boto3.client(
        "s3",
        region_name=region,
        config=Config(
            retries={"max_attempts": 15, "mode": "adaptive"},
            connect_timeout=60,
            read_timeout=300,
            tcp_keepalive=True,
            signature_version="s3v4",
            max_pool_connections=50,
        ),
    )

    sf = SnowflakeService()

    logger.info("\nFetching companies from database...")

    placeholders = ",".join([f"%(t{i})s" for i in range(len(tickers))])
    company_rows = sf.execute_query(
        f"""
        SELECT id, ticker
        FROM companies
        WHERE is_deleted = FALSE
          AND UPPER(ticker) IN ({placeholders})
        """,
        {f"t{i}": tickers[i] for i in range(len(tickers))},
    )

    ticker_to_company: dict[str, str] = {}
    for r in company_rows:
        tid = r.get("TICKER") if "TICKER" in r else r.get("ticker")
        cid = r.get("ID") if "ID" in r else r.get("id")
        ticker_to_company[str(tid).upper()] = str(cid)

    missing = [t for t in tickers if t not in ticker_to_company]
    if missing:
        logger.info(f"Auto-inserting {len(missing)} unknown companies: {missing}")
        default_industry_id = _get_default_industry_id(sf)
        for ticker in missing:
            new_id = str(uuid4())
            sf.execute_update(
                """
                INSERT INTO companies (id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at)
                VALUES (%(id)s, %(name)s, %(ticker)s, %(industry_id)s, 0.0, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """,
                {"id": new_id, "name": ticker, "ticker": ticker, "industry_id": default_industry_id},
            )
            ticker_to_company[ticker] = new_id
            logger.info(f"  Auto-inserted company: {ticker} → {new_id}")

    stats = {
        "inserted": 0,
        "skipped_dedup": 0,
        "skipped_missing_file": 0,
        "skipped_sec_download_error": 0,
        "skipped_s3_upload_error": 0,
        "skipped_invalid_folder": 0,
    }

    run_date = date.today().isoformat()

    logger.info("\nStarting downloads...")
    logger.info(f"Expected total: {sum(ftype_map.values()) * len(tickers)} documents")

    for ticker_idx, ticker in enumerate(tickers, 1):
        logger.info(f"\n[{ticker_idx}/{len(tickers)}] Processing {ticker}...")

        company_stats = {ft: 0 for ft in ftypes_list}

        for filing_type in ftypes_list:
            limit = ftype_map[filing_type]

            logger.info(f"  {filing_type}: Downloading up to {limit} filings...")

            try:
                dl.get(filing_type, ticker, limit=limit, after=after)
                logger.info(f"  {filing_type}: Download request completed")
            except Exception as e:
                logger.error(f"SEC download failed for {ticker} {filing_type}: {e}")
                stats["skipped_sec_download_error"] += 1
                time.sleep(SEC_REQUEST_SLEEP_SECONDS)
                continue

            time.sleep(SEC_REQUEST_SLEEP_SECONDS)

            folders = get_all_download_folders(ticker, filing_type, limit)

            if not folders:
                logger.warning(f"  No download folders found for {ticker} {filing_type}")
                stats["skipped_missing_file"] += 1
                continue

            logger.info(f"  {filing_type}: Found {len(folders)} downloaded filings")

            for folder_idx, folder in enumerate(folders, 1):
                try:
                    main_file = pick_main_file(folder)
                    if not main_file:
                        logger.warning(f"  No main file in {folder.name}")
                        stats["skipped_missing_file"] += 1
                        continue

                    content_hash = sha256_file(main_file)

                    existing = sf.execute_query(
                        f"""
                        SELECT id
                        FROM {DOCUMENTS_TABLE}
                        WHERE ticker = %(ticker)s
                          AND filing_type = %(filing_type)s
                          AND content_hash = %(content_hash)s
                        LIMIT 1
                        """,
                        {
                            "ticker": ticker,
                            "filing_type": filing_type,
                            "content_hash": content_hash,
                        },
                    )

                    if existing:
                        logger.debug(f"Skipping duplicate: {folder.name}")
                        stats["skipped_dedup"] += 1
                        continue

                    doc_id = str(uuid4())
                    ext = main_file.suffix or ".txt"
                    ft_path = filing_type_for_paths(filing_type)
                    s3_key = f"sec/{ticker}/{ft_path}/{run_date}/{doc_id}{ext}"

                    upload_success = upload_file_with_retry(
                        s3,
                        main_file,
                        bucket,
                        s3_key,
                        transfer_config,
                        max_retries=5,
                    )

                    if not upload_success:
                        stats["skipped_s3_upload_error"] += 1
                        continue

                    source_url = build_sec_source_url(folder, main_file)
                    filing_date = extract_filing_date_from_folder(folder) or date.today()

                    sf.execute_update(
                        f"""
                        INSERT INTO {DOCUMENTS_TABLE}
                          (id, company_id, ticker, filing_type, filing_date,
                           source_url, local_path, s3_key, content_hash,
                           status, created_at)
                        VALUES
                          (%(id)s, %(company_id)s, %(ticker)s, %(filing_type)s, %(filing_date)s,
                           %(source_url)s, %(local_path)s, %(s3_key)s, %(content_hash)s,
                           'downloaded', CURRENT_TIMESTAMP())
                        """,
                        {
                            "id": doc_id,
                            "company_id": ticker_to_company[ticker],
                            "ticker": ticker,
                            "filing_type": filing_type,
                            "filing_date": filing_date,
                            "source_url": source_url,
                            "local_path": str(main_file),
                            "s3_key": s3_key,
                            "content_hash": content_hash,
                        },
                    )

                    stats["inserted"] += 1
                    company_stats[filing_type] += 1

                    logger.info(f"  [{folder_idx}/{len(folders)}] Processed: {doc_id[:8]}...")

                except Exception as e:
                    logger.error(f"Error processing {folder.name}: {e}")
                    stats["skipped_invalid_folder"] += 1
                    continue

        total_for_company = sum(company_stats.values())
        logger.info(f"  {ticker} Summary: {total_for_company} documents")
        for ft, count in company_stats.items():
            logger.info(f"    - {ft}: {count}")

    logger.info("\n" + "=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Inserted documents:       {stats['inserted']}")
    logger.info(f"Skipped (duplicate):      {stats['skipped_dedup']}")
    logger.info(f"Skipped (missing file):   {stats['skipped_missing_file']}")
    logger.info(f"Skipped (SEC error):      {stats['skipped_sec_download_error']}")
    logger.info(f"Skipped (S3 error):       {stats['skipped_s3_upload_error']}")
    logger.info(f"Skipped (invalid folder): {stats['skipped_invalid_folder']}")
    logger.info("=" * 60)

    expected = sum(ftype_map.values()) * len(tickers)
    logger.info(f"\nExpected: ~{expected} documents")
    logger.info(f"Actual:    {stats['inserted']} documents")

    if stats["inserted"] >= 90:
        logger.info("SUCCESS: Downloaded 90+ documents!")
    else:
        logger.warning(f"WARNING: Only {stats['inserted']} documents (target: 90+)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"\nFatal error: {e}", exc_info=True)
        raise