"""
Backfill S3 Uploads from Local Files

Uploads files to S3 for documents that have local_path but no s3_key.
Uses AWS CLI or put_object for better reliability.

Usage:
    poetry run python scripts/backfill_s3.py
    poetry run python scripts/backfill_s3.py --limit 20
    poetry run python scripts/backfill_s3.py --ticker JPM
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, SSLError
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.services.snowflake import SnowflakeService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================================================================
# CONFIGURATION
# =========================================================================

DOCUMENTS_TABLE = "documents_sec"
USE_AWS_CLI = True  # Use AWS CLI instead of boto3 (more reliable)

# =========================================================================
# UTILITY FUNCTIONS
# =========================================================================

def require_env(name: str) -> str:
    """Get required environment variable or raise error."""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def filing_type_for_paths(filing_type: str) -> str:
    """Normalize filing type for S3 paths."""
    t = filing_type.upper().strip()
    t = t.replace(" ", "")
    t = t.replace("-", "")
    return t


def upload_with_aws_cli(local_path: str, bucket: str, s3_key: str) -> bool:
    """
    Upload file using AWS CLI (most reliable for large files).
    
    Returns True if successful, False otherwise.
    """
    s3_uri = f"s3://{bucket}/{s3_key}"
    
    try:
        # Run AWS CLI command
        result = subprocess.run(
            ["aws", "s3", "cp", local_path, s3_uri, "--no-progress"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            logger.info(f"    ✅ AWS CLI upload successful")
            return True
        else:
            logger.error(f"    ❌ AWS CLI failed: {result.stderr[:200]}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"    ❌ AWS CLI timeout after 5 minutes")
        return False
    except FileNotFoundError:
        logger.error(f"    ❌ AWS CLI not found. Install it: https://aws.amazon.com/cli/")
        return False
    except Exception as e:
        logger.error(f"    ❌ AWS CLI error: {e}")
        return False


def upload_with_boto3(
    s3_client,
    local_path: str,
    bucket: str,
    s3_key: str,
    max_retries: int = 3
) -> bool:
    """
    Upload file using boto3 put_object with retry.
    
    Returns True if successful, False otherwise.
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Read file and upload
            with open(local_path, 'rb') as f:
                file_content = f.read()
            
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=file_content,
                ContentType='text/plain'
            )
            
            logger.info(f"    ✅ boto3 upload successful on attempt {attempt}")
            return True
            
        except (SSLError, BotoCoreError, ClientError) as e:
            error_msg = str(e)
            
            if attempt == max_retries:
                logger.error(f"    ❌ boto3 failed after {attempt} attempts: {error_msg[:100]}")
                return False
            
            wait_time = 2 ** attempt
            logger.warning(f"    ⚠️ Attempt {attempt}/{max_retries} failed, retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    return False


def row_get(row: dict, *keys: str):
    """Safe dictionary access for uppercase/lowercase Snowflake columns."""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None

# =========================================================================
# MAIN BACKFILL FUNCTION
# =========================================================================

def main(limit: Optional[int] = None, ticker_filter: Optional[str] = None) -> None:
    """
    Backfill S3 uploads for documents that have local files but no S3 key.
    
    Args:
        limit: Maximum number of documents to process
        ticker_filter: Only process this ticker (e.g., 'JPM')
    """
    logger.info("=" * 60)
    logger.info("S3 BACKFILL FROM LOCAL FILES")
    logger.info("=" * 60)
    
    # -------------------------------------------------------------------------
    # INITIALIZATION
    # -------------------------------------------------------------------------
    
    bucket = require_env("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "us-east-1")
    
    logger.info(f"S3 Bucket: {bucket}")
    logger.info(f"AWS Region: {region}")
    logger.info(f"Upload Method: {'AWS CLI' if USE_AWS_CLI else 'boto3'}")
    if ticker_filter:
        logger.info(f"Ticker Filter: {ticker_filter}")
    
    # Initialize Snowflake
    sf = SnowflakeService()
    
    # Initialize S3 (only if not using AWS CLI)
    s3 = None
    if not USE_AWS_CLI:
        s3 = boto3.client(
            "s3",
            region_name=region,
            config=Config(
                retries={"max_attempts": 10, "mode": "adaptive"},
                connect_timeout=60,
                read_timeout=300,
            ),
        )
    
    # -------------------------------------------------------------------------
    # QUERY DOCUMENTS NEEDING S3 UPLOAD
    # -------------------------------------------------------------------------
    
    logger.info("\nQuerying documents needing S3 upload...")
    
    # Build query
    where_clause = "WHERE (s3_key IS NULL OR s3_key = '') AND local_path IS NOT NULL"
    if ticker_filter:
        where_clause += f" AND ticker = '{ticker_filter.upper()}'"
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
        SELECT id, company_id, ticker, filing_type, filing_date, local_path, content_hash
        FROM {DOCUMENTS_TABLE}
        {where_clause}
        ORDER BY ticker, filing_type, filing_date
        {limit_clause}
    """
    
    rows = sf.execute_query(query)
    
    if not rows:
        logger.info("No documents need S3 upload!")
        return
    
    logger.info(f"Found {len(rows)} documents to upload\n")
    
    # -------------------------------------------------------------------------
    # PROCESS EACH DOCUMENT
    # -------------------------------------------------------------------------
    
    stats = {
        "uploaded": 0,
        "skipped_missing_file": 0,
        "failed": 0
    }
    
    run_date = date.today().isoformat()
    
    for idx, r in enumerate(rows, 1):
        doc_id = row_get(r, "id", "ID")
        ticker = (row_get(r, "ticker", "TICKER") or "").upper()
        filing_type = row_get(r, "filing_type", "FILING_TYPE") or ""
        local_path = row_get(r, "local_path", "LOCAL_PATH")
        
        if not doc_id or not local_path:
            logger.warning(f"[{idx}/{len(rows)}] ⚠️ Skipping malformed row")
            continue
        
        logger.info(f"[{idx}/{len(rows)}] Processing: {ticker} {filing_type}")
        logger.info(f"    Local: {local_path}")
        
        # Check if file exists
        local_file = Path(local_path)
        if not local_file.exists():
            logger.warning(f"    ⚠️ Local file not found!")
            stats["skipped_missing_file"] += 1
            continue
        
        # Build S3 key
        ft_path = filing_type_for_paths(filing_type)
        ext = local_file.suffix or ".txt"
        s3_key = f"sec/{ticker}/{ft_path}/{run_date}/{doc_id}{ext}"
        
        logger.info(f"    S3: s3://{bucket}/{s3_key}")
        
        # Upload
        upload_success = False
        
        if USE_AWS_CLI:
            upload_success = upload_with_aws_cli(str(local_file), bucket, s3_key)
        else:
            upload_success = upload_with_boto3(s3, str(local_file), bucket, s3_key)
        
        if not upload_success:
            stats["failed"] += 1
            logger.error(f"    ❌ Upload failed for {ticker} {filing_type}")
            continue
        
        # Update Snowflake with S3 key
        sf.execute_update(
            f"""
            UPDATE {DOCUMENTS_TABLE}
            SET s3_key = %(s3_key)s
            WHERE id = %(id)s
            """,
            {"id": doc_id, "s3_key": s3_key}
        )
        
        stats["uploaded"] += 1
        logger.info(f"    ✅ Uploaded and updated Snowflake")
    
    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    
    logger.info("\n" + "=" * 60)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Uploaded:              {stats['uploaded']}")
    logger.info(f"⚠️  Skipped (missing):     {stats['skipped_missing_file']}")
    logger.info(f"❌ Failed:                {stats['failed']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill S3 uploads from local files")
    parser.add_argument("--limit", type=int, help="Max documents to process")
    parser.add_argument("--ticker", type=str, help="Filter by ticker (e.g., JPM)")
    
    args = parser.parse_args()
    
    try:
        main(limit=args.limit, ticker_filter=args.ticker)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}", exc_info=True)
        raise