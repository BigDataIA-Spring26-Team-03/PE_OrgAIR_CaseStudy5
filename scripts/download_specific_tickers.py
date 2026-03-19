"""
Download filings for specific tickers only.

Usage:
    poetry run python scripts/download_specific_tickers.py JPM GS
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Temporarily override TARGET_TICKERS
import app.pipelines.sec_edgar as sec_module

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/download_specific_tickers.py JPM GS")
        sys.exit(1)
    
    # Override target tickers
    sec_module.TARGET_TICKERS = [t.upper() for t in sys.argv[1:]]
    
    print(f"Downloading for tickers: {sec_module.TARGET_TICKERS}")
    
    # Run main
    sec_module.main()