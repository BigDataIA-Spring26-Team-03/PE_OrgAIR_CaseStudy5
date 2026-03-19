import sys
from pathlib import Path

from dotenv import load_dotenv

# Ensure repo root is on PYTHONPATH (Windows + Poetry safe)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load .env
load_dotenv(dotenv_path=ROOT / ".env")

from app.pipelines.document_parser_from_s3 import main

if __name__ == "__main__":
    main(limit=50)
