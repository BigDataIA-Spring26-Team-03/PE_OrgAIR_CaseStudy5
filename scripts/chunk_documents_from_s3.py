from pathlib import Path
import sys
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

print("🚀 Starting chunk_documents_from_s3.py ...")

from app.pipelines.document_chunker_s3 import main

if __name__ == "__main__":
    main(limit=1000)
