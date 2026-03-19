import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv(dotenv_path=ROOT / ".env")

from app.pipelines.document_text_cleaner import main  # noqa: E402


if __name__ == "__main__":
    main(limit=50)
