import sys
import logging

from app.pipelines.sec_edgar import main as download_main

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Entry point for Poetry script."""
    try:
        logger.info("Starting SEC EDGAR downloader...")
        download_main()
        logger.info("Download completed successfully!")
        return 0
    except KeyboardInterrupt:
        logger.warning("\nDownload interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\nDownload failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())    