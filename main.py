import asyncio
import logging
from src.matcher import Matcher
from src.email_client import fetch_financial_emails
from src.google_process import process_file,process_unmatched_emails
from src.config import (
    STATEMENTS_DIR,
)

#-----------------------------
# :: Logger Variable
#-----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#-----------------------------
# :: Main Function
#-----------------------------
async def main():
    try:
        model = Matcher.get_model()
        logger.info("AI model loaded successfully at startup")

        if not STATEMENTS_DIR.exists() or not STATEMENTS_DIR.is_dir():
            raise FileNotFoundError("STATEMENTS_DIR folder not found")

        all_files = list(STATEMENTS_DIR.glob("*.*"))
        if not all_files:
            logger.info("No files found in statements folder")
            return

        logger.info(f"Found {len(all_files)} files in statements folder")

        emails = await fetch_financial_emails(limit=500)
        logger.info(f"Fetched {len(emails)} financial emails")

        semaphore = asyncio.Semaphore(1)
        matched_email_hashes = set()

        async def safe_process(file):
            try:
                return await process_file(file, emails, semaphore)
            except Exception as e:
                logger.error(f"Error processing file {file.name}: {e}")
                return set()

        results = await asyncio.gather(*(safe_process(f) for f in all_files), return_exceptions=False)
        for r in results:
            if r:
                matched_email_hashes.update(r)

        await process_unmatched_emails(emails, matched_email_hashes)
        logger.info("All files and unmatched emails processed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")

#-----------------------------
# :: Run Main
#-----------------------------


if __name__ == "__main__":
    asyncio.run(main())