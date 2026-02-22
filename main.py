import asyncio
import logging
from src.matcher import Matcher
from src.config import STATEMENTS_DIR
from src.email_client import fetch_financial_emails
from src.google_process import process_file,process_unmatched_emails


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#-----------------------------
# :: Main Function
#-----------------------------

""" 
Asynchronously loads the AI model, processes all files in STATEMENTS_DIR against fetched 
financial emails, handles errors gracefully, and processes unmatched emails.
"""

async def main():
    try:
        model = Matcher.get_model()
        logger.info("AI model loaded successfully at startup")
        if not STATEMENTS_DIR.exists() or not STATEMENTS_DIR.is_dir():
            raise FileNotFoundError(f"STATEMENTS_DIR not found: {STATEMENTS_DIR}")
        all_files = list(STATEMENTS_DIR.glob("*.*"))
        if not all_files:
            logger.warning("No files found in statements folder")
            return
        logger.info(f"Found {len(all_files)} files in statements folder")
        emails = await fetch_financial_emails(limit=500)
        logger.info(f"Fetched {len(emails)} financial emails")
        semaphore = asyncio.Semaphore(1)
        matched_email_hashes = set()
        async def safe_process(file_path):
            try:
                return await process_file(file_path, emails, semaphore)
            except Exception as e:
                logger.error(f"Failed to process file '{file_path.name}': {e}")
                return set()
        results = await asyncio.gather(*(safe_process(f) for f in all_files))
        for matched_hashes in results:
            matched_email_hashes.update(matched_hashes or set())
        await process_unmatched_emails(emails, matched_email_hashes)
        logger.info("All files and unmatched emails processed successfully")
    except FileNotFoundError as fnf_err:
        logger.critical(f"File not found error: {fnf_err}")
    except Exception as exc:
        logger.critical(f"Unexpected error in main execution: {exc}", exc_info=True)

#-----------------------------
# :: Run Main
#-----------------------------

""" 
This block ensures that your script runs the asynchronous main() function only when the file
is executed directly, not when it's imported as a module:
"""

if __name__ == "__main__":
    asyncio.run(main())