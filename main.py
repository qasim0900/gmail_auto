import json
import asyncio
import hashlib
import logging
from io import BytesIO
from pathlib import Path

from src.config import STATEMENTS_DIR, DRIVE_FOLDER_ID, OTHER_EMAIL_FOLDER_ID
from src.matcher import Matcher
from src.pdf_parser import extract_records_from_file
from src.drive_uploader import drive_sheet_manager, upload_to_drive
from src.email_client import fetch_financial_emails

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set to track processed records (hashes)
processed_records = set()


# ------------------------------
# :: Process each statement file
# ------------------------------
async def process_file(file_path: Path, emails: list, semaphore: asyncio.Semaphore):
    async with semaphore:
        records = await asyncio.to_thread(extract_records_from_file, file_path)
        if not records:
            logger.warning(f"No records found in {file_path}")
            return set()

        sheet_name = f"{file_path.stem}_check_records"
        matched_records = []
        matched_email_hashes = set()

        for idx, record in enumerate(records):
            record_hash = hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
            if record_hash in processed_records:
                continue
            processed_records.add(record_hash)

            email, score = Matcher.match_record_email(record, emails)
            attachment_paths = []

            if email and score > 0.7:
                matched_email_hashes.add(email["hash"])
                for att_idx, att in enumerate(email["attachments"]):
                    ext = Path(att["filename"]).suffix or ".bin"
                    save_name = f"{email['sender_email']}_{email['hash']}_{idx}{ext}"
                    temp_file = BytesIO(att["content"])
                    temp_file.name = save_name
                    try:
                        file_id = upload_to_drive(temp_file, DRIVE_FOLDER_ID)
                        if file_id:
                            attachment_paths.append(
                                f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                            )
                    except Exception as e:
                        logger.error(f"Failed to upload attachment {save_name}: {e}")

                rec_data = {
                    "sender_name": email["sender_name"],
                    "sender_email": email["sender_email"],
                    "email_received_time": email["date"],
                    "attach_file_or_image_path": ", ".join(attachment_paths),
                    "email_link": ""
                }
                matched_records.append(rec_data)

        if matched_records:
            try:
                await asyncio.to_thread(
                    drive_sheet_manager,
                    sheet_name,
                    DRIVE_FOLDER_ID,
                    matched_records,
                    True
                )
            except Exception as e:
                logger.error(f"Failed to update sheet {sheet_name}: {e}")

        return matched_email_hashes


# ------------------------------
# :: Process unmatched emails
# ------------------------------
async def process_unmatched_emails(emails: list, matched_email_hashes: set):
    sheet_name = "other_email"
    unmatched_records = []

    for email in emails:
        if email['hash'] not in matched_email_hashes:
            attachment_paths = []
            for idx, att in enumerate(email['attachments']):
                ext = Path(att["filename"]).suffix or ".bin"
                save_name = f"{email['sender_email']}_{email['hash']}_{idx}{ext}"
                temp_file = BytesIO(att["content"])
                temp_file.name = save_name
                try:
                    file_id = upload_to_drive(temp_file, OTHER_EMAIL_FOLDER_ID)
                    attachment_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
                except Exception as e:
                    logger.error(f"Failed to upload unmatched attachment {save_name}: {e}")

            rec_data = {
                "sender_name": email["sender_name"],
                "sender_email": email["sender_email"],
                "email_received_time": email["date"],
                "attach_file_or_image_path": ", ".join(attachment_paths),
                "email_link": ""
            }
            unmatched_records.append(rec_data)

    if unmatched_records:
        try:
            await asyncio.to_thread(
                drive_sheet_manager,
                sheet_name,
                OTHER_EMAIL_FOLDER_ID,
                unmatched_records,
                True
            )
            logger.info(f"Processed {len(unmatched_records)} unmatched emails")
        except Exception as e:
            logger.error(f"Failed to update unmatched emails sheet: {e}")


# ------------------------------
# :: Main Async Function
# ------------------------------
async def main():
    if not STATEMENTS_DIR.exists() or not STATEMENTS_DIR.is_dir():
        raise FileNotFoundError("STATEMENTS_DIR folder is not found")

    all_files = list(STATEMENTS_DIR.glob("*.*"))
    if not all_files:
        logger.info("No files found in statements folder")
        return

    logger.info(f"Found {len(all_files)} files in statements folder")

    # Fetch financial emails
    emails = await fetch_financial_emails(limit=500)
    logger.info(f"Fetched {len(emails)} financial emails")

    semaphore = asyncio.Semaphore(5)
    matched_email_hashes = set()

    tasks = [process_file(f, emails, semaphore) for f in all_files]
    results = await asyncio.gather(*tasks)

    # Merge matched email hashes
    for r in results:
        if r:
            matched_email_hashes.update(r)

    # Process unmatched emails
    await process_unmatched_emails(emails, matched_email_hashes)


# ------------------------------
# :: Entry Point
# ------------------------------
if __name__ == "__main__":
    asyncio.run(main())