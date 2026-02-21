import json
import asyncio
import hashlib
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd

from src.config import STATEMENTS_DIR, DRIVE_FOLDER_ID, OTHER_EMAIL_FOLDER_ID
from src.matcher import Matcher
from src.pdf_parser import extract_records_from_file
from src.drive_uploader import drive_sheet_manager, upload_to_drive
from src.email_client import fetch_financial_emails

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# JSON Encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

# Track processed record hashes and uploaded file hashes
processed_records = set()
uploaded_file_hashes = set()
processed_email_hashes = set()


async def upload_unique_file(content: bytes, save_name: str, folder_id: str):
    file_hash = hashlib.md5(content).hexdigest()
    if file_hash in uploaded_file_hashes:
        return None  # Already uploaded
    uploaded_file_hashes.add(file_hash)

    temp_file = BytesIO(content)
    temp_file.name = save_name
    return upload_to_drive(temp_file, folder_id)


async def process_file(file_path: Path, emails: list, semaphore: asyncio.Semaphore):
    async with semaphore:
        records = await asyncio.to_thread(extract_records_from_file, file_path)
        if not records:
            logger.warning(f"No records found in {file_path}")
            return set()

        matched_records = []
        matched_email_hashes = set()
        sheet_name = f"{file_path.stem}_records"

        for idx, record in enumerate(records):
            # Record hash to prevent duplicates
            record_hash = hashlib.md5(json.dumps(record, sort_keys=True, cls=DateTimeEncoder).encode()).hexdigest()
            if record_hash in processed_records:
                continue
            processed_records.add(record_hash)

            email, score = Matcher.match_record_email(record, emails)
            if not email or score < 0.7:
                continue

            if email["hash"] in processed_email_hashes:
                continue  # Already processed this email
            processed_email_hashes.add(email["hash"])
            matched_email_hashes.add(email["hash"])

            attachment_paths = []
            for att_idx, att in enumerate(email["attachments"]):
                ext = Path(att["filename"]).suffix or ".bin"
                save_name = f"{email['sender_email']}_{email['hash']}_{att_idx}{ext}"
                file_id = await upload_unique_file(att["content"], save_name, OTHER_EMAIL_FOLDER_ID)
                if file_id:
                    attachment_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")

            rec_data = {
                "sender_name": email["sender_name"],
                "sender_email": email["sender_email"],
                "email_received_time": email["date"],
                "attach_file_or_image_path": ", ".join(attachment_paths),
                "email_link": f"https://mail.google.com/mail/u/0/#search/rfc822msgid:{email['hash']}"
            }
            matched_records.append(rec_data)

        if matched_records:
            await asyncio.to_thread(
                drive_sheet_manager,
                sheet_name,
                DRIVE_FOLDER_ID,
                matched_records,
                True
            )
            logger.info(f"Updated sheet: {sheet_name} with {len(matched_records)} records")

        return matched_email_hashes


async def process_unmatched_emails(emails: list, matched_email_hashes: set):
    sheet_name = "other_emails"
    unmatched_records = []

    for email in emails:
        if email['hash'] in matched_email_hashes or email['hash'] in processed_email_hashes:
            continue  # Already processed

        processed_email_hashes.add(email['hash'])
        attachment_paths = []

        for idx, att in enumerate(email['attachments']):
            ext = Path(att["filename"]).suffix or ".bin"
            save_name = f"{email['sender_email']}_{email['hash']}_{idx}{ext}"
            file_id = await upload_unique_file(att["content"], save_name, OTHER_EMAIL_FOLDER_ID)
            if file_id:
                attachment_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")

        rec_data = {
            "sender_name": email["sender_name"],
            "sender_email": email["sender_email"],
            "email_received_time": email["date"],
            "attach_file_or_image_path": ", ".join(attachment_paths),
            "email_link": f"https://mail.google.com/mail/u/0/#search/rfc822msgid:{email['hash']}"
        }
        unmatched_records.append(rec_data)

    if unmatched_records:
        await asyncio.to_thread(
            drive_sheet_manager,
            sheet_name,
            OTHER_EMAIL_FOLDER_ID,
            unmatched_records,
            True
        )
        logger.info(f"Processed {len(unmatched_records)} unmatched emails")


async def main():
    if not STATEMENTS_DIR.exists() or not STATEMENTS_DIR.is_dir():
        raise FileNotFoundError("STATEMENTS_DIR folder is not found")

    all_files = list(STATEMENTS_DIR.glob("*.*"))
    if not all_files:
        logger.info("No files found in statements folder")
        return

    logger.info(f"Found {len(all_files)} files in statements folder")

    emails = await fetch_financial_emails(limit=500)
    logger.info(f"Fetched {len(emails)} financial emails")

    semaphore = asyncio.Semaphore(5)
    matched_email_hashes = set()

    tasks = [process_file(f, emails, semaphore) for f in all_files]
    results = await asyncio.gather(*tasks)

    for r in results:
        if r:
            matched_email_hashes.update(r)

    await process_unmatched_emails(emails, matched_email_hashes)


if __name__ == "__main__":
    asyncio.run(main())