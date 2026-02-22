import json
import asyncio
import hashlib
import logging
import pandas as pd
from io import BytesIO
from pathlib import Path
from datetime import datetime
from src.matcher import Matcher
from src.email_client import fetch_financial_emails
from src.pdf_parser import extract_records_from_file
from src.drive_uploader import drive_sheet_manager, upload_to_drive
from src.config import (
    STATEMENTS_DIR,
    DRIVE_FOLDER_ID,
    OTHER_EMAIL_FOLDER_ID,
    ATTACH_FILES_ID
)

#-----------------------------
# :: Logger Variable
#-----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#-----------------------------
# :: Date Time Encode Class
#-----------------------------
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            return super().default(obj)
        except Exception as e:
            logger.error(f"DateTimeEncoder failed for object {obj}: {e}")
            return str(obj)

#-----------------------------
# :: Process Track Variables
#-----------------------------
processed_records = set()
uploaded_file_hashes = set()
processed_email_hashes = set()
email_attachments_cache = {}

#----------------------------------
# :: Upload Unique File Function
#----------------------------------
async def upload_unique_file(content: bytes, save_name: str, folder_id: str):
    try:
        file_hash = hashlib.md5(content).hexdigest()
        if file_hash in uploaded_file_hashes:
            logger.info(f"Skipped upload: {save_name} already uploaded.")
            return None
        uploaded_file_hashes.add(file_hash)
        temp_file = BytesIO(content)
        temp_file.name = save_name
        file_id = upload_to_drive(temp_file, folder_id)
        if file_id:
            logger.info(f"Uploaded file successfully: {save_name} with ID {file_id}")
            return file_id
        else:
            logger.error(f"Failed to upload file: {save_name}")
            return None
    except Exception as e:
        logger.exception(f"Error uploading file {save_name}: {e}")
        return None

#-----------------------------
# :: Process File Function (Matched)
#-----------------------------
async def process_file(file_path: Path, emails: list, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            records = await asyncio.to_thread(extract_records_from_file, file_path)
            if not records:
                logger.warning(f"No records found in {file_path}")
                return set()

            matched_records, matched_email_hashes = [], set()
            sheet_name = f"{file_path.stem}*records"

            for record in records:
                record_hash = hashlib.md5(
                    json.dumps(record, sort_keys=True, cls=DateTimeEncoder).encode()
                ).hexdigest()
                if record_hash in processed_records:
                    continue
                processed_records.add(record_hash)

                email, score = Matcher.match_record_email(record, emails)
                if not email or score < 0.7:
                    continue

                email_hash = email["hash"]
                if email_hash in processed_email_hashes:
                    attach_paths = email_attachments_cache.get(email_hash, [])
                else:
                    processed_email_hashes.add(email_hash)
                    attach_paths = []
                    for idx, att in enumerate(email.get("attachments", [])):
                        ext = Path(att["filename"]).suffix or ".bin"
                        save_name = f"{email['sender_email']}*{email_hash[:8]}_{idx}{ext}"
                        file_id = await upload_unique_file(att["content"], save_name, ATTACH_FILES_ID)
                        if file_id:
                            view_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
                            attach_paths.append(view_url)
                    email_attachments_cache[email_hash] = attach_paths

                attach_str = ", ".join(attach_paths) if attach_paths else ""
                matched_records.append({
                    "sender_name": email["sender_name"],
                    "received_time": email["date"],
                    "sender_email_address": email["sender_email"],
                    "attach_file or image path": attach_str
                })
                matched_email_hashes.add(email_hash)

            if matched_records:
                await asyncio.to_thread(
                    drive_sheet_manager,
                    sheet_name,
                    DRIVE_FOLDER_ID,
                    matched_records,
                    True
                )
                logger.info(f"Updated matched sheet: {sheet_name} with {len(matched_records)} records")

            return matched_email_hashes

        except Exception as e:
            logger.exception(f"Failed to process file {file_path}: {e}")
            return set()

#-------------------------------------
# :: Process Unmatched Emails
#-------------------------------------
async def process_unmatched_emails(emails: list, matched_email_hashes: set):
    sheet_name = "other_email"
    unmatched_records = []
    try:
        for email in emails:
            email_hash = email['hash']
            if email_hash in matched_email_hashes or email_hash in processed_email_hashes:
                continue
            processed_email_hashes.add(email_hash)
            attach_paths = []
            for idx, att in enumerate(email.get("attachments", [])):
                ext = Path(att.get("filename", "")).suffix or ".bin"
                save_name = f"{email.get('sender_email','unknown')}*{email_hash[:8]}*{idx}{ext}"
                file_id = await upload_unique_file(att.get("content"), save_name, ATTACH_FILES_ID)
                if file_id:
                    attach_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
            email_attachments_cache[email_hash] = attach_paths
            unmatched_records.append({
                "sender_name": email.get("sender_name", ""),
                "received_time": email.get("date", ""),
                "sender_email_address": email.get("sender_email", ""),
                "attach_file or image path": ", ".join(attach_paths) if attach_paths else ""
            })

        if unmatched_records:
            await asyncio.to_thread(
                drive_sheet_manager,
                sheet_name,
                OTHER_EMAIL_FOLDER_ID,
                unmatched_records,
                True
            )
            logger.info(f"Updated unmatched sheet: {sheet_name} with {len(unmatched_records)} records")

    except Exception as e:
        logger.error(f"Error processing unmatched emails: {e}")

#-----------------------------
# :: Main Function
#-----------------------------
async def main():
    try:
        model = Matcher.get_model()
        logger.info("AI model loaded successfully at startup")

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

        async def process_file_safe(file):
            try:
                return await process_file(file, emails, semaphore)
            except Exception as e:
                logger.error(f"Error processing file {file.name}: {e}")
                return set()

        results = await asyncio.gather(
            *(process_file_safe(f) for f in all_files),
            return_exceptions=False
        )

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