import json
import asyncio
import hashlib
import logging
from io import BytesIO
from pathlib import Path
from src.matcher import Matcher
from src.pdf_parser import extract_records_from_file
from src.drive_uploader import drive_sheet_manager, upload_to_drive, sanitize_filename, is_record_unique_in_sheet, DateTimeEncoder,file_exists_in_drive
from src.config import DRIVE_FOLDER_ID, OTHER_EMAIL_FOLDER_ID, ATTACH_FILES_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

processed_records = set()
processed_email_hashes = set()
email_attachments_cache = {}

# ---------------------------
# Upload attachment only once
# ---------------------------
async def upload_unique_file(content: bytes, save_name: str, folder_id: str):
    save_name = sanitize_filename(save_name)

    # Check if already exists in Drive
    exists = await asyncio.to_thread(lambda: file_exists_in_drive(save_name, folder_id))
    if exists:
        logger.info(f"Skipped Drive duplicate: {save_name}")
        return None

    temp_file = BytesIO(content)
    temp_file.name = save_name
    return upload_to_drive(temp_file, folder_id)

# ---------------------------
# Process matched file
# ---------------------------
async def process_file(file_path: Path, emails: list, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            records = await asyncio.to_thread(extract_records_from_file, file_path)
            if not records:
                return set()

            sheet_name = sanitize_filename(f"{file_path.stem}_records")
            matched_email_hashes = set()
            final_records = []

            for record in records:
                record_hash = hashlib.md5(json.dumps(record, sort_keys=True, cls=DateTimeEncoder).encode()).hexdigest()
                if record_hash in processed_records:
                    continue
                processed_records.add(record_hash)

                email, score = Matcher.match_record_email(record, emails)
                if not email or score < 0.7:
                    continue

                is_unique = await asyncio.to_thread(is_record_unique_in_sheet, sheet_name, DRIVE_FOLDER_ID, record)
                if not is_unique:
                    continue

                email_hash = email["hash"]
                if email_hash in email_attachments_cache:
                    attach_paths = email_attachments_cache[email_hash]
                else:
                    attach_paths = []
                    for idx, att in enumerate(email.get("attachments", [])):
                        ext = Path(att["filename"]).suffix or ".bin"
                        save_name = f"{email['sender_email']}_{email_hash[:8]}_{idx}{ext}"
                        file_id = await upload_unique_file(att["content"], save_name, ATTACH_FILES_ID)
                        if file_id:
                            attach_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
                    email_attachments_cache[email_hash] = attach_paths
                processed_email_hashes.add(email_hash)

                final_records.append({
                    "sender_name": email["sender_name"],
                    "received_time": email["date"],
                    "sender_email_address": email["sender_email"],
                    "attach_path": ", ".join(attach_paths)
                })
                matched_email_hashes.add(email_hash)

            if final_records:
                await asyncio.to_thread(drive_sheet_manager, sheet_name, DRIVE_FOLDER_ID, final_records, True)
                logger.info(f"Saved {len(final_records)} unique records → {sheet_name}")

            return matched_email_hashes

        except Exception as e:
            logger.exception(f"Failed processing file {file_path}: {e}")
            return set()

# ---------------------------
# Process unmatched emails
# ---------------------------
async def process_unmatched_emails(emails: list, matched_email_hashes: set):
    sheet_name = "other_email"
    unmatched_records = []

    for email in emails:
        email_hash = email["hash"]
        if email_hash in matched_email_hashes or email_hash in processed_email_hashes:
            continue

        processed_email_hashes.add(email_hash)
        if email_hash in email_attachments_cache:
            attach_paths = email_attachments_cache[email_hash]
        else:
            attach_paths = []
            for idx, att in enumerate(email.get("attachments", [])):
                ext = Path(att.get("filename","")).suffix or ".bin"
                save_name = f"{email.get('sender_email','unknown')}_{email_hash[:8]}_{idx}{ext}"
                file_id = await upload_unique_file(att.get("content"), save_name, ATTACH_FILES_ID)
                if file_id:
                    attach_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
            email_attachments_cache[email_hash] = attach_paths

        unmatched_records.append({
            "sender_name": email.get("sender_name",""),
            "received_time": email.get("date",""),
            "sender_email_address": email.get("sender_email",""),
            "attach_path": ", ".join(attach_paths)
        })

    if unmatched_records:
        await asyncio.to_thread(drive_sheet_manager, sheet_name, OTHER_EMAIL_FOLDER_ID, unmatched_records, True)
        logger.info(f"Saved {len(unmatched_records)} unmatched emails → {sheet_name}")