import json
import asyncio
import hashlib
import logging
from io import BytesIO
from pathlib import Path
from src.matcher import Matcher
from src.pdf_parser import extract_records_from_file
from src.config import DRIVE_FOLDER_ID, OTHER_EMAIL_FOLDER_ID, ATTACH_FILES_ID
from src.drive_uploader import drive_sheet_manager, upload_to_drive, sanitize_filename, is_record_unique_in_sheet, DateTimeEncoder,file_exists_in_drive


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#---------------------------------
# :: Set Track Process variables
#---------------------------------

""" 
Initialises sets to track processed records and email hashes, and a dictionary to cache email attachments.
"""

processed_records = set()
processed_email_hashes = set()
email_attachments_cache = {}

#-----------------------------------
# :: Upload Unique File Function
#-----------------------------------

""" 
This asynchronous function uploads a file to Google Drive only if it doesn't already exist, 
sanitising the filename, handling errors, and logging all actions.
"""

async def upload_unique_file(content: bytes, save_name: str, folder_id: str):
    if not isinstance(content, (bytes, bytearray)) or not content:
        raise ValueError("File content must be non-empty bytes.")
    if not isinstance(save_name, str) or not save_name.strip():
        raise ValueError(f"Invalid save_name: '{save_name}'")
    if not isinstance(folder_id, str) or not folder_id.strip():
        raise ValueError(f"Invalid folder_id: '{folder_id}'")
    try:
        save_name = sanitize_filename(save_name)
        exists = await asyncio.to_thread(lambda: file_exists_in_drive(save_name, folder_id))
        if exists:
            logger.info(f"Skipped Drive duplicate: '{save_name}'")
            return None
        temp_file = BytesIO(content)
        temp_file.name = save_name
        file_id = await asyncio.to_thread(lambda: upload_to_drive(temp_file, folder_id))
        logger.info(f"Uploaded file '{save_name}' to Drive with ID: {file_id}")
        return file_id

    except ValueError as ve:
        logger.error(f"Upload failed due to invalid input ({type(ve).__name__}): {ve}")
        raise ve
    except Exception as e:
        logger.error(f"Unexpected error during Drive upload of '{save_name}' ({type(e).__name__}): {e}")
        return None

#-----------------------------
# :: Process File Function
#-----------------------------

""" 
Asynchronously processes a file by extracting records, matching emails, uploading unique attachments to Drive,
updating a Google Sheet with unique records, 
and tracking processed records and email hashes with full error handling and logging.
"""

async def process_file(file_path: Path, emails: list, semaphore: asyncio.Semaphore):
    if not isinstance(file_path, Path) or not file_path.exists():
        logger.warning(f"Invalid or missing file: {file_path}")
        return set()
    if not isinstance(emails, list):
        logger.warning("Emails must be a list")
        return set()
    async with semaphore:
        try:
            records = await asyncio.to_thread(extract_records_from_file, file_path)
            if not records:
                logger.info(f"No records found in file: {file_path}")
                return set()
            sheet_name = sanitize_filename(f"{file_path.stem}_records")
            matched_email_hashes = set()
            final_records = []
            for record in records:
                try:
                    record_hash = hashlib.md5(
                        json.dumps(record, sort_keys=True, cls=DateTimeEncoder).encode()
                    ).hexdigest()
                    if record_hash in processed_records:
                        continue
                    processed_records.add(record_hash)
                    email, score = Matcher.match_record_email(record, emails)
                    if not email or score < 0.7:
                        continue
                    is_unique = await asyncio.to_thread(
                        is_record_unique_in_sheet, sheet_name, DRIVE_FOLDER_ID, record
                    )
                    if not is_unique:
                        continue
                    email_hash = email["hash"]
                    attach_paths = email_attachments_cache.get(email_hash, [])
                    if not attach_paths:
                        for idx, att in enumerate(email.get("attachments", [])):
                            ext = Path(att["filename"]).suffix or ".bin"
                            save_name = f"{email['sender_email']}_{email_hash[:8]}_{idx}{ext}"
                            file_id = await upload_unique_file(att["content"], save_name, ATTACH_FILES_ID)
                            if file_id:
                                attach_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
                        email_attachments_cache[email_hash] = attach_paths
                    processed_email_hashes.add(email_hash)
                    final_records.append({
                        "sender_name": email.get("sender_name"),
                        "received_time": email.get("date"),
                        "sender_email_address": email.get("sender_email"),
                        "attach_path": ", ".join(attach_paths)
                    })
                    matched_email_hashes.add(email_hash)
                except Exception as inner_e:
                    logger.exception(f"Error processing record in file {file_path}: {inner_e}")
                    continue
            if final_records:
                await asyncio.to_thread(drive_sheet_manager, sheet_name, DRIVE_FOLDER_ID, final_records, True)
                logger.info(f"Saved {len(final_records)} unique records → {sheet_name}")
            return matched_email_hashes
        except Exception as e:
            logger.exception(f"Failed processing file {file_path} ({type(e).__name__}): {e}")
            return set()



#-----------------------------------------
# :: Process unmatch Emails Function
#-----------------------------------------

""" 
Asynchronously processes unmatched emails by uploading their attachments to Drive, updating a Google Sheet with their details, 
and tracking processed emails with full error handling and logging.
"""

async def process_unmatched_emails(emails: list, matched_email_hashes: set):
    sheet_name = "other_email"
    unmatched_records = []
    if not isinstance(emails, list):
        logger.warning("Emails input must be a list")
        return
    try:
        for email in emails:
            try:
                email_hash = email.get("hash")
                if not email_hash:
                    logger.warning(f"Email missing hash, skipping: {email}")
                    continue
                if email_hash in matched_email_hashes or email_hash in processed_email_hashes:
                    continue
                processed_email_hashes.add(email_hash)
                attach_paths = email_attachments_cache.get(email_hash, [])
                if not attach_paths:
                    for idx, att in enumerate(email.get("attachments", [])):
                        content = att.get("content")
                        if not content:
                            continue
                        ext = Path(att.get("filename", "")).suffix or ".bin"
                        save_name = f"{email.get('sender_email','unknown')}_{email_hash[:8]}_{idx}{ext}"
                        file_id = await upload_unique_file(content, save_name, ATTACH_FILES_ID)
                        if file_id:
                            attach_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
                    email_attachments_cache[email_hash] = attach_paths
                unmatched_records.append({
                    "sender_name": email.get("sender_name", ""),
                    "received_time": email.get("date", ""),
                    "sender_email_address": email.get("sender_email", ""),
                    "attach_path": ", ".join(attach_paths)
                })
            except Exception as record_e:
                logger.exception(f"Failed processing unmatched email {email.get('sender_email','unknown')} ({type(record_e).__name__}): {record_e}")
                continue
        if unmatched_records:
            await asyncio.to_thread(drive_sheet_manager, sheet_name, OTHER_EMAIL_FOLDER_ID, unmatched_records, True)
            logger.info(f"Saved {len(unmatched_records)} unmatched emails → {sheet_name}")
    except Exception as e:
        logger.exception(f"Failed processing unmatched emails ({type(e).__name__}): {e}")