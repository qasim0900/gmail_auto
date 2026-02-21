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
from src.config import STATEMENTS_DIR, DRIVE_FOLDER_ID, OTHER_EMAIL_FOLDER_ID


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#-----------------------------
# ::  Date Time Encode Class
#-----------------------------

""" 
This JSON encoder converts datetime and pandas.Timestamp objects to ISO strings, logging errors if conversion fails.
"""

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
# ::  Process Track Variable
#-----------------------------

""" 
These lines initialise sets to track processed records, uploaded files, and processed emails to avoid duplicates.
"""

processed_records = set()
uploaded_file_hashes = set()
processed_email_hashes = set()



#----------------------------------
# :: Upload Unique File Function
#----------------------------------

""" 
This async function uploads a file to Google Drive only if its content hasn't been uploaded before, 
tracking duplicates via MD5 hashes and logging the result.
"""

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
        else:
            logger.error(f"Failed to upload file: {save_name}")
        return file_id
    except Exception as e:
        logger.exception(f"Error uploading file {save_name}: {e}")
        return None



#-----------------------------
# :: Process File Function
#-----------------------------

""" 
This function processes a file by extracting records, matching them to emails, uploading
attachments, and updating a Google Sheet, avoiding duplicates.
"""

async def process_file(file_path: Path, emails: list, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            records = await asyncio.to_thread(extract_records_from_file, file_path)
            if not records:
                logger.warning(f"No records found in {file_path}")
                return set()
            matched_records, matched_email_hashes = [], set()
            sheet_name = f"{file_path.stem}_records"
            for record in records:
                record_hash = hashlib.md5(
                    json.dumps(record, sort_keys=True, cls=DateTimeEncoder).encode()
                ).hexdigest()
                if record_hash in processed_records:
                    continue
                processed_records.add(record_hash)
                email, score = Matcher.match_record_email(record, emails)
                if not email or score < 0.7 or email["hash"] in processed_email_hashes:
                    continue
                processed_email_hashes.add(email["hash"])
                matched_email_hashes.add(email["hash"])
                attachment_paths = []
                for idx, att in enumerate(email.get("attachments", [])):
                    ext = Path(att["filename"]).suffix or ".bin"
                    save_name = f"{email['sender_email']}_{email['hash']}_{idx}{ext}"
                    file_id = await upload_unique_file(att["content"], save_name, OTHER_EMAIL_FOLDER_ID)
                    if file_id:
                        attachment_paths.append(f"https://drive.google.com/file/d/{file_id}/view?usp=sharing")
                matched_records.append({
                    "sender_name": email["sender_name"],
                    "sender_email": email["sender_email"],
                    "email_received_time": email["date"],
                    "attach_file_or_image_path": ", ".join(attachment_paths),
                    "email_link": f"https://mail.google.com/mail/u/0/#search/rfc822msgid:{email['hash']}"
                })
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
        except Exception as e:
            logger.exception(f"Failed to process file {file_path}: {e}")
            return set()



#-------------------------------------
# :: Process Unmatch Email Function
#-------------------------------------

""" 
Processes unmatched emails, uploads attachments to Google Drive, and updates the spreadsheet with email details asynchronously
"""

async def process_unmatched_emails(emails: list, matched_email_hashes: set):
    sheet_name = "other_emails"
    unmatched_records = []
    try:
        for email in emails:
            if email['hash'] in matched_email_hashes or email['hash'] in processed_email_hashes:
                continue
            processed_email_hashes.add(email['hash'])
            attachment_tasks = []
            for idx, att in enumerate(email.get("attachments", [])):
                ext = Path(att.get("filename", "")).suffix or ".bin"
                save_name = f"{email.get('sender_email','unknown')}_{email['hash']}_{idx}{ext}"
                attachment_tasks.append(upload_unique_file(att.get("content"), save_name, OTHER_EMAIL_FOLDER_ID))
            attachment_ids = await asyncio.gather(*attachment_tasks, return_exceptions=True)
            attachment_paths = []
            for result in attachment_ids:
                if isinstance(result, Exception):
                    logger.error(f"Attachment upload failed: {result}")
                elif result:
                    attachment_paths.append(f"https://drive.google.com/file/d/{result}/view?usp=sharing")
            unmatched_records.append({
                "sender_name": email.get("sender_name", ""),
                "sender_email": email.get("sender_email", ""),
                "email_received_time": email.get("date", ""),
                "attach_file_or_image_path": ", ".join(attachment_paths),
                "email_link": f"https://mail.google.com/mail/u/0/#search/rfc822msgid:{email['hash']}"
            })
        if unmatched_records:
            await asyncio.to_thread(
                drive_sheet_manager,
                sheet_name,
                OTHER_EMAIL_FOLDER_ID,
                unmatched_records,
                True
            )
        logger.info(f"Successfully processed {len(unmatched_records)} unmatched emails")
    except Exception as e:
        logger.error(f"Error processing unmatched emails: {e}")





#-----------------------------
# :: Main Function
#-----------------------------

""" 
Orchestrates asynchronous processing of statement files and financial emails, handling matched 
and unmatched data efficiently with concurrency control and error logging
"""

async def main():
    try:
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

""" 
Runs the asynchronous main function to process files and emails when the script is executed directly.
"""

if __name__ == "__main__":
    asyncio.run(main())