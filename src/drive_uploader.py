import os
import json
import pickle
import logging
import hashlib
import pandas as pd
from io import BytesIO
from src import config
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload,MediaIoBaseDownload

#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)




#------------------------------------
# :: Date Time Encoder Function
#------------------------------------

""" 
This DateTimeEncoder class is a custom JSON encoder that converts datetime and pandas.
Timestamp objects to ISO 8601 strings, logging any encoding errors.
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



#-----------------------------------
# :: Senitize File Name Function
#-----------------------------------

""" 
This sanitize_filename function safely cleans a string for use as a filename by replacing invalid characters with underscores, 
trimming whitespace, and raising descriptive errors for invalid input.
"""


def sanitize_filename(name: str):
    invalid_chars = '<>:"/\\|?*'
    try:
        if not isinstance(name, str):
            raise TypeError(f"Expected a string for filename, got {type(name).__name__}")
        sanitized = name
        for ch in invalid_chars:
            sanitized = sanitized.replace(ch, "_")
        sanitized = sanitized.strip()
        if not sanitized:
            raise ValueError("Filename cannot be empty after sanitization.")
        return sanitized
    except Exception as e:
        raise RuntimeError(f"Failed to sanitize filename '{name}': {e}") from e



#-----------------------------
# :: Upload Drive Function
#-----------------------------

""" 
This function uploads a file to Google Drive, logs success with the file ID, and handles errors gracefully.
"""

def upload_to_drive(file_path, folder_id):
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        if hasattr(file_path, 'read'):
            file_name = getattr(file_path, 'name', 'unnamed_file')
            file_metadata = {"name": file_name, "parents": [folder_id]}
            file_path.seek(0)
            media = MediaIoBaseUpload(file_path, mimetype='application/octet-stream', resumable=False)
        else:
            file_metadata = {"name": os.path.basename(file_path), "parents": [folder_id]}
            media = MediaFileUpload(file_path, resumable=False)
        file_id = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute().get("id")
        if file_id:
            logger.info(f"Uploaded '{file_metadata['name']}' → Drive ID: {file_id}")
            return file_id
        return None
    except Exception as e:
        logger.error(f"Drive upload error ({type(e).__name__}): {e}")
        return None




#--------------------------------------
# :: File Exists in Drive Function
#--------------------------------------

""" 
This file_exists_in_drive function checks if a specific file exists in a Google Drive folder by querying 
the Drive API, handling errors, and logging the results.
"""

def file_exists_in_drive(filename, folder_id):
    if not isinstance(filename, str) or not filename.strip():
        raise ValueError(f"Invalid filename: '{filename}'")
    if not isinstance(folder_id, str) or not folder_id.strip():
        raise ValueError(f"Invalid folder_id: '{folder_id}'")
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        res = service.files().list(
            q=query,
            fields="files(id)",
            pageSize=1
        ).execute()
        exists = bool(res.get("files"))
        logger.debug(f"File '{filename}' existence in folder '{folder_id}': {exists}")
        return exists
    except HttpError as e:
        logger.error(f"Google Drive API error while checking file '{filename}' in folder '{folder_id}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while checking file '{filename}' in folder '{folder_id}': {e}")
        return False


#-----------------------------
# :: Get Credentials Function
#-----------------------------

""" 
This function obtains Google API credentials, loading them from a token file if available, or 
initiating an OAuth flow and saving the token if not.
"""

def get_credentials():
    creds = None
    if hasattr(config, "TOKEN_PICKLE") and config.TOKEN_PICKLE and os.path.exists(config.TOKEN_PICKLE):
        with open(config.TOKEN_PICKLE, "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(config.CREDS_FILE, config.SCOPES)
        creds = flow.run_local_server(port=0)
        with open(config.TOKEN_PICKLE, "wb") as token:
            pickle.dump(creds, token)
    return creds


#--------------------------------------
# :: Driver Excel Manager Function
#--------------------------------------

""" 
This function manages a Excel Sheet in Drive: it creates the sheet if missing, checks for 
duplicate records, and appends only unique records, logging all actions and errors.
"""


def drive_sheet_manager(sheet_name, folder_id, records=None, append=True):
    if not isinstance(sheet_name, str) or not sheet_name.strip():
        raise ValueError(f"Invalid sheet_name: '{sheet_name}'")
    if not isinstance(folder_id, str) or not folder_id.strip():
        raise ValueError(f"Invalid folder_id: '{folder_id}'")
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        sheet_name = sanitize_filename(sheet_name)
        if not sheet_name.lower().endswith(".xlsx"):
            sheet_name += ".xlsx"
        query = (
            f"name='{sheet_name}' and "
            f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
            f"'{folder_id}' in parents and trashed=false"
        )
        files = service.files().list(q=query, fields="files(id,name)", pageSize=1).execute().get("files", [])
        file_id = files[0]["id"] if files else None
        if not records:
            if not file_id:
                buffer = BytesIO()
                pd.DataFrame().to_excel(buffer, index=False, engine="openpyxl")
                buffer.seek(0)
                media = MediaIoBaseUpload(
                    buffer,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    resumable=False
                )
                file_id = service.files().create(
                    body={"name": sheet_name, "parents": [folder_id]},
                    media_body=media,
                    fields="id"
                ).execute().get("id")
            return file_id
        df_existing = pd.DataFrame()
        if file_id:
            fh = BytesIO()
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            try:
                df_existing = pd.read_excel(fh, engine="openpyxl")
            except Exception:
                df_existing = pd.DataFrame()
        existing_hashes = set()
        if not df_existing.empty:
            cols = [c for c in df_existing.columns if c != "attach_path"]
            for _, row in df_existing[cols].fillna("").iterrows():
                h = hashlib.md5(json.dumps(row.to_dict(), sort_keys=True).encode()).hexdigest()
                existing_hashes.add(h)
        unique_records = []
        for record in records:
            record_for_hash = {k: v for k, v in record.items() if k != "attach_path"}
            h = hashlib.md5(json.dumps(record_for_hash, sort_keys=True, cls=DateTimeEncoder).encode()).hexdigest()
            if h not in existing_hashes:
                unique_records.append(record)
                existing_hashes.add(h)
        if not unique_records:
            logger.info(f"No new unique records for '{sheet_name}'")
            return file_id
        df_new = pd.DataFrame(unique_records)
        if df_existing.empty:
            df_final = df_new
        else:
            all_cols = list(set(df_existing.columns) | set(df_new.columns))
            df_existing = df_existing.reindex(columns=all_cols).fillna("")
            df_new = df_new.reindex(columns=all_cols).fillna("")
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        buffer = BytesIO()
        df_final.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        media = MediaIoBaseUpload(
            buffer,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=False
        )
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_id = service.files().create(
                body={"name": sheet_name, "parents": [folder_id]},
                media_body=media,
                fields="id"
            ).execute().get("id")
        logger.info(f"Added {len(unique_records)} new records to '{sheet_name}'")
        return file_id
    except HttpError as e:
        logger.error(f"Google Drive API error for '{sheet_name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Drive Excel Manager Error ({type(e).__name__}) for '{sheet_name}': {e}")
        return None

    
    

#-----------------------------------------
# :: Is Record Unique Sheet Function
#-----------------------------------------

""" 
This function checks if a record is unique in a Google Drive Excel sheet by hashing existing 
rows and comparing them to the input, handling errors and logging all steps.
"""

def is_record_unique_in_sheet(sheet_name, folder_id, record: dict):
    if not isinstance(sheet_name, str) or not sheet_name.strip():
        raise ValueError(f"Invalid sheet_name: '{sheet_name}'")
    if not isinstance(folder_id, str) or not folder_id.strip():
        raise ValueError(f"Invalid folder_id: '{folder_id}'")
    if not isinstance(record, dict) or not record:
        raise ValueError(f"Invalid record: '{record}'")
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        sheet_name = sanitize_filename(sheet_name)
        if not sheet_name.lower().endswith(".xlsx"):
            sheet_name += ".xlsx"
        query = (
            f"name='{sheet_name}' and "
            f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
            f"'{folder_id}' in parents and trashed=false"
        )
        files = service.files().list(q=query, fields="files(id)", pageSize=1).execute().get("files", [])
        if not files:
            return True

        file_id = files[0]["id"]
        fh = BytesIO()
        request = service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        try:
            df = pd.read_excel(fh, engine="openpyxl")
        except Exception:
            df = pd.DataFrame()

        if df.empty:
            return True
        cols = [c for c in df.columns if c != "attach_path"]
        existing_hashes = {
            hashlib.md5(json.dumps(row.to_dict(), sort_keys=True).encode()).hexdigest()
            for _, row in df[cols].fillna("").iterrows()
        }
        record_filtered = {k: v for k, v in record.items() if k != "attach_path"}
        record_hash = hashlib.md5(json.dumps(record_filtered, sort_keys=True, cls=DateTimeEncoder).encode()).hexdigest()
        return record_hash not in existing_hashes
    except HttpError as e:
        logger.error(f"Google Drive API error during uniqueness check for '{sheet_name}': {e}")
        return True
    except Exception as e:
        logger.error(f"Sheet uniqueness check failed ({type(e).__name__}) for '{sheet_name}': {e}")
        return True