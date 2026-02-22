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
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload,MediaIoBaseDownload

#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)



class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            return super().default(obj)
        except Exception as e:
            logger.error(f"DateTimeEncoder failed for object {obj}: {e}")
            return str(obj)


# -----------------------------
# :: Helper Functions
# -----------------------------
def sanitize_filename(name: str):
    """Replace invalid Windows filename characters"""
    invalid_chars = '<>:"/\\|?*'
    for ch in invalid_chars:
        name = name.replace(ch, "_")
    return name

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



def file_exists_in_drive(filename, folder_id):
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        res = service.files().list(q=query, fields="files(id)").execute()
        return len(res.get("files", [])) > 0
    except Exception as e:
        logger.error(f"Drive file check failed: {e}")
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
# :: Driver Sheet Manager Function
#--------------------------------------

""" 
This function manages a Google Sheet in Drive: it creates the sheet if missing, checks for 
duplicate records, and appends only unique records, logging all actions and errors.
"""
def drive_sheet_manager(sheet_name, folder_id, records=None, append=True):
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        sheet_name = sanitize_filename(sheet_name)
        if not sheet_name.lower().endswith(".xlsx"):
            sheet_name += ".xlsx"

        # Search file
        query = f"name='{sheet_name}' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and '{folder_id}' in parents and trashed=false"
        files = service.files().list(q=query, fields="files(id,name)").execute().get("files", [])
        file_id = files[0]["id"] if files else None

        # Ensure file exists
        if not records:
            if not file_id:
                buffer = BytesIO()
                pd.DataFrame().to_excel(buffer, index=False, engine="openpyxl")
                buffer.seek(0)
                media = MediaIoBaseUpload(buffer, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=False)
                file_id = service.files().create(body={"name": sheet_name, "parents": [folder_id]}, media_body=media, fields="id").execute().get("id")
            return file_id

        # Download existing data
        df_existing = pd.DataFrame()
        if file_id:
            request = service.files().get_media(fileId=file_id)
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            try:
                df_existing = pd.read_excel(fh, engine="openpyxl")
            except Exception:
                df_existing = pd.DataFrame()

        # Deduplicate
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

        # Merge data
        df_new = pd.DataFrame(unique_records)
        if df_existing.empty:
            df_final = df_new
        else:
            all_cols = list(set(df_existing.columns) | set(df_new.columns))
            df_existing = df_existing.reindex(columns=all_cols).fillna("")
            df_new = df_new.reindex(columns=all_cols).fillna("")
            df_final = pd.concat([df_existing, df_new], ignore_index=True)

        # Upload from memory
        buffer = BytesIO()
        df_final.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        media = MediaIoBaseUpload(buffer, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=False)
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_id = service.files().create(body={"name": sheet_name, "parents": [folder_id]}, media_body=media, fields="id").execute().get("id")

        logger.info(f"Added {len(unique_records)} new records to '{sheet_name}'")
        return file_id

    except Exception as e:
        logger.error(f"Drive Excel Manager Error ({type(e).__name__}): {e}")
        return None

    
    
    
def is_record_unique_in_sheet(sheet_name, folder_id, record: dict):
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        sheet_name = sanitize_filename(sheet_name)
        if not sheet_name.endswith(".xlsx"):
            sheet_name += ".xlsx"
        query = f"name='{sheet_name}' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and '{folder_id}' in parents and trashed=false"
        files = service.files().list(q=query, fields="files(id)").execute().get("files", [])
        if not files:
            return True
        file_id = files[0]["id"]
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_excel(fh, engine="openpyxl")
        if df.empty:
            return True
        cols = [c for c in df.columns if c != "attach_path"]
        existing_hashes = set()
        for _, row in df[cols].fillna("").iterrows():
            h = hashlib.md5(json.dumps(row.to_dict(), sort_keys=True).encode()).hexdigest()
            existing_hashes.add(h)
        record_hash = hashlib.md5(json.dumps(record, sort_keys=True, cls=DateTimeEncoder).encode()).hexdigest()
        return record_hash not in existing_hashes
    except Exception as e:
        logger.error(f"Sheet uniqueness check failed: {e}")
        return True