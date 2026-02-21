import os
import json
import pickle
import logging
import hashlib
from src import config
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)



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
            logger.info(f"File '{file_metadata['name']}' successfully uploaded to Drive with ID: {file_id}")
            return file_id
        else:
            logger.error(f"File '{file_metadata['name']}' upload returned no file ID.")
            return None
    except Exception as e:
        logger.error(f"Error uploading to Drive: {type(e).__name__}: {e}")
        return None




#-----------------------------
# :: Get Credentials Function
#-----------------------------

""" 
This function obtains Google API credentials, loading them from a token file if available, or 
initiating an OAuth flow and saving the token if not.
"""

def get_credentials():
    creds = None
    if config.TOKEN_PICKLE and os.path.exists(config.TOKEN_PICKLE):
        with open(config.TOKEN_PICKLE, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(config.CREDS_FILE, config.SCOPES)
        creds = flow.run_local_server(port=0)
        with open(config.TOKEN_PICKLE, 'wb') as token:
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
        drive_service = build("drive", "v3", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)
        query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
        files = drive_service.files().list(q=query, fields="files(id)").execute().get("files", [])
        if files:
            sheet_id = files[0]["id"]
        else:
            sheet_id = sheets_service.spreadsheets().create(
                body={"properties": {"title": sheet_name}}, 
                fields="spreadsheetId"
            ).execute()["spreadsheetId"]
            drive_service.files().update(fileId=sheet_id, addParents=folder_id, removeParents="root").execute()
        if not records:
            return sheet_id
        existing = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range="A1:Z100000"
        ).execute().get("values", [])
        headers = existing[0] if existing else None
        existing_hashes = {hashlib.md5(json.dumps(dict(zip(headers, r)), sort_keys=True).encode()).hexdigest()
                           for r in existing[1:]} if existing else set()
        unique_records = []
        for r in records:
            h = hashlib.md5(json.dumps(r, sort_keys=True).encode()).hexdigest()
            if h not in existing_hashes:
                unique_records.append(r)
                existing_hashes.add(h)
        if not unique_records:
            logger.info(f"No new unique records to add in '{sheet_name}'")
            return sheet_id
        if headers is None:
            headers = list(unique_records[0].keys())
        values = [[r.get(h, "") for h in headers] for r in unique_records]

        sheets_service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range="A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values}
        ).execute()
        logger.info(f"Added {len(unique_records)} new unique records to '{sheet_name}'")
        return sheet_id
    except Exception as e:
        logger.error(f"Drive Sheet Manager Error ({type(e).__name__}): {e}")
        return None