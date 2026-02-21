import os
import json
import pickle
import logging
import hashlib
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload
from src import config

logger = logging.getLogger(__name__)


def upload_to_drive(file_path, folder_id):
    try:
        creds = get_credentials()
        service = build("drive", "v3", credentials=creds)
        
        file_metadata = {
            "name": os.path.basename(file_path),
            "parents": [folder_id]
        }
        
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        
        file_id = file.get("id")
        logger.info(f"Uploaded {file_path} to Drive with ID: {file_id}")
        return file_id
    except Exception as e:
        logger.error(f"Failed to upload {file_path}: {e}")
        return None




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


def drive_sheet_manager(sheet_name, folder_id, records=None, append=True):
    try:
        creds = get_credentials()
        drive_service = build("drive", "v3", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)

        # 1️⃣ Check if sheet exists
        query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id,name)").execute()
        files = results.get("files", [])

        if files:
            sheet_id = files[0]["id"]
        else:
            spreadsheet = {"properties": {"title": sheet_name}}
            sheet = sheets_service.spreadsheets().create(body=spreadsheet, fields="spreadsheetId").execute()
            sheet_id = sheet["spreadsheetId"]
            drive_service.files().update(fileId=sheet_id, addParents=folder_id, removeParents="root").execute()

        # 2️⃣ Agar records provide hue hain
        if not records:
            return sheet_id

        # 3️⃣ Fetch existing data to prevent duplicates
        existing_data = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="A1:Z100000"
        ).execute().get("values", [])

        existing_hashes = set()
        headers = None
        if existing_data:
            headers = existing_data[0]
            for row in existing_data[1:]:
                row_dict = dict(zip(headers, row))
                row_hash = hashlib.md5(json.dumps(row_dict, sort_keys=True).encode()).hexdigest()
                existing_hashes.add(row_hash)

        # 4️⃣ Filter unique new records
        unique_records = []
        for r in records:
            record_hash = hashlib.md5(json.dumps(r, sort_keys=True).encode()).hexdigest()
            if record_hash not in existing_hashes:
                unique_records.append(r)
                existing_hashes.add(record_hash)

        if not unique_records:
            logger.info(f"No new unique records to add in '{sheet_name}'")
            return sheet_id

        # 5️⃣ Prepare data to append
        if headers is None:
            headers = list(unique_records[0].keys())
        values = [[r.get(h, "") for h in headers] for r in unique_records]
        body = {"values": values}

        sheets_service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range="A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        logger.info(f"Added {len(unique_records)} new unique records to '{sheet_name}'")
        return sheet_id

    except Exception as e:
        logger.error(f"Drive Sheet Manager Error: {e}")
        return None