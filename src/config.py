import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
STATEMENTS_DIR = Path(os.getenv("STATEMENTS_DIR", "statements"))
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
OTHER_EMAIL_FOLDER_ID = os.getenv("OTHER_EMAIL_FOLDER_ID", DRIVE_FOLDER_ID)
CREDS_FILE = os.getenv("CREDS_FILE", "credentials.json")
TOKEN_PICKLE = os.getenv("TOKEN_PICKLE", "token.pickle")

SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, DRIVE_FOLDER_ID]):
    raise ValueError("Missing required environment variables: EMAIL_ADDRESS, EMAIL_PASSWORD, or DRIVE_FOLDER_ID")

STATEMENTS_DIR.mkdir(parents=True, exist_ok=True)