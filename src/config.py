import os
import sys
from pathlib import Path
from dotenv import load_dotenv

#-----------------------------
# :: Load ENV Loader
#-----------------------------

""" 
Load a ,env file variables
"""
load_dotenv()


#-----------------------------
# :: Load ENV Varibales
#-----------------------------

""" 
This code loads email, Google Drive, and credentials 
configurations from environment variables, providing default values if they are not set.
"""

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
STATEMENTS_DIR = Path(os.getenv("STATEMENTS_DIR", "statements"))
OTHER_EMAIL_FOLDER_ID = os.getenv("OTHER_EMAIL_FOLDER_ID", DRIVE_FOLDER_ID)
CREDS_FILE = os.getenv("CREDS_FILE", "credentials.json")
TOKEN_PICKLE = os.getenv("TOKEN_PICKLE", "token.pickle")
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]


#-----------------------------
# :: Env Variable Validation
#-----------------------------

""" 
This code ensures required environment variables are set and creates the statements directory, exiting on failure.
"""

missing_vars = [var_name for var_name, var_value in {
    "EMAIL_ADDRESS": EMAIL_ADDRESS,
    "EMAIL_PASSWORD": EMAIL_PASSWORD,
    "DRIVE_FOLDER_ID": DRIVE_FOLDER_ID
}.items() if not var_value]
if missing_vars:
    sys.exit(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
try:
    STATEMENTS_DIR.mkdir(parents=True, exist_ok=True)
except Exception as e:
    sys.exit(f"ERROR: Failed to create statements directory '{STATEMENTS_DIR}': {e}")