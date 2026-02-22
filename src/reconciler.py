import logging
import fitz
import pandas as pd
from src import config
from pathlib import Path
from datetime import datetime
from src.matcher import Matcher
from src.models import Transaction, Receipt
from src.email_client import fetch_financial_emails
from src.pdf_parser import extract_records_from_file 
from src.drive_uploader import drive_sheet_manager, upload_to_drive


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# --------------------------------------
# :: Smart Column Detector Function
# --------------------------------------

""" 
This function automatically detects and returns the date, description, 
and amount column names from a DataFrame based on common keyword patterns.
"""
def detect_columns(df):
    date_col = next((c for c in df.columns if "date" in c.lower()), None)
    desc_col = next((c for c in df.columns if "desc" in c.lower() or "merchant" in c.lower()), None)
    amt_col  = next((c for c in df.columns if "amount" in c.lower() or "amt" in c.lower() or "debit" in c.lower() or "credit" in c.lower()), None)
    return date_col, desc_col, amt_col


# ------------------------------------
# :: Smart Sheet Detector Function
# ------------------------------------

""" 
This function finds and returns the Excel sheet name containing the word “transaction” 
(or the first sheet if none match), and returns 0 if an error occurs.
"""
def detect_sheet(path):
    try:
        xls = pd.ExcelFile(path)
        for s in xls.sheet_names:
            if "transaction" in s.lower():
                return s
        return xls.sheet_names[0]
    except:
        return 0

#---------------------------------------
# :: Run Reconcilication Function
#---------------------------------------

""" 
This function automates fetching emails, extracting receipts, 
matching them to card transactions using AI, and saving results to Drive and Excel.
"""

async def run_full_reconciliation(limit=500):
    try:
        logger.info("=" * 80)
        logger.info("AI DYNAMIC CREDIT CARD RECONCILIATION STARTED")
        logger.info("=" * 80)
        emails_metadata = await fetch_financial_emails(limit=limit)
        logger.info(f"Fetched emails: {len(emails_metadata)}")

        statements_dir = Path(config.STATEMENTS_DIR)
        statements_dir.mkdir(exist_ok=True)
        for email_data in emails_metadata:
            category = email_data.get("Category", "Other_Bills")
            record = {
                "Sender Name": email_data.get("sender_name", ""),
                "Sender Email": email_data.get("sender_email", ""),
                "Email Subject": email_data.get("subject", ""),
                "Email Date": email_data.get("date", ""),
                "Category": category
            }

            links = []
            for att in email_data.get("attachments", []):
                temp_path = statements_dir / att["filename"]
                temp_path.write_bytes(att["content"])
                links.append(upload_to_drive(str(temp_path), config.DRIVE_FOLDER_ID))

            record["Attachment Link"] = ", ".join(links)
            drive_sheet_manager(category, config.DRIVE_FOLDER_ID, records=[record])
        receipts = []
        for file_path in statements_dir.glob("*.*"):
            if file_path.suffix.lower() in [".pdf", ".jpg", ".jpeg", ".png", ".xlsx", ".xls", ".csv", ".json", ".txt"]:
                for r in extract_records_from_file(file_path):
                    receipt = Receipt(
                        filename=file_path.name,
                        date=r.get("date", datetime.now().strftime("%Y-%m-%d")),
                        merchant=r.get("merchant", "Unknown"),
                        amount=float(r.get("amount", 0.0)),
                        email_id=""
                    )
                    receipt.original_path = file_path
                    receipts.append(receipt)

        logger.info(f"Total receipts parsed: {len(receipts)}")
        cards = []
        for file in statements_dir.glob("*.*"):
            if file.suffix.lower() in [".pdf", ".xlsx", ".xls", ".csv"]:
                card_name = file.stem.replace("_", " ").strip()
                cards.append((card_name, file))

        logger.info(f"Detected cards: {[c[0] for c in cards]}")
        for card_name, path in cards:
            logger.info(f"Processing card file: {card_name}")

            if not path.exists():
                continue
            transactions = []
            if path.suffix.lower() in [".xlsx", ".xls"]:
                sheet_name = detect_sheet(path)
                df = pd.read_excel(path, sheet_name=sheet_name)

                date_col, desc_col, amt_col = detect_columns(df)

                for _, row in df.iterrows():
                    if pd.isna(row.get(date_col)) or pd.isna(row.get(amt_col)):
                        continue

                    transactions.append(
                        Transaction(
                            str(row[date_col]),
                            str(row.get(desc_col, "Unknown Merchant")),
                            abs(float(row[amt_col]))
                        )
                    )
            else:
                with fitz.open(path) as doc:
                    text = "\n".join(page.get_text() for page in doc)

                import re
                for line in text.splitlines():
                    line = line.strip()
                    if re.match(r"\d{4}-\d{2}-\d{2}", line):
                        date = line.split()[0]
                        amt_match = re.search(r"(-?\d+\.\d{2})", line)
                        if amt_match:
                            transactions.append(
                                Transaction(date, "Unknown Merchant", abs(float(amt_match.group(1))))
                            )
            for receipt in [r for r in receipts if r.matched_card is None]:
                best_email, score = Matcher.match_record_email(vars(receipt), emails_metadata)
                if score > 0.7:
                    receipt.matched_transaction = Transaction(receipt.date, best_email["subject"], receipt.amount)
                    receipt.matched_card = card_name
            data = []
            counter = 1
            for r in receipts:
                if getattr(r, "matched_transaction", None) and r.matched_card == card_name:
                    if not r.label:
                        r.label = f"{card_name}_{counter:04d}"
                        counter += 1

                    data.append({
                        "Receipt_Label": r.label,
                        "Receipt_Filename": r.filename,
                        "Transaction_Date": r.matched_transaction.date,
                        "Transaction_Description": r.matched_transaction.description,
                        "Transaction_Amount": r.matched_transaction.amount,
                        "Receipt_Merchant": r.merchant,
                        "Receipt_Amount": r.amount,
                        "Amount_Difference": abs(r.matched_transaction.amount - r.amount),
                        "Receipt_Path": str(r.original_path)
                    })

            if data:
                excel_path = statements_dir / f"{card_name}.xlsx"
                df = pd.DataFrame(data)

                if excel_path.exists():
                    old = pd.read_excel(excel_path)
                    df = pd.concat([old, df]).drop_duplicates(subset=["Receipt_Label"])

                df.to_excel(excel_path, index=False)
                drive_sheet_manager(card_name, config.DRIVE_FOLDER_ID, records=data)
                logger.info(f"Saved {len(data)} matched records for {card_name}")
        unmatched = []
        for r in receipts:
            if r.matched_transaction is None:
                rec = {
                    "Receipt_Filename": r.filename,
                    "Receipt_Date": r.date,
                    "Receipt_Merchant": r.merchant,
                    "Receipt_Amount": r.amount,
                    "Receipt_Path": str(r.original_path),
                    "Type": "Unmatched"
                }
                unmatched.append(rec)

                if r.original_path:
                    upload_to_drive(str(r.original_path), config.OTHER_EMAIL_FOLDER_ID)
        if unmatched:
            unmatched_path = statements_dir / "Unmatched_Receipts.xlsx"
            df = pd.DataFrame(unmatched)
            if unmatched_path.exists():
                old = pd.read_excel(unmatched_path)
                df = pd.concat([old, df]).drop_duplicates(subset=["Receipt_Filename"])
            df.to_excel(unmatched_path, index=False)
            drive_sheet_manager("Unmatched_Receipts", config.OTHER_EMAIL_FOLDER_ID, records=unmatched)
        logger.info("AI FULL RECONCILIATION COMPLETED SUCCESSFULLY")
    except Exception as e:
        logger.exception(f"Reconcilation Failed: {e}")