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


#---------------------------------------
# :: Run Reconcilication Function
#---------------------------------------

""" 
This function automates fetching emails, extracting receipts, 
matching them to card transactions using AI, and saving results to Drive and Excel.
"""

async def run_full_reconciliation(limit=500):
    try:
        logger.info("="*80)
        logger.info("AI-POWERED CREDIT CARD RECONCILIATION STARTED")
        logger.info("="*80)
        emails_metadata = await fetch_financial_emails(limit=limit)
        logger.info(f"Total financial emails fetched: {len(emails_metadata)}")
        statements_dir = Path(config.STATEMENTS_DIR)
        for email_data in emails_metadata:
            category = email_data.get("Category", "Other_Bills")
            target_sheet = category if category.lower() in ["meriwest","amex","chase","jkgarnerdesign"] else "Other_Bills"
            record = {
                "Sender Name": email_data.get("sender_name",""),
                "Sender Email": email_data.get("sender_email",""),
                "Email Subject": email_data.get("subject",""),
                "Email Date": email_data.get("date",""),
                "Attachment Path": ", ".join([a["filename"] for a in email_data.get("attachments",[])]),
                "Category": category
            }
            links = []
            for att in email_data.get("attachments",[]):
                temp_path = statements_dir / att["filename"]
                temp_path.write_bytes(att["content"])
                links.append(upload_to_drive(str(temp_path), config.DRIVE_FOLDER_ID))
            record["Attachment Link"] = ", ".join(links)

            drive_sheet_manager(target_sheet, config.DRIVE_FOLDER_ID, records=[record])
            logger.info(f"Email record uploaded to sheet: {target_sheet}")
        receipts = []
        for file_path in statements_dir.glob("*.*"):
            if file_path.suffix.lower() in [".pdf",".jpg",".jpeg",".png",".gif",".xlsx",".xls",".csv",".json",".txt"]:
                for r in extract_records_from_file(file_path):
                    receipt = Receipt(
                        filename=file_path.name,
                        date=r.get("date",datetime.now().strftime("%Y-%m-%d")),
                        merchant=r.get("merchant","Unknown"),
                        amount=float(r.get("amount",0.0)),
                        email_id=""
                    )
                    receipt.original_path = file_path
                    receipts.append(receipt)
        logger.info(f"Total receipts parsed: {len(receipts)}")
        cards = [
            ("Meriwest", statements_dir / "Meriwest_Credit_Card_Statement.pdf"),
            ("Amex", statements_dir / "Amex_Credit_Card_Statement.xlsx"),
            ("Chase", statements_dir / "Chase_Credit_Card_Statement.xlsx")
        ]
        for card_name, path in cards:
            if not path.exists():
                logger.warning(f"Statement not found: {path}")
                continue
            transactions = []
            if path.suffix.lower() in [".xlsx",".xls"]:
                sheet_name = "Transaction Details" if "Amex" in card_name else 0
                df = pd.read_excel(path, sheet_name=sheet_name)
                date_col, desc_col, amt_col = ("Transaction Date","Description","Amount") if "Chase" in card_name else ("Date","Description","Amount")
                for _, row in df.iterrows():
                    if pd.isna(row.get(date_col)) or pd.isna(row.get(amt_col)):
                        continue
                    transactions.append(Transaction(str(row[date_col]), str(row.get(desc_col,"")), abs(float(row[amt_col]))))
            else:
                with fitz.open(path) as doc:
                    text = "\n".join(page.get_text() for page in doc)
                import re
                for line in text.splitlines():
                    line = line.strip()
                    if re.match(r"^\d{4}-\d{2}-\d{2}", line):
                        date = line.split()[0]
                        amt_match = re.search(r"(-?\d+\.\d{2})", line)
                        if amt_match:
                            transactions.append(Transaction(date,"Unknown Merchant", abs(float(amt_match.group(1)))))
            for receipt in [r for r in receipts if r.matched_card is None]:
                best_email, score = Matcher.match_record_email(vars(receipt), emails_metadata)
                if score > 0.7:
                    receipt.matched_transaction = Transaction(receipt.date, best_email["subject"], receipt.amount)
                    receipt.matched_card = card_name
            counter = 1
            data = []
            for r in receipts:
                if getattr(r,"matched_transaction",None) and r.matched_card == card_name:
                    if not r.label:
                        r.label = f"{card_name}_{counter:03d}"
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
                        "Receipt_Path": str(r.original_path) if r.original_path else ""
                    })
            if data:
                excel_path = statements_dir / f"{card_name}.xlsx"
                df = pd.DataFrame(data)
                if excel_path.exists():
                    df = pd.concat([pd.read_excel(excel_path, engine="openpyxl"), df], ignore_index=True).drop_duplicates(subset=["Receipt_Label"])
                df.to_excel(excel_path, index=False, engine="openpyxl")
                drive_sheet_manager(card_name, config.DRIVE_FOLDER_ID, records=data)
                logger.info(f"Saved reconciliation for {card_name}: {len(data)} records")
        unmatched_data = []
        for r in receipts:
            if r.matched_transaction is None:
                is_jk = "jkgarnerdesign" in r.merchant.lower() or "jkgarnerdesign" in r.filename.lower()
                folder_id = config.OTHER_EMAIL_FOLDER_ID if is_jk else config.DRIVE_FOLDER_ID
                rec = {
                    "Receipt_Filename": r.filename,
                    "Receipt_Date": r.date,
                    "Receipt_Merchant": r.merchant,
                    "Receipt_Amount": r.amount,
                    "Receipt_Path": str(r.original_path) if r.original_path else "",
                    "Type": "JKGarnerDesign" if is_jk else "Unmatched"
                }
                unmatched_data.append(rec)
                if r.original_path and r.original_path.exists():
                    upload_to_drive(str(r.original_path), folder_id)
        if unmatched_data:
            unmatched_path = statements_dir / "Unmatched_Receipts.xlsx"
            df_unmatched = pd.DataFrame(unmatched_data)
            if unmatched_path.exists():
                df_unmatched = pd.concat([pd.read_excel(unmatched_path, engine="openpyxl"), df_unmatched], ignore_index=True).drop_duplicates(subset=["Receipt_Filename"])
            df_unmatched.to_excel(unmatched_path, index=False, engine="openpyxl")
            drive_sheet_manager("Unmatched_and_JKGarner", config.OTHER_EMAIL_FOLDER_ID, records=unmatched_data)
            logger.info(f"Processed {len(unmatched_data)} unmatched/JKGarner receipts")
        logger.info("Full reconciliation workflow completed successfully.")
    except Exception as e:
        logger.exception(f"Full reconciliation workflow failed: {e}")