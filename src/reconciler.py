import asyncio
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
from src import config
from src.drive_uploader import drive_sheet_manager, upload_to_drive
from src.email_client import fetch_financial_emails
from src.matcher import Matcher
from src.models import Transaction, Receipt
from src.pdf_parser import extract_records_from_file 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def run_full_reconciliation(limit=500):
    try:
        logger.info("="*80)
        logger.info("AI-POWERED CREDIT CARD RECONCILIATION STARTED")
        logger.info("="*80)

        # --------------------------
        # 1️⃣ Fetch financial emails
        # --------------------------
        emails_metadata = await fetch_financial_emails(limit=limit)
        logger.info(f"Total financial emails fetched: {len(emails_metadata)}")

        # --------------------------
        # 2️⃣ Process emails -> upload attachments + update sheet
        # --------------------------
        for email_data in emails_metadata:
            category = email_data.get("Category", "Other_Bills")
            target_sheet = category if category.lower() in ["meriwest", "amex", "chase", "jkgarnerdesign"] else "Other_Bills"
            record = {
                "Sender Name": email_data.get("sender_name", ""),
                "Sender Email": email_data.get("sender_email", ""),
                "Email Subject": email_data.get("subject", ""),
                "Email Date": email_data.get("date", ""),
                "Attachment Path": ", ".join([a["filename"] for a in email_data.get("attachments", [])]),
                "Category": category
            }

            # Upload attachments to Drive
            links = []
            for att in email_data.get("attachments", []):
                filename = att["filename"]
                temp_path = Path(config.STATEMENTS_DIR) / filename
                with open(temp_path, "wb") as f:
                    f.write(att["content"])
                links.append(upload_to_drive(str(temp_path), config.DRIVE_FOLDER_ID))
            record["Attachment Link"] = ", ".join(links)

            # Update Google Sheet
            sheet_id = drive_sheet_manager(target_sheet, config.DRIVE_FOLDER_ID, records=[record])
            logger.info(f"Email record uploaded to sheet: {target_sheet}")

        # --------------------------
        # 3️⃣ Parse all receipts
        # --------------------------
        receipts = []
        statements_dir = Path(config.STATEMENTS_DIR)
        if statements_dir.exists():
            for file_path in statements_dir.glob("*.*"):
                if file_path.suffix.lower() in [".pdf", ".jpg", ".jpeg", ".png", ".gif", ".xlsx", ".xls", ".csv", ".json", ".txt"]:
                    records = extract_records_from_file(file_path)
                    for r in records:
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

        # --------------------------
        # 4️⃣ Reconcile credit card statements
        # --------------------------
        cards = [
            ("Meriwest", statements_dir / "Meriwest_Credit_Card_Statement.pdf"),
            ("Amex", statements_dir / "Amex_Credit_Card_Statement.xlsx"),
            ("Chase", statements_dir / "Chase_Credit_Card_Statement.xlsx"),
        ]

        for card_name, path in cards:
            if not path.exists():
                logger.warning(f"Statement not found: {path}")
                continue

            # Parse transactions
            transactions = []
            if path.suffix.lower() in [".xlsx", ".xls"]:
                sheet_name = "Transaction Details" if "Amex" in card_name else 0
                df = pd.read_excel(path, sheet_name=sheet_name)
                date_col, desc_col, amt_col = ("Transaction Date", "Description", "Amount") if "Chase" in card_name else ("Date", "Description", "Amount")
                for _, row in df.iterrows():
                    if pd.isna(row.get(date_col)) or pd.isna(row.get(amt_col)):
                        continue
                    transactions.append(Transaction(str(row[date_col]), str(row.get(desc_col, "")), abs(float(row[amt_col]))))
            else:
                # PDF parsing
                with path.open("rb") as f:
                    text = PDFParser.extract_text(f.read())
                import re
                for line in text.splitlines():
                    line = line.strip()
                    if re.match(r"^\d{4}-\d{2}-\d{2}", line):
                        date = line.split()[0]
                        amt_match = re.search(r"(-?\d+\.\d{2})", line)
                        if amt_match:
                            transactions.append(Transaction(date, "Unknown Merchant", abs(float(amt_match.group(1)))))

            # Match receipts with transactions
            unmatched_receipts = [r for r in receipts if r.matched_card is None]
            for receipt in unmatched_receipts:
                best_email, score = Matcher.match_record_email(vars(receipt), emails_metadata)
                if score > 0.7:  # semantic matching threshold
                    receipt.matched_transaction = Transaction(
                        date=receipt.date,
                        description=best_email["subject"],
                        amount=receipt.amount
                    )

            # Label matched receipts
            counter = 1
            for r in receipts:
                if getattr(r, "matched_transaction", None) and r.matched_card is None:
                    r.matched_card = card_name
                    r.label = f"{card_name}_{counter:03d}"
                    counter += 1

            # Save reconciliation Excel
            data = [
                {
                    "Receipt_Label": r.label,
                    "Receipt_Filename": r.filename,
                    "Transaction_Date": r.matched_transaction.date if r.matched_transaction else "N/A",
                    "Transaction_Description": r.matched_transaction.description if r.matched_transaction else "N/A",
                    "Transaction_Amount": r.matched_transaction.amount if r.matched_transaction else 0.0,
                    "Receipt_Merchant": r.merchant,
                    "Receipt_Amount": r.amount,
                    "Amount_Difference": abs(r.matched_transaction.amount - r.amount) if r.matched_transaction else 0.0,
                    "Receipt_Path": str(r.original_path) if r.original_path else ""
                }
                for r in receipts if r.matched_card == card_name and r.matched_transaction
            ]
            if data:
                excel_path = statements_dir / f"{card_name}.xlsx"
                df = pd.DataFrame(data)
                if excel_path.exists():
                    existing_df = pd.read_excel(excel_path, engine='openpyxl')
                    df = pd.concat([existing_df, df], ignore_index=True)
                    df = df.drop_duplicates(subset=['Receipt_Label'], keep='last')
                df.to_excel(excel_path, index=False, engine='openpyxl')
                logger.info(f"Saved reconciliation file: {excel_path.name} with {len(data)} records")

                # Update Google Sheet
                sheet_id = drive_sheet_manager(card_name, config.DRIVE_FOLDER_ID, records=data)
                logger.info(f"Updated Google Sheet for card: {card_name}")

        # --------------------------
        # 5️⃣ Handle unmatched receipts
        # --------------------------
        unmatched_data = [
            {
                "Receipt_Filename": r.filename,
                "Receipt_Date": r.date,
                "Receipt_Merchant": r.merchant,
                "Receipt_Amount": r.amount,
                "Receipt_Path": str(r.original_path) if r.original_path else ""
            }
            for r in receipts if r.matched_transaction is None
        ]
        if unmatched_data:
            unmatched_path = statements_dir / "Unmatched_Receipts.xlsx"
            df_unmatched = pd.DataFrame(unmatched_data)
            if unmatched_path.exists():
                existing_df = pd.read_excel(unmatched_path, engine='openpyxl')
                df_unmatched = pd.concat([existing_df, df_unmatched], ignore_index=True)
                df_unmatched = df_unmatched.drop_duplicates(subset=['Receipt_Filename'], keep='last')
            df_unmatched.to_excel(unmatched_path, index=False, engine='openpyxl')
            for r in receipts:
                if r.matched_transaction is None and r.original_path and r.original_path.exists():
                    upload_to_drive(str(r.original_path), config.DRIVE_FOLDER_ID)
            logger.info(f"✓ Uploaded {len(unmatched_data)} unmatched receipt files to Google Drive")

        logger.info("✅ Full reconciliation workflow completed successfully.")

    except Exception as e:
        logger.exception(f"Full reconciliation workflow failed: {e}")