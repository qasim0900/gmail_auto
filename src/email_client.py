import asyncio
from email.utils import parseaddr
import imaplib, email, hashlib, logging
from src.config import EMAIL_ADDRESS, EMAIL_PASSWORD, IMAP_SERVER


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)


#-------------------------------------------
# ::  Fetch Financial Emails Function
#-------------------------------------------

""" 
This async function fetches emails from the inbox, filters for financial-related messages (like invoices or receipts), 
avoids duplicates using hashes, extracts attachments and content, and returns the relevant emails.
"""

async def fetch_financial_emails(limit=500):
    try:
        processed = set()
        financial_emails = []
        def fetch_process():
            mail = imaplib.IMAP4_SSL(IMAP_SERVER)
            mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            mail.select("INBOX")
            _, messages = mail.search(None, "ALL")
            ids = messages[0].split()[-limit:]

            for msg_id in ids:
                _, data = mail.fetch(msg_id, "(RFC822)")
                if not data or not data[0]:
                    continue
                raw = data[0][1]
                h = hashlib.md5(raw).hexdigest()
                if h in processed:
                    continue
                processed.add(h)
                msg = email.message_from_bytes(raw)
                subject = msg.get("Subject") or ""
                sender_name, sender_email = parseaddr(msg.get("From") or "")
                date = msg.get("Date") or ""
                body = ""
                attachments = []
                for part in msg.walk():
                    ct = part.get_content_type()
                    if ct == "text/plain" and not body:
                        body = part.get_payload(decode=True).decode(errors="ignore")
                    if part.get_filename():
                        attachments.append({
                            "filename": part.get_filename(),
                            "content": part.get_payload(decode=True)
                        })
                email_data = {
                    "hash": h,
                    "sender_name": sender_name,
                    "sender_email": sender_email,
                    "subject": subject,
                    "body": body,
                    "date": date,
                    "attachments": attachments
                }
                text = f"{subject} {body} {sender_email}".lower()
                patterns = ["receipt", "invoice", "bill", "statement", "payment", "order",
                            "confirmation", "transaction", "paid", "amount due", "total paid", "jkgarnerdesign"]
                if any(p in text for p in patterns):
                    financial_emails.append(email_data)
            mail.logout()
            logger.info(f"Fetched {len(financial_emails)} financial emails")
            return financial_emails
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, fetch_process)
    except Exception as e:
        logger.error(f"Error fetching financial emails ({type(e).__name__}): {e}")
        return []