import asyncio
from email.utils import parseaddr
import imaplib, email, hashlib, logging
from src.config import EMAIL_ADDRESS, EMAIL_PASSWORD, IMAP_SERVER

logger = logging.getLogger(__name__)

async def fetch_financial_emails(limit=500):
    processed_emails = set()
    financial_emails = []

    def _get_body(msg):
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    return part.get_payload(decode=True).decode(errors="ignore")
        return msg.get_payload(decode=True).decode(errors="ignore") if msg.get_content_type() == "text/plain" else ""

    def _get_attachments(msg):
        atts = []
        for part in msg.walk():
            if part.get_filename():
                atts.append({"filename": part.get_filename(), "content": part.get_payload(decode=True)})
        return atts

    def _is_financial(email_data):
        text = f"{email_data['subject']} {email_data['body']} {email_data['sender_email']}".lower()
        patterns = ["receipt", "invoice", "bill", "statement", "payment", "order", "confirmation", "transaction", "paid", "amount due", "total paid", "jkgarnerdesign"]
        return any(p in text for p in patterns)

    def _fetch_and_process():
        try:
            mail = imaplib.IMAP4_SSL(IMAP_SERVER)
            mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            mail.select("INBOX")
            _, messages = mail.search(None, "ALL")
            ids = messages[0].split()[-limit:]

            for msg_id in ids:
                _, data = mail.fetch(msg_id, "(RFC822)")
                if not data or not data[0]:
                    continue

                raw_email = data[0][1]
                email_hash = hashlib.md5(raw_email).hexdigest()
                if email_hash in processed_emails:
                    continue
                processed_emails.add(email_hash)

                msg = email.message_from_bytes(raw_email)
                subject = msg.get("Subject") or ""
                sender_name, sender_email = parseaddr(msg.get("From") or "")
                date = msg.get("Date") or ""
                body = _get_body(msg)
                attachments = _get_attachments(msg)

                email_data = {
                    "hash": email_hash,
                    "sender_name": sender_name,
                    "sender_email": sender_email,
                    "subject": subject,
                    "body": body,
                    "date": date,
                    "attachments": attachments
                }

                if _is_financial(email_data):
                    financial_emails.append(email_data)

            mail.logout()
            logger.info(f"Fetched {len(financial_emails)} financial emails")
            return financial_emails
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            return []

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_and_process)