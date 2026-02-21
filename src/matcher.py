import logging
import re
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class Matcher:
    @staticmethod
    def match_record_email(record, emails):
        best_score = 0
        best_email = None
        merchant = str(record.get('merchant', '')).lower()
        amount = str(record.get('amount', ''))
        record_text = f"{merchant} {amount}"
        for email in emails:
            subject = str(email.get('subject', '')).lower()
            body = str(email.get('body', '')).lower()
            email_text = f"{subject} {body}"
            score = SequenceMatcher(None, record_text, email_text).ratio()
            if merchant and (merchant in subject or merchant in body):
                score += 0.3
            
            if score > best_score:
                best_score = score
                best_email = email
                
        return best_email, min(best_score, 1.0)
