from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('all-MiniLM-L6-v2')

class Matcher:
    @staticmethod
    def match_record_email(record, emails):
        best_score = 0
        best_email = None
        record_text = f"{record.get('merchant','')} {record.get('amount','')}"
        for email in emails:
            email_text = f"{email['subject']} {email.get('body','')}"
            score = util.cos_sim(model.encode(record_text), model.encode(email_text)).item()
            if score > best_score:
                best_score = score
                best_email = email
        return best_email, best_score