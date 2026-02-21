import joblib
import os
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

class EmailMatcherModel:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words='english')
        self.processed_emails = []
        self.tfidf_matrix = None

    def train(self, emails):
        """
        'Train' the model by vectorizing the available emails using TF-IDF.
        This is much lighter than using transformer models.
        """
        if not emails:
            logger.warning("No emails provided for training.")
            return

        self.processed_emails = emails
        texts = [f"{e.get('subject', '')} {e.get('body', '')}" for e in emails]
        logger.info(f"Vectorizing {len(texts)} emails...")
        self.tfidf_matrix = self.vectorizer.fit_transform(texts)
        logger.info("Vectorization complete.")

    def match(self, merchant, amount, threshold=0.5):
        """
        Match a transaction (merchant + amount) against the vectorized emails.
        """
        if self.tfidf_matrix is None:
            logger.error("Model not trained with emails.")
            return None, 0.0

        query_text = f"{merchant} {amount}"
        query_vec = self.vectorizer.transform([query_text])
        
        # Calculate similarities
        similarities = cosine_similarity(query_vec, self.tfidf_matrix)[0]
        
        best_idx = similarities.argmax()
        best_score = similarities[best_idx]
        
        if best_score >= threshold:
            return self.processed_emails[best_idx], float(best_score)
        
        return None, float(best_score)

    def save(self, file_path):
        joblib.dump(self, file_path)
        logger.info(f"Model saved to {file_path}")

    @staticmethod
    def load(file_path):
        if os.path.exists(file_path):
            try:
                return joblib.load(file_path)
            except Exception as e:
                logger.error(f"Failed to load model: {e}")
        return None
