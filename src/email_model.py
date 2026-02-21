import os
import joblib
import logging
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)



#---------------------------------
# ::  Email Matcher Model Class
#---------------------------------

""" 
EmailMatcherModel trains a TF-IDF model on emails, matches them to a merchant and amount 
using cosine similarity, and supports saving/loading, with logging.
"""

class EmailMatcherModel:
    
    #-----------------------------
    # :: __init__ Function
    #-----------------------------

    """ 
    This constructor initializes the TF-IDF vectorizer, an empty email list, and a placeholder for the TF-IDF matrix.
    """

    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words='english')
        self.emails = []
        self.tfidf_matrix = None

    #-----------------------------
    # ::  Train Function
    #-----------------------------

    """ 
    This method trains the TF-IDF model on email subjects and bodies, storing the matrix and logging the training status.
    """
    
    def train(self, emails):
        try:
            if not emails:
                logger.warning("No emails provided for training.")
                return
            self.emails = emails
            texts = [f"{e.get('subject','')} {e.get('body','')}" for e in emails]
            self.tfidf_matrix = self.vectorizer.fit_transform(texts)
            logger.info(f"Trained model on {len(texts)} emails.")
        except Exception as e:
            logger.error(f"Training failed ({type(e).__name__}): {e}")


    #-----------------------------
    # ::  Match Function
    #-----------------------------

    """ 
    This method compares a merchant and amount against trained emails using cosine 
    similarity, returning the best match and its score if above the threshold.
    """

    def match(self, merchant, amount, threshold=0.5):
        try:
            if self.tfidf_matrix is None:
                logger.error("Model not trained.")
                return None, 0.0
            query_vec = self.vectorizer.transform([f"{merchant} {amount}"])
            sims = cosine_similarity(query_vec, self.tfidf_matrix)[0]
            idx = sims.argmax()
            score = float(sims[idx])
            return (self.emails[idx], score) if score >= threshold else (None, score)
        except Exception as e:
            logger.error(f"Matching failed ({type(e).__name__}): {e}")
            return None, 0.0

    #-----------------------------
    # ::  Save Function
    #-----------------------------

    """ 
    This method saves the trained model to a file at the specified path and logs success or errors.
    """

    def save(self, path):
        try:
            joblib.dump(self, path)
            logger.info(f"Model saved to {path}")
        except Exception as e:
            logger.error(f"Failed to save model ({type(e).__name__}): {e}")


    #-----------------------------
    # ::  Load Function
    #-----------------------------

    """ 
    This static method loads a saved model from a file, returning it if successful or None on failure, with error logging.
    """

    @staticmethod
    def load(path):
        try:
            if os.path.exists(path):
                return joblib.load(path)
        except Exception as e:
            logger.error(f"Failed to load model ({type(e).__name__}): {e}")
        return None