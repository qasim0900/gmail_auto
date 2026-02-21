import os
import logging
from src.email_model import EmailMatcherModel


#-----------------------------
# ::  Logger Variable
#-----------------------------

""" 
This line creates a logger named after the current module for logging messages and errors.
"""

logger = logging.getLogger(__name__)


#-----------------------------
# :: Matcher Class
#-----------------------------

""" 
This Matcher class manages a singleton EmailMatcherModel, loading or initializing it, training it if needed, 
and providing a method to match a record's merchant and amount to relevant emails.
"""

class Matcher:
    _model = None
    _model_path = "email_ai_model.pkl"


    #-----------------------------
    # :: Get model Function
    #-----------------------------

    """ 
    This code safely loads or initializes the EmailMatcherModel, logs any errors, and ensures a 
    valid model instance is always returned to prevent failures during frequent calls.
    """

    @classmethod
    def get_model(cls):
        try:
            if cls._model is None:
                if os.path.exists(cls._model_path):
                    cls._model = EmailMatcherModel.load(cls._model_path)
                    logger.info("Loaded Email AI Model from pkl")
                else:
                    cls._model = EmailMatcherModel()
                    logger.info("Initialized new Email AI Model")
            return cls._model
        except Exception as e:
            logger.error(f"Failed to get Email AI Model ({type(e).__name__}): {e}")
            if cls._model is None:
                cls._model = EmailMatcherModel()
            return cls._model


    #-----------------------------
    # :: Match Email Function
    #-----------------------------

    """ 
    Match a record (merchant + amount) against emails using a singleton EmailMatcherModel.
    """

    @staticmethod
    def match_record_email(record, emails=None, threshold=0.5):
        try:
            if Matcher._model is None:
                if os.path.exists(Matcher._model_path):
                    Matcher._model = EmailMatcherModel.load(Matcher._model_path)
                    logger.info("Loaded Email AI Model from pkl")
                else:
                    Matcher._model = EmailMatcherModel()
                    logger.info("Initialized new Email AI Model")
            model = Matcher._model
            if getattr(model, 'tfidf_matrix', None) is None and emails:
                model.train(emails)
                model.save(Matcher._model_path)
                logger.info("Model trained and saved successfully")
            merchant = str(record.get('merchant', ''))
            amount = str(record.get('amount', ''))
            return model.match(merchant, amount, threshold)
        except Exception as e:
            logger.error(f"match_record_email failed ({type(e).__name__}): {e}")
            return None, 0.0
