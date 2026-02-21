import logging
import re
import os
from src.email_model import EmailMatcherModel

logger = logging.getLogger(__name__)

class Matcher:
    _model = None

    @classmethod
    def get_model(cls):
        if cls._model is None:
            model_path = "email_ai_model.pkl"
            if os.path.exists(model_path):
                cls._model = EmailMatcherModel.load(model_path)
                logger.info("Loaded Email AI Model from pkl")
            else:
                cls._model = EmailMatcherModel()
                logger.info("Initialized new Email AI Model")
        return cls._model

    @staticmethod
    def match_record_email(record, emails):
        model = Matcher.get_model()
        
        # If model has no tfidf_matrix, "train" it with current emails
        if getattr(model, 'tfidf_matrix', None) is None and emails:
            model.train(emails)
            model.save("email_ai_model.pkl")

        merchant = str(record.get('merchant', ''))
        amount = str(record.get('amount', ''))
        
        return model.match(merchant, amount)
