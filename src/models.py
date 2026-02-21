from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

# ----------------------------
# :: Transaction Class
# ----------------------------

""" 
The Transaction class represents a single financial transaction.
"""


@dataclass(frozen=True, slots=True)
class Transaction:
    date: str
    description: str
    amount: float = field(metadata={"units": "USD"})

    # ----------------------------------------------
    # :: Post Initialization Validation Function
    # ----------------------------------------------

    """ 
    The __post_init__ method is called immediately after the dataclass is initialized.
    """

    def __post_init__(self):
        if not isinstance(self.date, str):
            raise TypeError(
                f"date must be str, got {type(self.date).__name__}")
        if not isinstance(self.description, str):
            raise TypeError(
                f"description must be str, got {type(self.description).__name__}")
        if not isinstance(self.amount, (int, float)):
            raise TypeError(
                f"amount must be float, got {type(self.amount).__name__}")

# ----------------------------
# :: Receipt Class
# ----------------------------


""" 
The Receipt class represents a single receipt.
"""


@dataclass(slots=True)
class Receipt:
    filename: str
    date: str
    merchant: str
    amount: float
    email_id: str
    matched_transaction: Optional[Transaction] = field(default=None)
    label: str = ""
    matched_card: Optional[str] = field(default=None)
    original_path: Optional[Path] = field(default=None)

    # ----------------------------------------------
    # :: Post Initialization Validation Function
    # ---------------------------------------------

    """ 
    The __post_init__ method is called immediately after the dataclass is initialized.
    """

    def __post_init__(self):
        if not all(isinstance(attr, str) for attr in (self.filename, self.date, self.merchant, self.email_id)):
            raise TypeError(
                "filename, date, merchant, and email_id must all be strings")
        if not isinstance(self.amount, (int, float)):
            raise TypeError(
                f"amount must be a float, got {type(self.amount).__name__}")
        if self.matched_transaction and not isinstance(self.matched_transaction, Transaction):
            raise TypeError(
                "matched_transaction must be a Transaction or None")
