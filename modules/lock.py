from enum import Enum

class LockType(Enum):
    IRL = "IRL"  # Intention Read Lock
    IWL = "IWL"  # Intention Write Lock
    IUL = "IUL"  # Intention Update Lock
    R = "R"  # Shared Lock (Read)
    W = "W"  # Exclusive Lock (Write)
    U = "U"  # Update Lock
    CL = "CL"  # Certify Lock

class Lock:
    def __init__(self, lock_type: LockType, transaction):
        """
        Initializes a lock with a type and the transaction holding it.
        """
        self.lock_type = lock_type
        self.transaction = transaction

    def __repr__(self):
        return f"Lock({self.lock_type.value}, Transaction: {self.transaction.id})"
