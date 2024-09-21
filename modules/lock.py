from enum import Enum
from modules.operation import OperationType


class LockType(Enum):
    IRL = "INTENTION READ"
    IWL = "INTENTION WRITE"
    IUL = "INTENTION UPDATE"
    ICL = "INTENTION CERTIFY"
    RL = "READ"
    WL = "WRITE"
    UL = "UPDATE"
    CL = "CERTIFY"


class Lock:
    def __init__(self, lock_type: LockType, transaction):
        """
        Initializes a lock with a type and the transaction holding it.
        """

        self.lock_type = lock_type
        self.transaction = transaction

    @classmethod
    def get_lock_type_based_on_operation(cls, operation):
        operation_to_lock_type = {
            OperationType.READ: LockType.RL,
            OperationType.UPDATE: LockType.UL,
            OperationType.WRITE: LockType.WL,
        }

        lock_type = operation_to_lock_type.get(operation)

        if lock_type is None:
            raise ValueError("Invalid operation")

        return lock_type

    def __repr__(self):
        return f"Lock({self.lock_type.value}, Transaction: {self.transaction.id})"
