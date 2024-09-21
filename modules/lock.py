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

    @staticmethod
    def get_lock_type_based_on_operation(operation):
        operation_to_lock_type = {
            OperationType.READ: LockType.RL,
            OperationType.UPDATE: LockType.UL,
            OperationType.WRITE: LockType.WL,
        }

        lock_type = operation_to_lock_type.get(operation)

        if lock_type is None:
            raise ValueError("Invalid operation")

        return lock_type

    @staticmethod
    def validate_promotion(current_lock_type: LockType, new_lock_type: LockType):
        """
        Validates if the promotion from the current lock type to the new lock type is allowed.
        """

        valid_promotions = {
            LockType.RL: [
                LockType.UL,
                LockType.WL,
            ],  # Read Lock can be promoted to Update or Write
            LockType.UL: [
                LockType.WL,
                LockType.CL,
            ],  # Update Lock can be promoted to Write or Certify
            LockType.WL: [LockType.CL],  # Write Lock can be promoted to Certify
            LockType.IRL: [
                LockType.RL
            ],  # Intention Read Lock can be promoted to Read Lock
            LockType.IWL: [
                LockType.WL
            ],  # Intention Write Lock can be promoted to Write Lock
            LockType.IUL: [
                LockType.UL
            ],  # Intention Update Lock can be promoted to Update Lock
        }

        if new_lock_type not in valid_promotions.get(current_lock_type, []):
            raise ValueError(
                f"Invalid lock promotion from {current_lock_type} to {new_lock_type}."
            )

    @staticmethod
    def check_conflicting_locks(
        current_locks, current_lock_type: LockType, new_lock_type: LockType
    ):
        """
        Ensures that there are no conflicting locks that would prevent the promotion.
        Write, Update, and Certify Locks require exclusivity.
        """

        if new_lock_type in [
            LockType.WL,
            LockType.UL,
            LockType.CL,
        ]:  # Write, Update, and Certify require exclusivity
            if any(
                current_locks[lock_type]
                for lock_type in [LockType.RL, LockType.UL, LockType.WL]
                if lock_type != current_lock_type
            ):
                return False  # Conflicting locks exist
        return True

    def __repr__(self):
        return f"Lock({self.lock_type.value}, Transaction: {self.transaction.id})"
