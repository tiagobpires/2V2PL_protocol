from modules.lock import LockType
from modules.lock_manager import LockManager
from modules.transaction import Transaction
from modules.operation import OperationType


def main():
    # Initialize the lock manager
    lock_manager = LockManager()

    # Create transactions
    t1 = Transaction(lock_manager)
    t2 = Transaction(lock_manager)

    # Resource we're going to lock
    resource_1 = "Resource1"

    # Transaction 1 requests a Read Lock (RL)
    print(f"Transaction {t1.transaction_id} creates a Read Operation on {resource_1}")
    assert (
        t1.create_operation(OperationType.READ, resource_1) == True
    ), "Transaction 1 should acquire a Read Lock"

    # Transaction 2 requests a Write Lock (WL) (should be blocked)
    print(
        f"Transaction {t2.transaction_id} creates a Write Operation on {resource_1} (should be blocked)"
    )
    assert (
        t2.create_operation(OperationType.WRITE, resource_1) == False
    ), "Transaction 2 should be blocked by Transaction 1's Read Lock"

    # Transaction 1 promotes Read Lock to Write Lock (WL)
    print(
        f"Transaction {t1.transaction_id} promotes Read Lock to Write Lock on {resource_1}"
    )
    assert (
        lock_manager.promote_lock(t1, resource_1, LockType.WL) == True
    ), "Transaction 1 should successfully promote to Write Lock"

    # Transaction 2 tries Write Lock again (should still fail)
    print(
        f"Transaction {t2.transaction_id} tries Write Operation on {resource_1} (should still be blocked)"
    )
    assert (
        t2.create_operation(OperationType.WRITE, resource_1) == False
    ), "Transaction 2 should still be blocked by Transaction 1's Write Lock"

    # Commit Transaction 1, releasing locks
    print(f"Transaction {t1.transaction_id} commits")
    t1.commit_transaction()

    # Transaction 2 should be able to proceed after Transaction 1 commits
    print(f"Transaction {t2.transaction_id} retries Write Operation on {resource_1}")
    assert (
        t2.create_operation(OperationType.WRITE, resource_1) == True
    ), "Transaction 2 should now acquire the Write Lock"
    t2.commit_transaction()

    # Output final state of the lock manager
    print("\nFinal lock manager state:")
    print(lock_manager)


if __name__ == "__main__":
    main()
