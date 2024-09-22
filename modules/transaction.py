from modules.operation import Operation, OperationType






class Transaction:
    transaction_counter = 0

    def __init__(self, lock_manager):
        """
        Initializes a transaction with a unique transaction ID.
        """
        self.transaction_id = Transaction.transaction_counter
        Transaction.transaction_counter += 1

        self.state = "active"  # Possible states: active, blocked, committed, aborted
        self.waiting_for = None
        self.operations = []
        self.lock_manager = lock_manager
        self.locks_held = {}
        

    def create_operation(self, operation_type: OperationType, resource: str):
        """
        Creates an operation and manages locking.
        """
        if self.state != "active":
            print(f"Transaction {self.transaction_id} cannot create operations in state {self.state}.")
            return False
        operation = Operation(operation_type, resource)
        self.operations.append(operation)

        if not self.lock_manager.request_lock(self, resource, operation_type):
            self.block_transaction(resource)
            print(
                f"Transaction {self.transaction_id} is blocked, waiting for {resource}."
            )
            return False
        else:
            print(
                f"Transaction {self.transaction_id} obtained {operation_type.name} lock on {resource}."
            )
            return True

    def block_transaction(self, resource: str):
        """
        Blocks the transaction and sets the resource it is waiting for.
        """

        self.state = "blocked"
        self.waiting_for = resource

    def unblock_transaction(self):
        """
        Unblocks the transaction and resets the waiting state.
        """

        self.state = "active"
        self.waiting_for = None

    def commit_transaction(self):
        """
        Marks the transaction as committed and releases all locks.
        """

        self.state = "committed"
        self.lock_manager.release_all_locks(self)

        print(f"Transaction {self.transaction_id} committed.")

    def abort_transaction(self):
        """
        Aborts the transaction, clears all held locks, and resets the state.
        """
        self.state = "aborted"
        self.lock_manager.release_all_locks(self)
        self.operations.clear()

        print(f"Transaction {self.transaction_id} aborted.")

    def __repr__(self):
        return f"Transaction({self.transaction_id}, State: {self.state}, Operations: {self.operations})"
