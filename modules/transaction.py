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
        
        # Adiciona um dicionário para rastrear os bloqueios detidos
        self.locks_held = {}

    def create_operation(self, operation_type: OperationType, resource: str):
        """
        Creates an operation and manages locking.
        """
        operation = Operation(operation_type, resource)
        self.operations.append(operation)

        # Solicita o bloqueio necessário para esta operação
        if not self.lock_manager.request_lock(self, resource, operation_type):  # Atualizando para passar self
            # Bloqueia a transação se o bloqueio não puder ser garantido
            self.block_transaction(resource)
            print(f"Transaction {self.transaction_id} is blocked, waiting for {resource}.")
        else:
            print(f"Transaction {self.transaction_id} obtained {operation_type.name} lock on {resource}.")

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
        self.lock_manager.release_all_locks(self)  # Libera todos os bloqueios

    def abort_transaction(self):
        """
        Aborts the transaction, clears all held locks, and resets the state.
        """
        self.state = "aborted"
        self.lock_manager.release_all_locks(self)  # Libera todos os bloqueios
        self.operations.clear()  # Clear all operations upon abort

    def __repr__(self):
        return f"Transaction({self.transaction_id}, State: {self.state}, Operations: {self.operations})"
