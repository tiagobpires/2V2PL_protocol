from modules.operation import Operation, OperationType
from datetime import datetime
from modules.granularity_graph import GranularityGraphNode
import time

class Transaction:
    transaction_counter = 1

    def __init__(self, lock_manager, await_graph):
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
        self.await_graph = await_graph
        time.sleep(0.1)
        self.timestamp = datetime.now()

        await_graph.add_vertex(self)

    def create_operation(self, node: GranularityGraphNode, operation_type: OperationType):
        """
        Creates an operation and manages locking.
        """

        if self.state != "active":
            print(f"Transaction {self.transaction_id} cannot create operations in state {self.state}.")
            return False
        
        operation = Operation(operation_type, node)
        self.operations.append(operation)
        self.lock_manager.request_lock(self, node, operation_type)

    def block_transaction(self, node):
        """
        Blocks the transaction and sets the resource it is waiting for.
        """

        self.state = "blocked"
        self.waiting_for = node

    def unblock_transaction(self):
        """
        Unblocks the transaction and resets the waiting state.
        """

        self.state = "active"
        self.waiting_for = None

    def commit_transaction(self):
        """
        Marks the transaction as committed, releases all locks, and wakes up waiting transactions.
        """
        
        self.state = "committed"

        # Release all locks held by the transaction
        self.lock_manager.release_all_locks(self)
        self._unblock_waiting_transactions()
        del self.await_graph.vertices[self.transaction_id]

        print(f"Transaction {self.transaction_id} committed.")

    def abort_transaction(self):
        """
        Aborts the transaction, clears all held locks, wakes up waiting transactions, and resets the state.
        """
        self.state = "aborted"

        # Release all locks held by the transaction
        self.lock_manager.release_all_locks(self)
        self._unblock_waiting_transactions()        
        del self.await_graph.vertices[self.transaction_id]

        self.operations.clear()

        print(f"Transaction {self.transaction_id} aborted.")
        
    def _unblock_waiting_transactions(self):
        """
        Unblock transactions waiting for current transaction
        """

        # Find and wake up any transactions waiting for locks held by this transaction
        waiting_transactions = self.await_graph.get_waiting_transactions(self.transaction_id)

        for vertex, data in waiting_transactions:
            waiting_transaction = data["transaction"]
            waiting_transaction.unblock_transaction()  # Unblocks the waiting transaction

            self.await_graph.remove_edge(vertex, self.transaction_id)  # Remove edge from wait-for graph

    @staticmethod
    def get_most_recent_transaction(transaction, blocking_transaction):
        """
        Compares two transactions and returns the most recent one based on the timestamp.
        """
        if transaction.timestamp > blocking_transaction.timestamp:
            return transaction
        else:
            return blocking_transaction

    def __repr__(self):
        return f"Transaction({self.transaction_id}, State: {self.state}, Operations: {self.operations})"
