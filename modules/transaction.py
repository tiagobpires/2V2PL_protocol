from modules.operation import Operation, OperationType
from datetime import datetime
from modules.granularity_graph import GranularityGraphNode
import time
from modules.lock import Lock


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
        self.pending_operations = []  # Operations waiting to be retried
        self.lock_manager = lock_manager
        self.locks_held = {}
        self.await_graph = await_graph
        time.sleep(0.1)  # Simulate delay for unique timestamp
        self.timestamp = datetime.now()

        await_graph.add_vertex(self)

    def create_operation(
        self, node: GranularityGraphNode, operation_type: OperationType
    ):
        """
        Adds an operation to the pending_operations and executes it if possible.
        """
        operation = Operation(operation_type, node)
        self.pending_operations.append(operation)
        self.execute_operations()

    def execute_operations(self):
        """
        Tries to execute pending operations.
        """
        while self.pending_operations:
            if self.state == "active":
                # Try to execute the first pending operation
                operation = self.pending_operations[0]
                if operation.operation_type == OperationType.COMMIT:
                    self.commit_transaction()
                    continue

                requested_lock_type = Lock.get_lock_type_based_on_operation(
                    operation.operation_type
                )

                # Check if the transaction already holds any lock on the node
                if operation.node in self.locks_held:
                    current_lock_type = self.locks_held[operation.node]

                    # If the current lock is not the requested one, promote it
                    if current_lock_type != requested_lock_type:
                        print(
                            f"Transaction {self.transaction_id} is promoting lock from {current_lock_type} to {requested_lock_type} on {operation.node}."
                        )
                        success = self.lock_manager.promote_lock(
                            self, operation.node, requested_lock_type
                        )

                        if success:
                            print(
                                f"Transaction {self.transaction_id} successfully promoted lock on {operation.node} to {requested_lock_type}."
                            )
                            self.lock_manager.operations_order.append((self, operation))
                            self.pending_operations.pop(
                                0
                            )  # Remove the operation after success
                        else:
                            print(
                                f"Transaction {self.transaction_id} failed to promote lock on {operation.node}."
                            )
                            break  # Stop if promotion fails
                    else:
                        # If the lock is the same as requested, no need to promote
                        print(
                            f"Transaction {self.transaction_id} already holds the requested lock {current_lock_type} on {operation.node}."
                        )
                        self.lock_manager.operations_order.append((self, operation))
                        self.pending_operations.pop(0)  # Remove the operation
                else:
                    # If no lock is held, request the lock
                    success = self.lock_manager.request_lock(
                        self, operation.node, operation.operation_type
                    )
                    if success:
                        print(
                            f"Transaction {self.transaction_id} acquired lock {requested_lock_type} on {operation.node}."
                        )
                        self.lock_manager.operations_order.append((self, operation))
                        self.pending_operations.pop(
                            0
                        )  # Remove the operation after success
                    else:
                        break  # Stop if the lock cannot be acquired
            else:
                break  # Is not active

    def block_transaction(self, node):
        """
        Blocks the transaction and prevents further operations.
        """
        self.state = "blocked"
        self.waiting_for = node
        print(f"Transaction {self.transaction_id} is now blocked waiting for {node}.")

    def unblock_transaction(self):
        """
        Unblocks the transaction and retries pending operations.
        """
        self.state = "active"
        self.waiting_for = None
        print(f"Transaction {self.transaction_id} is now unblocked.")

    def commit_transaction(self):
        """
        Commits the transaction, releases all locks, and clears pending operations.
        """
        self.state = "committed"
        self.lock_manager.release_all_locks(self)
        self.pending_operations.clear()
        waiting_transactions = self._unblock_waiting_transactions()
        del self.await_graph.vertices[self.transaction_id]
        self.lock_manager.operations_order.append((self, "Commited"))

        print(f"Transaction {self.transaction_id} committed.")

        for _, data in waiting_transactions:
            data["transaction"].execute_operations()

    def abort_transaction(self):
        """
        Aborts the transaction, clears all locks, and resets its state.
        """
        self.state = "aborted"
        self.lock_manager.release_all_locks(self)
        self.pending_operations.clear()

        self.lock_manager.operations_order.append((self, "Aborted"))
        print(f"Transaction {self.transaction_id} aborted.")

        waiting_transactions = self._unblock_waiting_transactions()
        del self.await_graph.vertices[self.transaction_id]

        for _, data in waiting_transactions:
            data["transaction"].execute_operations()

    def _unblock_waiting_transactions(self):
        """
        Unblock transactions waiting for current transaction
        """
        waiting_transactions = self.await_graph.get_waiting_transactions(
            self.transaction_id
        )

        for vertex, data in waiting_transactions:
            waiting_transaction = data["transaction"]
            self.await_graph.remove_edge(
                vertex, self.transaction_id
            )  # Remove edge from wait-for graph

            waiting_transaction.unblock_transaction()  # Unblocks the waiting transaction

        return waiting_transactions

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
        return f"Transaction({self.transaction_id}, State: {self.state}, Pending Operations: {len(self.pending_operations)})"
