from modules.lock import LockType, Lock
from modules.transaction import Transaction
from modules.granularity_graph import GranularityGraph, GranularityGraphNode
from modules.await_graph import Graph


class LockManager:
    def __init__(self, granularity_graph: GranularityGraph, await_graph: Graph):
        """
        Initializes the lock manager to track locks on resources with multiple levels of granularity.
        """

        self.granularity_graph = granularity_graph
        self.await_graph = await_graph
        self.operations_order = []

    def _initialize_resource(self, resource: str):
        """
        Helper function to initialize the lock entry for a resource.
        """
        if resource not in self.locks:
            self.locks[resource] = {
                LockType.IRL: set(),
                LockType.IWL: set(),
                LockType.IUL: set(),
                LockType.ICL: set(),
                LockType.RL: set(),
                LockType.WL: set(),
                LockType.UL: set(),
                LockType.CL: set(),
            }

    def request_lock(
        self,
        transaction: Transaction,
        node: GranularityGraphNode,
        operation,
    ):
        """
        Requests a lock for the resource and updates the wait-for graph if blocked.
        """

        if transaction.state == "blocked":
            return False

        lock_type = Lock.get_lock_type_based_on_operation(operation)
        print(
            f"Transaction {transaction.transaction_id} requests {lock_type} on {node}."
        )
        current_locks = node.locks

        # Check if transaction already has this type
        if transaction in node.locks[lock_type]:
            return True

        # Check if Certify Lock is already present
        if current_locks[LockType.CL]:
            blocking_transaction = list(current_locks[LockType.CL])[
                0
            ]  # Certify lock held by another transaction

            if not self.await_graph.add_edge(
                transaction.transaction_id, blocking_transaction.transaction_id
            ):
                return False

            transaction.block_transaction(node)
            self._deal_with_deadlock(transaction, blocking_transaction)
            return False

        # If no locks are present, grant the lock
        if not any(current_locks.values()):
            current_locks[lock_type].add(transaction)
            transaction.locks_held[node] = lock_type
            node.add_lock(transaction, lock_type)
            return True

        # Certify Lock (CL) can only be granted if no other locks exist
        if lock_type == LockType.CL:
            has_block = False
                        
            for locks in current_locks.values():
                if len(list(locks)) > 0:
                    has_block = True
            
            if has_block:
                blocking_transaction = self._get_first_blocking_transaction(
                    current_locks
                )
                
                if not self.await_graph.add_edge(
                    transaction.transaction_id, blocking_transaction.transaction_id
                ):
                    return False
                                
                # self.await_graph.display_graph()
                
                transaction.block_transaction(node)
                self._deal_with_deadlock(transaction, blocking_transaction)
                return False

            current_locks[LockType.CL].add(transaction)
            transaction.locks_held[node] = LockType.CL
            node.add_lock(transaction, lock_type)
            return True

        # Handle conflicting locks and intention locks
        blocking_transaction = self._can_grant_lock(lock_type, current_locks)
        if blocking_transaction is True:
            current_locks[lock_type].add(transaction)
            transaction.locks_held[node] = lock_type
            node.add_lock(transaction, lock_type)
            return True
        else:
            # blocking_transaction contains the transaction that is holding a conflicting lock
            if not self.await_graph.add_edge(
                transaction.transaction_id, blocking_transaction.transaction_id
            ):
                return False

            transaction.block_transaction(node)
            self._deal_with_deadlock(transaction, blocking_transaction)
            return False

    def _can_grant_lock(
        self,
        lock_type,
        current_locks: dict,
    ):
        """
        Checks if the requested lock can be granted based on existing locks.
        Returns True if the lock can be granted, otherwise returns the transaction holding the conflicting lock.
        """

        # Certify Lock (CL) blocks all other locks
        if current_locks[LockType.CL]:
            return list(current_locks[LockType.CL])[
                0
            ]  # Certify Lock present, return the blocking transaction

        if lock_type == LockType.RL:
            if (
                current_locks[LockType.CL]
                or current_locks[LockType.UL]
                or current_locks[LockType.IUL]
                or current_locks[LockType.ICL]
            ):
                return self._get_first_blocking_transaction(
                    current_locks,
                    [LockType.WL, LockType.UL, LockType.IUL, LockType.IWL],
                )

            return True  # Lock can be granted

        elif lock_type in [LockType.WL, LockType.UL]:
            if (
                current_locks[LockType.WL]
                or current_locks[LockType.CL]
                or current_locks[LockType.UL]
                or current_locks[LockType.IWL]
                or current_locks[LockType.IUL]
                or current_locks[LockType.ICL]
            ):
                return self._get_first_blocking_transaction(current_locks)

            return True  # Lock can be granted

        elif lock_type == LockType.IRL:
            if current_locks[LockType.CL] or current_locks[LockType.UL]:
                return self._get_first_blocking_transaction(current_locks)

            return True  # Lock can be granted

        elif lock_type in [LockType.IWL, LockType.IUL]:
            if (
                current_locks[LockType.WL]
                or current_locks[LockType.CL]
                or current_locks[LockType.UL]
            ):
                return self._get_first_blocking_transaction(current_locks)

            return True  # Lock can be granted

        elif lock_type == LockType.ICL:
            if (
                current_locks[LockType.WL]
                or current_locks[LockType.RL]
                or current_locks[LockType.CL]
                or current_locks[LockType.UL]
            ):
                return self._get_first_blocking_transaction(current_locks)

            return True  # Lock can be granted

        return False  # Lock cannot be granted

    def _get_first_blocking_transaction(self, current_locks, lock_types=None):
        """
        Finds and returns the first transaction holding a conflicting lock.
        """
        if lock_types is None:
            lock_types = [
                LockType.WL,
                LockType.UL,
                LockType.RL,
                LockType.IWL,
                LockType.IUL,
                LockType.IRL,
            ]

        for lock_type in lock_types:
            if current_locks[lock_type]:
                return list(current_locks[lock_type])[
                    0
                ]  # Return the first transaction holding the conflicting lock

        return None

    def _deal_with_deadlock(
        self, transaction: Transaction, blocking_transaction: Transaction
    ):
        if self.await_graph.detect_deadlock():
            print("Deadlock found:\n")
            self.await_graph.display_graph()
            print()
            most_recent_transaction = Transaction.get_most_recent_transaction(
                transaction, blocking_transaction
            )

            most_recent_transaction.abort_transaction()

    def release_lock(self, transaction, node: GranularityGraphNode, lock_type=None):
        """
        Releases a specific lock type or all locks held by the transaction on the node.
        """
        
        if node in transaction.locks_held:
            current_locks = node.locks
            if lock_type:
                # Release the specific lock type if provided
                if lock_type in transaction.locks_held[node]:
                    current_locks[lock_type].discard(transaction)
                    del transaction.locks_held[node]
                    node.remove_lock(lock_type)
            else:
                # Release all locks if no lock type is provided
                for lock_type in current_locks:
                    current_locks[lock_type].discard(transaction)
                    node.remove_lock(transaction, lock_type)
                del transaction.locks_held[node]

    def release_all_locks(self, transaction):
        """
        Releases all locks held by a given transaction across all nodes.
        """

        for node in list(transaction.locks_held.keys()):
            self.release_lock(transaction, node)

    def promote_lock(
        self,
        transaction: Transaction,
        node: GranularityGraphNode,
        new_lock_type: LockType,
    ):
        """
        Promotes the current lock held by the transaction to a more restrictive lock
        """

        current_locks = node.locks

        if node not in transaction.locks_held:
            raise ValueError("Transaction does not hold a lock on this resource.")

        current_lock_type = transaction.locks_held[node]

        Lock.validate_promotion(current_lock_type, new_lock_type)
        
        node.remove_lock(transaction, current_lock_type)
        new_operation = Lock.get_operation_based_on_lock_type(new_lock_type)
        self.request_lock(transaction, node, new_operation)

        # if not Lock.check_conflicting_locks(
        #     current_locks, current_lock_type, new_lock_type
        # ):
        #     return False  # Cannot promote due to conflicting locks
        
        # print(">..")

        # # Remove the current lock and grant the new promoted lock
        # current_locks[current_lock_type].discard(transaction)
        # current_locks[new_lock_type].add(transaction)
        transaction.locks_held[node] = new_lock_type

        node.change_lock(transaction, current_lock_type, new_lock_type)

        return True

    def print_schedule_order(self):
        for transaction, operation in self.operations_order:
            if isinstance(operation, str):
                print(f"Transaction {transaction.transaction_id} - {operation}")
            else:
                print(
                    f"Transaction {transaction.transaction_id} - {operation.operation_type.value} - {operation.node.name}"
                )

    def __repr__(self):
        return f"LockManager({self.locks})"
