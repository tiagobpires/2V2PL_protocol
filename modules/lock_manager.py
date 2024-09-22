from modules.lock import LockType, Lock
from modules.transaction import Transaction
from modules.granularity_graph import GranularityGraph, GranularityGraphNode


class LockManager:
    def __init__(self, granularity_graph: GranularityGraph):
        """
        Initializes the lock manager to track locks on resources with multiple levels of granularity.
        """

        self.granularity_graph = granularity_graph

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
        Requests a lock for the resource.
        """

        lock_type = Lock.get_lock_type_based_on_operation(operation)
        current_locks = node.locks

        if not any(current_locks.values()):
            current_locks[lock_type].add(transaction)
            transaction.locks_held[node] = lock_type
            node.add_lock(transaction, lock_type)

            return True

        # Certify Lock (CL) só pode ser garantido se não houver outros bloqueios no recurso
        if lock_type == LockType.CL:
            if any(current_locks.values()):
                return False  # Não pode adquirir CL se há qualquer outro bloqueio

            current_locks[LockType.CL].add(transaction)
            transaction.locks_held[node] = LockType.CL
            node.add_lock(transaction, lock_type)

            return True

        # Handle conflicting locks and intention locks
        if lock_type in [
            LockType.IRL,
            LockType.IWL,
            LockType.IUL,
            LockType.RL,
            LockType.WL,
            LockType.UL,
        ]:
            if self._can_grant_lock(lock_type, current_locks):
                current_locks[lock_type].add(transaction)
                transaction.locks_held[node] = lock_type
                node.add_lock(transaction, lock_type)
                return True

        return False  # Failed to acquire the requested lock

    def _can_grant_lock(
        self,
        lock_type: LockType,
        current_locks: dict,
    ):
        """
        Checks if the requested lock can be granted based on existing locks.
        """
        if lock_type == LockType.RL:
            if not (
                current_locks[LockType.WL]
                or current_locks[LockType.UL]
                or current_locks[LockType.IUL]
                or current_locks[LockType.IWL]
            ):
                return True
        elif lock_type == LockType.WL:
            if not any(current_locks.values()):  # No other locks allowed for WL
                return True
        elif lock_type == LockType.UL:
            if not current_locks[LockType.WL] and not current_locks[LockType.UL]:
                return True
        elif lock_type in [LockType.IRL, LockType.IWL, LockType.IUL]:
            # Intention locks can be granted if no conflicting locks exist
            return True

        return False

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

        if not Lock.check_conflicting_locks(
            current_locks, current_lock_type, new_lock_type
        ):
            return False  # Cannot promote due to conflicting locks

        # Remove the current lock and grant the new promoted lock
        current_locks[current_lock_type].discard(transaction)
        current_locks[new_lock_type].add(transaction)
        transaction.locks_held[node] = new_lock_type

        node.change_lock(transaction, current_lock_type, new_lock_type)

        return True

    def __repr__(self):
        return f"LockManager({self.locks})"
