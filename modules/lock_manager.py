from modules.lock import LockType, Lock
from modules.transaction import Transaction


class LockManager:
    def __init__(self):
        """
        Initializes the lock manager to track locks on resources with multiple levels of granularity.
        """

        # {resource: {lock_type: {transactions}}}
        self.locks = {}

    def _initialize_resource(self, resource):
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

    def request_lock(self, transaction: Transaction, resource: str, operation):
        """
        Requests a lock for the resource.
        """

        lock_type = Lock.get_lock_type_based_on_operation(operation)

        self._initialize_resource(resource)
        current_locks = self.locks[resource]

        if not any(current_locks.values()):
            current_locks[lock_type].add(transaction)
            transaction.locks_held[resource] = lock_type

            return True

        # Certify Lock (CL) só pode ser garantido se não houver outros bloqueios no recurso
        if lock_type == LockType.CL:
            if any(current_locks.values()):
                return False  # Não pode adquirir CL se há qualquer outro bloqueio

            current_locks[LockType.CL].add(transaction)
            transaction.locks_held[resource] = LockType.CL

            return True

        # Se já há um Certify Lock (CL), nenhum outro bloqueio pode ser garantido
        if current_locks[LockType.CL]:
            return False

        # Lidar com Intention Locks e bloqueios regulares
        if lock_type in [
            LockType.IRL,
            LockType.IWL,
            LockType.IUL,
            LockType.RL,
            LockType.WL,
            LockType.UL,
        ]:
            if lock_type == LockType.RL:
                if not (
                    current_locks[LockType.WL]
                    or current_locks[LockType.UL]
                    or current_locks[LockType.IUL]
                    or current_locks[LockType.IWL]
                ):
                    current_locks[LockType.RL].add(transaction)
                    transaction.locks_held[resource] = LockType.RL
                    return True

            elif lock_type == LockType.WL:
                if not any(
                    current_locks.values()
                ):  # Nenhum outro bloqueio permitido para W
                    current_locks[LockType.WL].add(transaction)
                    transaction.locks_held[resource] = LockType.WL
                    return True
            elif lock_type == LockType.UL:
                if not current_locks[LockType.WL] and not current_locks[LockType.UL]:
                    current_locks[LockType.UL].add(transaction)
                    transaction.locks_held[resource] = LockType.UL
                    return True
            elif lock_type == LockType.IRL:
                if not (
                    current_locks[LockType.IWL]
                    or current_locks[LockType.IUL]
                    or current_locks[LockType.WL]
                ):
                    current_locks[LockType.IRL].add(transaction)
                    transaction.locks_held[resource] = LockType.IRL
                    return True
            elif lock_type == LockType.IWL:
                if not (
                    current_locks[LockType.IRL]
                    or current_locks[LockType.IUL]
                    or current_locks[LockType.RL]
                    or current_locks[LockType.UL]
                ):
                    current_locks[LockType.IWL].add(transaction)
                    transaction.locks_held[resource] = LockType.IWL
                    return True
            elif lock_type == LockType.IUL:
                if not (
                    current_locks[LockType.WL]
                    or current_locks[LockType.RL]
                    or current_locks[LockType.IRL]
                ):
                    current_locks[LockType.IUL].add(transaction)
                    transaction.locks_held[resource] = LockType.IUL
                    return True

        return False  # Falha na requisição de bloqueio

    def release_lock(self, transaction, resource, lock_type=None):
        """
        Releases a specific lock type or all locks held by the transaction on the resource.
        """
        if resource in transaction.locks_held:
            current_locks = self.locks[resource]
            if lock_type:
                # Libera o tipo de bloqueio específico se fornecido
                if lock_type in transaction.locks_held[resource]:
                    current_locks[lock_type].discard(transaction)
                    del transaction.locks_held[resource]
            else:
                # Libera todos os bloqueios se nenhum tipo de bloqueio for fornecido
                for lt in current_locks:
                    current_locks[lt].discard(transaction)
                del transaction.locks_held[resource]

    def release_all_locks(self, transaction):
        """
        Releases all locks held by a given transaction across all resources.
        """
        for resource in list(transaction.locks_held.keys()):
            self.release_lock(transaction, resource)

    def promote_lock(
        self, transaction: "Transaction", resource: str, new_lock_type: LockType
    ):
        """
        Promotes the current lock held by the transaction to a more restrictive lock, including handling Intention Locks and Certify Lock.
        """

        self._initialize_resource(resource)
        current_locks = self.locks[resource]

        if resource not in transaction.locks_held:
            raise ValueError("Transaction does not hold a lock on this resource.")

        current_lock_type = transaction.locks_held[resource]

        Lock.validate_promotion(current_lock_type, new_lock_type)

        if not Lock.check_conflicting_locks(
            current_locks, current_lock_type, new_lock_type
        ):
            return False  # Cannot promote due to conflicting locks

        # Remove the current lock and grant the new promoted lock
        current_locks[current_lock_type].discard(transaction)
        current_locks[new_lock_type].add(transaction)
        transaction.locks_held[resource] = new_lock_type

        return True

    def __repr__(self):
        return f"LockManager({self.locks})"
