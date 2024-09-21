from modules.lock import LockType


class LockManager:
    def __init__(self):
        """
        Initializes the lock manager to track locks on resources with multiple levels of granularity.
        """
        # Dicionário que mapeia recursos para seus bloqueios (com granularidade múltipla)
        self.locks = {}

    def _initialize_resource(self, resource):
        """Helper function to initialize the lock entry for a resource."""
        if resource not in self.locks:
            self.locks[resource] = {
                LockType.IRL: set(),
                LockType.IWL: set(),
                LockType.IUL: set(),
                LockType.R: set(),
                LockType.W: set(),
                LockType.U: set(),
                LockType.CL: set(),
            }

    def request_lock(self, transaction, resource, lock_type: LockType):
        """
        Requests a lock of the given type (IRL, IWL, IUL, R, W, U, CL) for the resource.
        """
        self._initialize_resource(resource)
        current_locks = self.locks[resource]

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
        if lock_type in [LockType.IRL, LockType.IWL, LockType.IUL, LockType.S, LockType.W, LockType.U]:
            if lock_type == LockType.R:
                if not (current_locks[LockType.W] or current_locks[LockType.U] or current_locks[LockType.IUL] or current_locks[LockType.IWL]):
                    current_locks[LockType.R].add(transaction)
                    transaction.locks_held[resource] = LockType.R
                    return True
            elif lock_type == LockType.W:
                if not any(current_locks.values()):  # Nenhum outro bloqueio permitido para W
                    current_locks[LockType.W].add(transaction)
                    transaction.locks_held[resource] = LockType.W
                    return True
            elif lock_type == LockType.U:
                if not current_locks[LockType.W] and not current_locks[LockType.U]:
                    current_locks[LockType.U].add(transaction)
                    transaction.locks_held[resource] = LockType.U
                    return True
            elif lock_type == LockType.IRL:
                if not (current_locks[LockType.IWL] or current_locks[LockType.IUL] or current_locks[LockType.W]):
                    current_locks[LockType.IRL].add(transaction)
                    transaction.locks_held[resource] = LockType.IRL
                    return True
            elif lock_type == LockType.IWL:
                if not (current_locks[LockType.IRL] or current_locks[LockType.IUL] or current_locks[LockType.R] or current_locks[LockType.U]):
                    current_locks[LockType.IWL].add(transaction)
                    transaction.locks_held[resource] = LockType.IWL
                    return True
            elif lock_type == LockType.IUL:
                if not (current_locks[LockType.W] or current_locks[LockType.R] or current_locks[LockType.IRL]):
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

    def __repr__(self):
        return f"LockManager({self.locks})"
