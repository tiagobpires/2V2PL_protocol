class LockManager:
    def __init__(self):
        """
        Initializes the lock manager to track locks on resources with multiple levels of granularity.
        """
        # Dictionary to track locks on resources with multiple granularity levels
        self.locks = {}

    def _initialize_resource(self, resource):
        """Helper function to initialize the lock entry for a resource."""
        if resource not in self.locks:
            self.locks[resource] = {
                "IRL": set(),  # Read Intention
                "IWL": set(),  # Write Intention
                "IUL": set(),  # Update Intention
                "R": set(),  # Read
                "W": set(),  # Write
                "U": set(),  # Update
                "CL": set(),  # Certify Lock
            }

    def request_lock(self, transaction, resource, lock_type):
        """
        Requests a lock of the given type (IRL, IWL, IUL, S, X, U, CL) for the resource.
        """
        self._initialize_resource(resource)
        current_locks = self.locks[resource]

        # Certify Lock can only be granted if no other locks are held on the resource
        if lock_type == "CL":
            if any(current_locks.values()):
                return False  # Cannot acquire CL if any other lock is held
            current_locks["CL"].add(transaction)
            transaction.locks_held[resource] = "CL"
            return True

        # If there is already a Certify Lock (CL), no other lock can be granted
        if current_locks["CL"]:
            return False

        # Handle Intention Locks and Regular Locks
        if lock_type in ["IRL", "IWL", "IUL", "S", "X", "U"]:
            # Implement logic for checking and granting locks based on current state
            if lock_type == "S":
                if (
                    not current_locks["X"]
                    and not current_locks["U"]
                    and not current_locks["IUL"]
                    and not current_locks["IWL"]
                ):
                    current_locks["S"].add(transaction)
                    transaction.locks_held[resource] = "S"
                    return True
            elif lock_type == "X":
                if not any(current_locks.values()):  # No other locks allowed for X
                    current_locks["X"].add(transaction)
                    transaction.locks_held[resource] = "X"
                    return True
            elif lock_type == "U":
                if not current_locks["X"] and not current_locks["U"]:
                    current_locks["U"].add(transaction)
                    transaction.locks_held[resource] = "U"
                    return True
            elif lock_type == "IRL":
                if (
                    not current_locks["IWL"]
                    and not current_locks["IUL"]
                    and not current_locks["X"]
                ):
                    current_locks["IRL"].add(transaction)
                    transaction.locks_held[resource] = "IRL"
                    return True
            elif lock_type == "IWL":
                if (
                    not current_locks["IRL"]
                    and not current_locks["IUL"]
                    and not current_locks["S"]
                    and not current_locks["U"]
                ):
                    current_locks["IWL"].add(transaction)
                    transaction.locks_held[resource] = "IWL"
                    return True
            elif lock_type == "IUL":
                if (
                    not current_locks["X"]
                    and not current_locks["S"]
                    and not current_locks["IRL"]
                ):
                    current_locks["IUL"].add(transaction)
                    transaction.locks_held[resource] = "IUL"
                    return True

        return False  # Lock request failed

    def release_lock(self, transaction, resource, lock_type=None):
        """
        Releases a specific lock type or all locks held by the transaction on the resource.
        """
        if resource in transaction.locks_held:
            current_locks = self.locks[resource]
            if lock_type:
                # Release the specific lock type if provided
                if lock_type in transaction.locks_held[resource]:
                    current_locks[lock_type].discard(transaction)
                    del transaction.locks_held[resource]
            else:
                # Release all locks if no specific lock type is provided
                for lt in current_locks:
                    current_locks[lt].discard(transaction)
                del transaction.locks_held[resource]

    def release_all_locks(self, transaction):
        """
        Releases all locks held by a given transaction across all resources.
        """
        for resource in list(transaction.locks_held.keys()):
            self.release_lock(transaction, resource)
