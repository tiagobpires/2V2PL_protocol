from modules.lock import Lock, LockType


class GranularityGraphNode:
    def __init__(self, name, is_root=False):
        self.name = name
        self.locks = {lock_type: set() for lock_type in LockType}  # Initialize locks
        self.children = []
        self.parent = None
        self.is_root = is_root

    def add_lock(self, transaction, lock_type):
        """
        Adds a lock to this node and propagates the change.
        """

        self.locks[lock_type].add(transaction)
        self.backpropagate_intention_locks(transaction, self.parent, lock_type)
        self.front_propagate_locks(transaction, self, lock_type)

    def change_lock(self, transaction, lock_type, new_lock_type):
        """
        Changes the lock type for a transaction in this node and propagates the change.
        """

        self.remove_lock(transaction, lock_type)
        self.add_lock(transaction, new_lock_type)

    def remove_lock(self, transaction, lock_type):
        """
        Removes a lock from this node and propagates the removal.
        """
        
        self.locks[lock_type].discard(transaction)

        self.remove_intention_locks(transaction, self.parent, lock_type)
        self.front_remove_locks(transaction, self, lock_type)

    def backpropagate_intention_locks(self, transaction, node, lock_type):
        """
        Backpropagates intention locks up the hierarchy.
        """

        if node is None:
            return

        if lock_type == LockType.RL:
            intention_lock = LockType.IRL
        elif lock_type == LockType.WL:
            intention_lock = LockType.IWL
        elif lock_type == LockType.UL:
            intention_lock = LockType.IUL
        elif lock_type == LockType.CL:
            intention_lock = LockType.ICL
        else:
            return  # No backpropagation needed for this lock type

        if transaction not in node.locks[intention_lock]:
            node.locks[intention_lock].add(transaction)

        # Add the intention lock to the parent if it's not the root
        if not node.is_root:
            self.backpropagate_intention_locks(transaction, node.parent, lock_type)

    def front_propagate_locks(self, transaction, node, lock_type):
        """
        Propagates locks down the hierarchy (to children).
        """

        for child in node.children:
            if transaction not in child.locks[lock_type]:
                child.locks[lock_type].add(transaction)

            # Recursive call to propagate to all descendants
            self.front_propagate_locks(transaction, child, lock_type)

    def remove_intention_locks(self, transaction, node, lock_type):
        """
        Removes intention locks up the hierarchy if no more locks exist.
        """

        if node is None:
            return

        if lock_type == LockType.RL:
            intention_lock = LockType.IRL
        elif lock_type == LockType.WL:
            intention_lock = LockType.IWL
        elif lock_type == LockType.UL:
            intention_lock = LockType.IUL
        elif lock_type == LockType.CL:
            intention_lock = LockType.ICL
        else:
            return

        # Remove the intention lock if the transaction holds it
        node.locks[intention_lock].discard(transaction)

        # Always propagate intention removal upwards
        if not node.is_root:
            self.remove_intention_locks(transaction, node.parent, lock_type)

    def front_remove_locks(self, transaction, node, lock_type):
        """
        Propagates lock removal down the hierarchy (to children).
        """

        for child in node.children:
            child.locks[lock_type].discard(transaction)

            # Recursive call to propagate removal to all descendants
            self.front_remove_locks(transaction, child, lock_type)

    def __repr__(self) -> str:
        return f"GranularityGraphNode({self.name})"

class GranularityGraph:
    def __init__(self):
        self.root = GranularityGraphNode("Database", is_root=True)

    def add_node(self, parent, node):
        parent.children.append(node)
        node.parent = parent

    def print_graph(self, node=None, level=0):
        """
        Prints the graph hierarchy starting from the given node.
        """
        if node is None:
            node = self.root  # Start from the root if no node is provided

        indent = "  " * level
        print(f"{indent}- {node.name} (Locks: {self._format_locks(node.locks)})")

        for child in node.children:
            self.print_graph(child, level + 1)

    def _format_locks(self, locks):
        """
        Helper method to format the locks of a node for printing.
        """
        formatted_locks = []
        for lock_type, transactions in locks.items():
            if transactions:
                formatted_locks.append(f"{lock_type.name}: {len(transactions)}")
        return ", ".join(formatted_locks) if formatted_locks else "No Locks"


if __name__ == "__main__":
    graph = GranularityGraph()
    tablespace_node = GranularityGraphNode("Tablespace 1")
    table_node = GranularityGraphNode("Table 1")
    page_node = GranularityGraphNode("Page 1")
    tuple_node = GranularityGraphNode("X")

    graph.add_node(graph.root, tablespace_node)
    graph.add_node(tablespace_node, table_node)
    graph.add_node(table_node, page_node)
    graph.add_node(page_node, tuple_node)
