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
        Adds a lock to this node.
        """

        self.locks[lock_type].add(transaction)

    def remove_lock(self, transaction, lock_type):
        """
        Removes a lock from this node.
        """

        self.locks[lock_type].discard(transaction)


class GranularityGraph:
    def __init__(self):
        self.root = GranularityGraphNode("Database", is_root=True)

    def add_node(self, parent, node):
        parent.children.append(node)
        node.parent = parent

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
            node.add_lock(transaction, intention_lock)

        # Add the intention lock to the parent
        if not node.is_root:
            # Recursive call to propagate to higher levels
            self.backpropagate_intention_locks(transaction, node.parent, intention_lock)

    def front_propagate_locks(self, transaction, node, lock_type):
        """
        Propagates locks down the hierarchy (to children).
        """

        for child in node.children:

            # If the lock type is RL/WL/CL, propagate RL/WL/CL to children
            if lock_type in [LockType.RL, LockType.WL, LockType.CL, LockType.UL]:
                if transaction not in child.locks[lock_type]:
                    child.add_lock(transaction, lock_type)

                # Recursive call to propagate to all descendants
                self.front_propagate_locks(transaction, child, lock_type)

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
