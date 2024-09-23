from modules.lock import LockType
from modules.lock_manager import LockManager
from modules.granularity_graph import GranularityGraph, GranularityGraphNode
from modules.transaction import Transaction
from modules.operation import OperationType
from modules.await_graph import Graph


def main():
    # Initialize the granularity graph and lock manager
    granularity_graph = GranularityGraph()
    await_graph = Graph()
    lock_manager = LockManager(granularity_graph, await_graph)

    # Create nodes for the database hierarchy
    database_node = granularity_graph.root
    area_node = GranularityGraphNode("Area1")
    table_node = GranularityGraphNode("Table1")
    page_node1 = GranularityGraphNode("Page1")
    page_node2 = GranularityGraphNode("Page2")
    tuple_node1 = GranularityGraphNode("Tuple1")
    tuple_node2 = GranularityGraphNode("Tuple2")

    # Add nodes to the graph to build the hierarchy
    granularity_graph.add_node(database_node, area_node)
    granularity_graph.add_node(area_node, table_node)
    granularity_graph.add_node(table_node, page_node1)
    granularity_graph.add_node(table_node, page_node2)
    granularity_graph.add_node(page_node1, tuple_node1)
    granularity_graph.add_node(page_node2, tuple_node2)

    # Create transactions
    t1 = Transaction(lock_manager, await_graph)
    t2 = Transaction(lock_manager, await_graph)

    # Define an input order for operations
    input_operations = [
        (t1, tuple_node1, OperationType.WRITE),
        (t2, tuple_node2, OperationType.WRITE),
        (t1, tuple_node2, OperationType.WRITE),
        (t1, page_node1, OperationType.READ),
        (t1, page_node1, OperationType.WRITE),
        (t2, tuple_node1, OperationType.WRITE),
    ]

    print("\nInput order of operations:")
    for t, node, op_type in input_operations:
        print(f"Transaction {t.transaction_id}: {op_type.name} on {node.name}")

    # Track the real order of execution, including aborts
    executed_operations = []

    print("\nStarting operations:")
    for t, node, op_type in input_operations:
        print(
            f"Transaction {t.transaction_id} is attempting {op_type.name} on {node.name}"
        )
        t.create_operation(node, op_type)

        if t.state == "active":  # Operation successfully executed
            executed_operations.append((t, node, op_type, "executed"))
        elif t.state == "aborted":  # Transaction was aborted
            print(
                f"Transaction {t.transaction_id} was aborted while trying {op_type.name} on {node.name}"
            )
            executed_operations.append((t, node, op_type, "aborted"))
        else:
            print(
                f"Transaction {t.transaction_id} is blocked on {node.name} for {op_type.name}"
            )
            executed_operations.append((t, node, op_type, "blocked"))

    # Print the real order of executed operations, including aborts
    print("\nReal order of executed operations:")
    lock_manager.print_schedule_order()

    # Print the final state of the graph and locks
    print("\nFinal state of the wait-for graph:")
    await_graph.display_graph()

    print("\nFinal state of the granularity graph:")
    granularity_graph.print_graph()


if __name__ == "__main__":
    main()
