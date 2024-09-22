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

    # Deadlock Scenario:
    # Transaction 1 requests a Write Lock (WL) on Area1
    print(f"Transaction {t1.transaction_id} requests Write Lock on Area1")
    assert lock_manager.request_lock(t1, area_node, OperationType.WRITE) == True, "Transaction 1 should acquire Write Lock on Area1"

    # Front propagation should automatically block Page1, Page2, Tuple1, and Tuple2
    print("Verifying front propagation after Transaction 1 acquires Write Lock on Area1...")
    assert LockType.WL in page_node1.locks and t1 in page_node1.locks[LockType.WL], "Page1 should be locked due to front propagation from Area1"
    assert LockType.WL in tuple_node1.locks and t1 in tuple_node1.locks[LockType.WL], "Tuple1 should be locked due to front propagation from Area1"

    # Transaction 2 tries to request a Write Lock (WL) on Page1 (should block due to Transaction 1's lock on Area1)
    print(f"Transaction {t2.transaction_id} requests Write Lock on Page1 (should block)")
    assert lock_manager.request_lock(t2, page_node1, OperationType.WRITE) == False, "Transaction 2 should be blocked due to Transaction 1 holding a Write Lock on Area1 (front propagated to Page1)"

    # Transaction 2 requests a Write Lock (WL) on Tuple2 (should also block due to Transaction 1's lock on Area1)
    print(f"Transaction {t2.transaction_id} requests Write Lock on Tuple2 (should block)")
    assert lock_manager.request_lock(t2, tuple_node2, OperationType.WRITE) == False, "Transaction 2 should be blocked due to Transaction 1 holding a Write Lock on Area1 (front propagated to Tuple2)"

    # Transaction 2 tries to request a Write Lock on Area1 (should block due to Transaction 1's lock)
    print(f"Transaction {t2.transaction_id} requests Write Lock on Area1 (should block)")
    assert lock_manager.request_lock(t2, area_node, OperationType.WRITE) == False, "Transaction 2 should be blocked due to Transaction 1 holding a Write Lock on Area1"

    # Detect deadlock in the system
    print("Detecting deadlock...")
    assert await_graph.detect_deadlock() == True, "Deadlock should be detected between Transaction 1 and Transaction 2"

    print("Deadlock test passed.")

    # Optional: print the state of the graph
    granularity_graph.print_graph()

if __name__ == "__main__":
    main()
