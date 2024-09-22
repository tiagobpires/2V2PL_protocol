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

    # Transaction 1 requests a Write Lock (WL) on Area1
    print(f"Transaction {t1.transaction_id} requests Write Lock on Area1")
    assert lock_manager.request_lock(t1, area_node, OperationType.WRITE) == True, "Transaction 1 should acquire Write Lock on Area1"
   
   
    # Transaction 2 requests a Write Lock (WL) on Area1 (should block)
    print(f"Transaction {t2.transaction_id} requests Write Lock on Area1 (should block)")
    assert lock_manager.request_lock(t2, area_node, OperationType.WRITE) == False, "Transaction 2 should be blocked due to Transaction 1 holding a Write Lock on Area1"
   
    
    # Transaction 2 tries to request a Write Lock (WL) on Page1
    print(f"Transaction {t2.transaction_id} requests Write Lock on Page1 (should block due to Transaction 1's lock on Area1)")
    assert lock_manager.request_lock(t2, page_node1, OperationType.WRITE) == False, "Transaction 2 should be blocked due to Transaction 1's lock on Area1"
   
    # Now, Transaction 1 requests a Write Lock on Page1 (should block due to Transaction 2)
    print(f"Transaction {t1.transaction_id} requests Write Lock on Page1 (should block)")
    assert lock_manager.request_lock(t1, page_node1, OperationType.WRITE) == False, "Transaction 1 should be blocked by Transaction 2 holding a lock on Page1"
    await_graph.display_graph()
    return
  
  
    # Detect deadlock in the system
    print("Detecting deadlock...p")
    granularity_graph.print_graph()
    assert await_graph.detect_deadlock() == True, "Deadlock should be detected between Transaction 1 and Transaction 2"
   
    print("Deadlock test passed.")

    # Optional: print the state of the graph
    granularity_graph.print_graph()



if __name__ == "__main__":
    main()
