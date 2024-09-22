from modules.lock import LockType
from modules.lock_manager import LockManager
from modules.granularity_graph import GranularityGraph, GranularityGraphNode
from modules.transaction import Transaction
from modules.operation import OperationType


def main():
    # Initialize the granularity graph and lock manager
    granularity_graph = GranularityGraph()
    lock_manager = LockManager(granularity_graph)

    # Create nodes for the database hierarchy
    database_node = granularity_graph.root
    area_node = GranularityGraphNode("Area1")
    table_node = GranularityGraphNode("Table1")
    page_node = GranularityGraphNode("Page1")
    tuple_node = GranularityGraphNode("Tuple1")

    # Add nodes to the graph to build the hierarchy
    granularity_graph.add_node(database_node, area_node)
    granularity_graph.add_node(area_node, table_node)
    granularity_graph.add_node(table_node, page_node)
    granularity_graph.add_node(page_node, tuple_node)

    # Create transactions
    t1 = Transaction(lock_manager)
    t2 = Transaction(lock_manager)

    # Transaction 1 requests a Read Lock (RL) on Table1
    print(f"Transaction {t1.transaction_id} requests Read Lock on Table1")
    assert (
        lock_manager.request_lock(t1, table_node, OperationType.READ) == True
    ), "Transaction 1 should acquire Read Lock on Table1"

    # # Verify front propagation: Tuple1 should also have the Read Lock (RL)
    # print(
    #     f"Transaction {t1.transaction_id} should have a Read Lock on Tuple1 (front propagation)"
    # )
    # assert (
    #     LockType.RL in tuple_node.locks and t1 in tuple_node.locks[LockType.RL]
    # ), "Read Lock should be propagated to Tuple1"

    # Transaction 2 tries to request Write Lock (WL) on Table1 (should fail due to RL by t1)
    print(
        f"Transaction {t2.transaction_id} requests Write Lock on Table1 (should fail)"
    )
    assert (
        lock_manager.request_lock(t2, table_node, OperationType.WRITE) == False
    ), "Transaction 2 should not acquire Write Lock on Table1 due to RL by Transaction 1"

    # Transaction 1 promotes the Read Lock (RL) to Write Lock (WL) on Table1
    print(f"Transaction {t1.transaction_id} promotes Read Lock to Write Lock on Table1")
    assert (
        lock_manager.promote_lock(t1, table_node, LockType.WL) == True
    ), "Transaction 1 should promote Read Lock to Write Lock on Table1"

    # Verify front propagation: Tuple1 should now have the Write Lock (WL)
    print(
        f"Transaction {t1.transaction_id} should have a Write Lock on Tuple1 (front propagation)"
    )
    assert (
        LockType.WL in tuple_node.locks and t1 in tuple_node.locks[LockType.WL]
    ), "Write Lock should be propagated to Tuple1"

    # Transaction 2 tries to request Write Lock on Tuple1 (should fail due to WL by t1)
    print(
        f"Transaction {t2.transaction_id} requests Write Lock on Tuple1 (should fail)"
    )
    assert (
        lock_manager.request_lock(t2, tuple_node, OperationType.WRITE) == False
    ), "Transaction 2 should not acquire Write Lock on Tuple1 due to WL by Transaction 1"

    # Transaction 1 commits and releases locks
    print(f"Transaction {t1.transaction_id} commits and releases all locks")
    t1.commit_transaction()

    # Verify that the locks on Tuple1 and Table1 are released
    print("Verifying that locks are released after commit...")
    assert (
        t1 not in tuple_node.locks[LockType.WL]
    ), "Write Lock should be released on Tuple1"
    assert (
        t1 not in table_node.locks[LockType.WL]
    ), "Write Lock should be released on Table1"

    # Transaction 2 retries Write Lock on Table1 (should succeed now)
    print(
        f"Transaction {t2.transaction_id} retries Write Lock on Table1 (should succeed)"
    )
    assert (
        lock_manager.request_lock(t2, table_node, OperationType.WRITE) == True
    ), "Transaction 2 should acquire Write Lock on Table1 after Transaction 1 commits"

    # Verify front propagation: Tuple1 should now have the Write Lock (WL) for t2
    print(
        f"Transaction {t2.transaction_id} should have a Write Lock on Tuple1 (front propagation)"
    )
    assert (
        LockType.WL in tuple_node.locks and t2 in tuple_node.locks[LockType.WL]
    ), "Write Lock should be propagated to Tuple1 for Transaction 2"

    print("All tests passed!")


if __name__ == "__main__":
    main()
