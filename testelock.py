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

    t1.create_operation(tuple_node1, OperationType.WRITE)
    t2.create_operation(tuple_node2, OperationType.WRITE)
    t1.create_operation(tuple_node2, OperationType.WRITE)
    t1.create_operation(page_node1, OperationType.READ)
    t2.create_operation(tuple_node1, OperationType.WRITE)   

    await_graph.display_graph()


if __name__ == "__main__":
    main()
