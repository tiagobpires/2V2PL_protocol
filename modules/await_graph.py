from queue import SimpleQueue


class Graph:
    def __init__(self):
        self.vertices = {}

    def add_vertex(self, transaction):
        """
        Adds a vertex to the graph.
        """

        self.vertices[transaction.transaction_id] = {
            "transaction": transaction,
            "edges": [],
        }

    def add_edge(self, source, destination):
        """
        Adds a directed edge from source to destination.
        """

        if source == destination:
            return False

        self.vertices[source]["edges"].append(destination)
        return True

    def remove_edge(self, source, destination):
        """
        Removes a directed edge from source to destination.
        """

        self.vertices[source]["edges"].remove(destination)

    def display_graph(self):
        """
        Prints the graph vertices and their edges.
        """
        for vertex, data in self.vertices.items():
            neighbors = data["edges"]
            if neighbors:
                neighbors_list = ", ".join(map(str, neighbors))
                print(f"Vertex {vertex} -> [{neighbors_list}]")
            else:
                print(f"Vertex {vertex} has no edges.")

        if not self.vertices:
            print("The graph is empty.")

    def find_vertex(self, name):
        """
        Checks if a vertex exists in the graph.
        """

        return name in self.vertices

    def detect_deadlock(self):
        """
        Detects if there's a cycle (deadlock) in the wait-for graph using DFS for all vertices.
        """
        visited = {v: False for v in self.vertices}  # Track visited nodes
        rec_stack = {v: False for v in self.vertices}  # Track recursion stack

        # Helper function for DFS
        def dfs(v):
            visited[v] = True
            rec_stack[v] = True

            # Recur for all neighbors
            for neighbor in self.vertices[v]["edges"]:
                if not visited[neighbor]:
                    if dfs(neighbor):
                        return True
                elif rec_stack[neighbor]:
                    # If the neighbor is in the recursion stack, we found a cycle (deadlock)
                    return True

            rec_stack[v] = False
            return False

        # Call DFS for each vertex
        for vertex in self.vertices:
            if not visited[vertex]:
                if dfs(vertex):
                    return True

        return False

    def get_waiting_transactions(self, transaction_id):
        """
        Returns a list of transactions that are waiting for the given transaction_id.
        """

        waiting_transactions = []

        for vertex, data in self.vertices.items():
            if transaction_id in data["edges"]:
                waiting_transactions.append((vertex, data))

        return waiting_transactions


if __name__ == "__main__":
    # Example usage
    g = Graph()

    g.add_vertex("A")
    g.add_vertex("B")
    g.add_vertex("C")

    g.add_edge("A", "B")
    g.add_edge("A", "C")
    g.add_edge("B", "C")
    # g.add_edge("C", "A")

    g.display_graph()

    if g.has_cycle():
        print("Cycle detected!")
    else:
        print("No cycle detected.")
