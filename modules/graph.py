from queue import SimpleQueue


class Graph:
    def __init__(self):
        self.vertices = {}

    def add_vertex(self, name):
        """
        Adds a vertex to the graph.
        """

        self.vertices[name] = []

    def add_edge(self, source, destination):
        """
        Adds a directed edge from source to destination.
        """

        self.vertices[source].append(destination)

    def remove_edge(self, source, destination):
        """
        Removes a directed edge from source to destination.
        """

        self.vertices[source].remove(destination)

    def display_graph(self):
        """
        Prints the graph vertices and their edges.
        """

        for vertex, neighbors in self.vertices.items():
            if neighbors:
                neighbors_list = ", ".join(neighbors)
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

    def has_cycle(self):
        """
        Detects if there's a cycle in the graph using BFS for all vertices.
        """

        visited = {v: False for v in self.vertices}  # Track visited nodes
        parent = {v: None for v in self.vertices}  # Track parent nodes

        for vertex in self.vertices:
            if not visited[vertex]:
                q = SimpleQueue()
                q.put(vertex)
                visited[vertex] = True

                while not q.empty():
                    current = q.get()

                    for neighbor in self.vertices[current]:
                        if not visited[neighbor]:
                            q.put(neighbor)
                            visited[neighbor] = True
                            parent[neighbor] = current

                        elif parent[current] != neighbor:
                            # A cycle is detected if a visited neighbor is not the parent of the current node
                            return True

        return False


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
