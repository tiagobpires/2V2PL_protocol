class Node:
    def __init__(self, name):
        self.name = name
        self.locks = []  
        self.children = []  
        self.parents = [] 

class GranularityGraph:
    def __init__(self):
        self.root = Node("Database")
    
  
    def add_node(self, parent, node):
        parent.children.append(node)  
        node.parents.append(parent)  

graph = GranularityGraph()
tablespace_node = Node("Tablespace/Filegroup")
table_node = Node("Table")
page_node = Node("Page")
tuple_node = Node("Tuple")

graph.add_node(graph.root, tablespace_node)  
graph.add_node(tablespace_node, table_node)  
graph.add_node(table_node, page_node)  
graph.add_node(page_node, tuple_node)  
