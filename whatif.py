class QueryNode:
    _id_counter = 1  # Static counter to uniquely identify nodes
    
    def __init__(self, node_type, value=None):
        self.node_type = node_type  
        self.value = value 
        self.children = []
        self.id = QueryNode._id_counter  # Unique ID for each node
        QueryNode._id_counter += 1

    def add_child(self, child_node):
        self.children.append(child_node)

    def __repr__(self, level=0):
        indent = "  " * level
        repr_str = f"{indent}{self.node_type}: {self.value} (ID: {self.id})\n"
        for child in self.children:
            repr_str += child.__repr__(level + 1)
        return repr_str

def build_query_tree(query_dict):
    # Root project then join
    root=QueryNode("Operation",query_dict["operation"])
    joins=[]
    ## Build the tree bottom up (start with the source) (On^2)
    for i in range(len(query_dict["source"])):
        ## define the source
        projections=[]
        source_alias=query_dict["source"][i]["alias"]
        source_node=QueryNode("Source",source_alias)

        # Check for the joins and the respective column, append the columns that need to be projected and put on the projections array
        for k in range(len(query_dict["joins"])):
            join_alias_1=query_dict["joins"][k][0]["alias"]
            join_alias_2=query_dict["joins"][k][1]["alias"]
            if(source_alias==join_alias_1):
                join=join_alias_1 + "." + query_dict["joins"][k][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][k][1]["on"]
                projections.append(query_dict["joins"][k][0]["on"])
            elif(source_alias==join_alias_2):
                join=join_alias_1 + "." + query_dict["joins"][k][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][k][1]["on"]
                projections.append(query_dict["joins"][k][1]["on"])
            join_node=QueryNode("Join",join)
            
            projections_node=None
            # Check for the projections and try to find out which variables need to be projected
            for j in range(len(query_dict["projections"])):
                projections_alias=query_dict["projections"][j][0]
                if(source_alias == projections_alias):
                    projections.append(query_dict["projections"][j].split(".")[-1])
                    projections_node=QueryNode("Projection",projections)
                    projections_node.add_child(source_node)
                    # Join the projections node if there is
                    join_node.add_child(projections_node)
            if(projections_node==None):
                join_node.add_child(source_node)
            
            
        root.add_child(join_node)
    return root

def get_nodes_and_edges(node):
    nodes = []
    edges = []

    # Recursive helper function to collect nodes and edges
    def traverse(node):
        nodes.append((node.id, node.node_type, node.value))
        for child in node.children:
            edges.append((node.id, child.id))
            traverse(child)
    
    traverse(node)
    return nodes, edges

# Test input JSON (as a dictionary for Python)
query_input = {
    "operation": "SELECT + Join",
    ## Indicates which alisa to project
    "projections": [
        "C.name",
        "O.id"
    ],
    ## each item indicates a kind of join
    "source": [
        {
            "table": "customer",
            "alias": "C",
        },
        {
            "table": "orders",
            "alias": "O",
        }
    ],
    "joins":[
        [
        {   "table": "customer",
            "alias":"C",
            "on":"c_custkey"
        },
        {   "table": "orders",
            "alias":"O",
            "on":"o_custkey"
        }]
    ]
}

# Build the query tree
query_tree = build_query_tree(query_input)

# Retrieve nodes and edges
nodes, edges = get_nodes_and_edges(query_tree)

# Print the query tree structure
print("Query Tree Structure:")
print(query_tree)

# Print the nodes and edges
print("\nNodes:")
for node_id, node_type, value in nodes:
    print(f"Node ID: {node_id}, Type: {node_type}, Value: {value}")

print("\nEdges:")
for parent_id, child_id in edges:
    print(f"Parent ID: {parent_id} -> Child ID: {child_id}")

import tkinter as tk

class TreeVisualizer:
    def __init__(self, root, nodes, edges):
        self.root = root
        self.nodes = nodes
        self.edges = edges
        self.canvas = tk.Canvas(root, width=600, height=400, bg='white')
        self.canvas.pack()
        
        self.node_positions = {}  # Store the positions of nodes for edge drawing
        self.create_tree_visualization()
        
    def create_tree_visualization(self):
        # Define starting point for root node
        start_x = 300
        start_y = 50
        # level_height = 80

        # Draw the nodes and store their positions
        self.draw_node(self.nodes[0], start_x, start_y)  # Start with the root node

        # Draw the edges based on the nodes' positions
        for edge in self.edges:
            parent_id, child_id = edge
            parent_pos = self.node_positions[parent_id]
            child_pos = self.node_positions[child_id]
            self.canvas.create_line(parent_pos[0], parent_pos[1] + 20, 
                                    child_pos[0], child_pos[1] - 20, arrow=tk.LAST)

    def draw_node(self, node, x, y):
        node_id, node_type, value = node

        # Draw the node as a rectangle with text
        text = f"{node_type}: {value}"
        node_width = max(100, len(text) * 7)
        rect = self.canvas.create_rectangle(x - node_width // 2, y - 20,
                                            x + node_width // 2, y + 20, fill="lightblue")
        self.canvas.create_text(x, y, text=text, font=("Arial", 10))

        # Store the position of this node
        self.node_positions[node_id] = (x, y)

        # Get children of this node
        children = [edge[1] for edge in self.edges if edge[0] == node_id]
        
        # Draw each child node, spacing them out horizontally
        child_x = x - (len(children) - 1) * 120  # Starting x position for child nodes
        for child_id in children:
            child_node = next(n for n in self.nodes if n[0] == child_id)
            self.draw_node(child_node, child_x, y + 80)  # Adjust y for next level
            child_x += 240  # Space out child nodes horizontally

# Define nodes and edges as per the tree structure
nodes, edges = get_nodes_and_edges(query_tree)

# Set up the Tkinter window and visualize the tree
root = tk.Tk()
root.title("Query Tree Visualization")
visualizer = TreeVisualizer(root, nodes, edges)
root.mainloop()