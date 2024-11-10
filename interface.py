
import tkinter as tk
from whatif import get_nodes_and_edges,build_query_tree
from example import query_input,query_input_2,query_input_3
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
            self.canvas.create_line(parent_pos[0], parent_pos[1] + 10, 
                                    child_pos[0], child_pos[1] - 10, arrow=tk.LAST)

    def draw_node(self, node, x, y):
        node_id, node_type, value = node

        # Draw the node as a rectangle with text
        text = f"{node_type}: {value}"
        node_width = max(100, len(text) * 7)
        rect = self.canvas.create_rectangle(x - node_width // 2, y - 10,
                                            x + node_width // 2, y + 10, fill="lightblue")
        self.canvas.create_text(x, y, text=text, font=("Arial", 10))

        # Store the position of this node
        self.node_positions[node_id] = (x, y)

        # Get children of this node
        children = [edge[1] for edge in self.edges if edge[0] == node_id]
        
        # Draw each child node, spacing them out horizontally
        child_x = x - (len(children) - 1) * 120  
        for child_id in children:
            child_node = next(n for n in self.nodes if n[0] == child_id)
            self.draw_node(child_node, child_x, y + 80)  
            child_x += 240 

# Define nodes and edges as per the tree structure

query_tree = build_query_tree(query_input)
nodes, edges = get_nodes_and_edges(query_tree)

# Set up the Tkinter window and visualize the tree
root = tk.Tk()
root.title("Query Tree Visualization")
visualizer = TreeVisualizer(root, nodes, edges)
root.mainloop()