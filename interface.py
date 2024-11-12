import tkinter as tk
from tkinter import ttk
from whatif import get_nodes_and_edges, build_query_tree
from example import query_input, query_input_2, query_input_3, query_input_4

class TreeVisualizer:
    def __init__(self, root, nodes, edges):
        self.root = root
        self.nodes = nodes
        self.edges = edges

        # Set the default window size to 700x500
        self.root.geometry("1000x1000")

        # Create a frame to hold the canvas and scrollbars
        self.frame = tk.Frame(root)
        self.frame.pack(fill=tk.BOTH, expand=True)

        # Create a canvas with scrollbars
        self.canvas = tk.Canvas(self.frame, bg='white', scrollregion=(0, 0, 1600, 1400))
        self.h_scrollbar = ttk.Scrollbar(self.frame, orient="horizontal", command=self.canvas.xview)
        self.v_scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(xscrollcommand=self.h_scrollbar.set, yscrollcommand=self.v_scrollbar.set)

        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.h_scrollbar.grid(row=1, column=0, sticky="ew")
        self.v_scrollbar.grid(row=0, column=1, sticky="ns")

        # Set frame to resize with window
        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(0, weight=1)

        # Create an options frame below the canvas for buttons
        self.options_frame = tk.Frame(root, bg="lightgray", pady=10)
        self.options_frame.pack(fill=tk.X)
        self.create_option_buttons(self.options_frame)  # Only create options once

        self.node_positions = {}
        self.create_tree_visualization()

    def create_tree_visualization(self):
        start_x = 300
        start_y = 50

        # Draw the root node
        self.draw_node(self.nodes[0], start_x, start_y)

        # Draw edges with updated arrow direction
        for edge in self.edges:
            parent_id, child_id = edge
            parent_pos = self.node_positions[parent_id]
            child_pos = self.node_positions[child_id]
            self.canvas.create_line(child_pos[0], child_pos[1] - 10,
                                    parent_pos[0], parent_pos[1] + 10, arrow=tk.LAST)

    def draw_node(self, node, x, y):
        node_id, node_type, value = node
        text = f"{node_type}: {value}"
        node_width = max(70, len(text) * 6.5)  # Responsive sizing
        rect = self.canvas.create_rectangle(x - node_width // 2, y - 10,
                                            x + node_width // 2, y + 10, fill="lightblue")
        self.canvas.create_text(x, y, text=text, font=("Arial", 10))
        self.node_positions[node_id] = (x, y)

        # Draw each child node
        children = [edge[1] for edge in self.edges if edge[0] == node_id]
        child_x = x - (len(children) - 1) * 120  
        for child_id in children:
            child_node = next(n for n in self.nodes if n[0] == child_id)
            self.draw_node(child_node, child_x, y + 80)
            child_x += 300 

    def create_option_buttons(self, frame):
        # Example buttons; customize for each what-if option
        options = ["Option 1", "Option 2", "Option 3"]
        for option in options:
            btn = tk.Button(frame, text=option, command=lambda o=option: self.update_option(o))
            btn.pack(side=tk.LEFT, padx=5, pady=5)

    def update_option(self, option):
        print(f"Selected option: {option}")
        # Implement additional functionality for option updates here

# Define nodes and edges as per the tree structure
query_tree = build_query_tree(query_input_4)
nodes, edges = get_nodes_and_edges(query_tree)

# Set up the Tkinter window and visualize the tree
root = tk.Tk()
root.title("Query Tree Visualization")
visualizer = TreeVisualizer(root, nodes, edges)
root.mainloop()
