import tkinter as tk
from tkinter import ttk
from whatif import get_nodes_and_edges, build_query_tree
from example import query_input_1, query_input_2, query_input_3, query_input_4,query_input_5

class TreeVisualizer:
    def __init__(self, root,query_dict):
        self.root = root
        self.nodes = None
        self.edges = None
        self.query_dict = query_dict
        self.join_order = list(range(len(query_dict["joins"])))
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
        # Initial run to display the tree
        self.run()

    def create_tree_visualization(self):
        self.canvas.delete("all")
        query_tree = build_query_tree(self.query_dict, self.join_order)
        self.nodes, self.edges = get_nodes_and_edges(query_tree)
        # Draw the root node
        start_x = 500
        start_y = 50
        self.draw_node(self.nodes[0], start_x, start_y)
        # Draw edges with updated arrow direction
        for edge in self.edges:
            parent_id, child_id = edge
            parent_pos = self.node_positions[parent_id]
            child_pos = self.node_positions[child_id]
            self.canvas.create_line(child_pos[0], child_pos[1] - 15,
                                    parent_pos[0], parent_pos[1] + 15, arrow=tk.LAST)

    def draw_node(self, node, x, y, level=0):
        node_id, node_type, value = node
        text = f"{node_type}: {value}"
        # each node is represented by rectangle

        text_width=150
        text_item=self.canvas.create_text(
        x, y,
        text=text,
        font=("Arial", 10),
        width=text_width,  
        anchor="center"  
        )
        # Get the bounding box of the text to adjust the rectangle size
        text_bbox = self.canvas.bbox(text_item)
        text_width = text_bbox[2] - text_bbox[0]  
        text_height = text_bbox[3] - text_bbox[1]  
        # Define padding for the rectangle
        padding = 10

        # Create the rectangle based on the text's bounding box size
        self.canvas.create_rectangle(
            x - text_width / 2 - padding, y - text_height / 2 - padding,
            x + text_width / 2 + padding, y + text_height / 2 + padding,
            fill="lightblue", outline="black"
        )
        
        self.canvas.delete(text_item)
        self.canvas.create_text(
        x, y,
        text=text,
        font=("Arial", 10),
        width=text_width,  
        anchor="center"  
        )

        self.node_positions[node_id] = (x, y)

        # Draw each child node
        children = [edge[1] for edge in self.edges if edge[0] == node_id]
        if children:
            # Set wider spacing for the first k levels, then reduce for deeper levels
            k = 1
            if level < k:
                child_spacing = 450  # Wider spacing
            else:
                child_spacing = 300  # Narrower spacing

            total_width = (len(children) - 1) * child_spacing
            child_x = x - total_width // 2 # Center children horizontally around the parent

            for child_id in children:
                child_node = next(n for n in self.nodes if n[0] == child_id)
                # Recursively draw each child node with increased level
                self.draw_node(child_node, child_x, y + 80, level + 1)
                child_x += child_spacing


    def create_option_buttons(self, frame):
        # Example buttons; customize for each what-if option
        options = ["Option 1", "Option 2", "Option 3"]
        for option in options:
            btn = tk.Button(frame, text=option, command=lambda o=option: self.update_option(o))
            btn.pack(side=tk.LEFT, padx=5, pady=5)

    def update_option(self, option):
        print(f"Selected option: {option}")
        # Implement additional functionality for option updates here
        self.rotate_join_order()

    def rotate_join_order(self):
        # Shift the join_order array left by 1
        if self.join_order:
            self.join_order.append(self.join_order.pop(0))
        self.run()
    def run(self):
        """Runs the visualization setup and updates tree visualization."""
        # Rotate the join order and rebuild the visualization
        self.create_tree_visualization()


# Set up the Tkinter window and visualize the tree
root = tk.Tk()
root.title("Query Tree Visualization")
visualizer = TreeVisualizer(root, query_input_4)
root.mainloop()
