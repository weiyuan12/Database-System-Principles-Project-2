import tkinter as tk
from tkinter import ttk
from whatif import get_nodes_and_edges, build_query_tree,total_IO_cost
from preprocessing import process_query_plan_full,preprocess_query
from constants import query_input_1,JOINS,SCANS,FILTERS
#{'lineitem': 6001215, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'customer': 150000, 'supplier': 10000, 'region': 5, 'nation': 25}


class TreeVisualizer:
    def __init__(self, root,query_dict,use_dict_IO_tuples,disable_buttons,screen_ratio):
        self.root = root
        self.nodes = None
        self.edges = None
        self.query_dict = query_dict
        self.use_dict_IO_tuples=use_dict_IO_tuples
        self.join_order = list(range(len(query_dict["joins"])))
        self.disable_buttons = disable_buttons
        self.screen_ratio = screen_ratio
        print(self.root)
         # Get the screen dimensions
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()

        # Set the canvas width to the screen width
        canvas_width = screen_width

        # Set the canvas height based on the screen_ratio
        canvas_height = screen_height // self.screen_ratio
        if isinstance(self.root, tk.Tk):
            root.geometry(f"{screen_width}x{screen_height}")
       
        self.join_types=[]
        # Create a frame to hold the canvas and scrollbars
        self.frame = tk.Frame(root)
        self.frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(0, weight=1)
        # Create a canvas with scrollbars
        self.canvas = tk.Canvas(self.frame, bg='white',width=canvas_width-100, height=canvas_height, scrollregion=(0, 0, 1600, 1400))
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
        self.options_frame.grid(row=2, column=0, sticky="ew")
        if self.disable_buttons is False:
            self.create_option_buttons(self.options_frame)  # Only create options once
            self.create_join_buttons()
            self.create_scan_buttons()
        self.node_positions = {}
        # Initial run to display the tree
        self.run()

    def create_tree_visualization(self):
        self.canvas.delete("all")
        query_tree = build_query_tree(self.query_dict, self.join_order,self.use_dict_IO_tuples)
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
            self.canvas.create_line(child_pos[0], child_pos[1] - 30,
                                    parent_pos[0], parent_pos[1] + 30, arrow=tk.LAST)
        # Add the statistics
        tree_IO_cost=total_IO_cost(query_tree)
        est_tuples = 0
        actual_tuples = 0
        # Remove old bottom label if it exists
        if hasattr(self, 'bottom_label') and self.bottom_label.winfo_exists():
            self.bottom_label.destroy()

        # Add the updated statistics as a new label
        if self.disable_buttons is True:
            mode= "Original"
            self.bottom_label = tk.Label(
            self.options_frame,
            text=f"{mode}: \nTotal IO cost: {tree_IO_cost} \nActual tuples: {est_tuples}",
            anchor="e",
            bg="lightgray",
            font=("Arial", 10,"bold")
        )
        else:
            mode = "Modified"
            self.bottom_label = tk.Label(
                self.options_frame,
                text=f"{mode}: \nEstimated IO cost: {tree_IO_cost} \nEstimated tuples: {est_tuples}",
                anchor="e",
                bg="lightgray",
                font=("Arial", 10,"bold")
            )
        self.bottom_label.pack(fill=tk.X, padx=5, pady=1, side="right")
    def create_join_buttons(self):
        """Create buttons to change join types."""
        for idx, join in enumerate(self.query_dict["joins"]):
            join_label = f"JOIN {join[0]['table']}={join[1]['table']}"
            btn = tk.Button(
                self.options_frame,
                text=join_label,
                command=lambda i=idx: self.update_join_type(i),
                padx=1,
                width=10, 
                height=1,  
                font=("Arial", 8) 
            )
            btn.pack(side=tk.LEFT, padx=2, pady=2)
    def create_scan_buttons(self):
        """Create buttons to change scan types."""
        for idx, source in enumerate(self.query_dict["source"]):
            source_label = f"SOURCE {source['alias']}"
            btn = tk.Button(
                self.options_frame,
                text=source_label,
                command=lambda i=idx: self.update_scan_types("source",i),
                padx=1,
                width=10, 
                height=1,  
                font=("Arial", 8) 
            )
            btn.pack(side=tk.LEFT, padx=2, pady=2)
        for idx, select in enumerate(self.query_dict["selects"]):
            select_label = f"SELECT {select['alias']}"
            btn = tk.Button(
                self.options_frame,
                text=select_label,
                command=lambda i=idx: self.update_scan_types("selects",i),
                padx=1,
                width=10, 
                height=1,  
                font=("Arial", 8) 
            )
            btn.pack(side=tk.LEFT, padx=1, pady=1)
    
    def update_join_type(self, join_index):
        """Toggle the join type for the selected join."""
        current_join = self.query_dict["joins"][join_index]
        for join in current_join:
            # Toggle between join types (Hash and Nested Loop as an example)
            if join["type"] == JOINS[0]:
                join["type"] = JOINS[1]
            elif join["type"] == JOINS[1]:
                join["type"] = JOINS[2]
            elif join["type"] == JOINS[2]:
                join["type"] = JOINS[3]
            else:
                join["type"] = JOINS[0]
        self.run()
    def update_scan_types(self,mode,index):
        current_scan = self.query_dict[mode][index]

        if current_scan["type"] == SCANS[0]:
            current_scan["type"] = SCANS[1]
        elif current_scan["type"] == SCANS[1]:
            current_scan["type"] = SCANS[2]
        elif current_scan["type"] == SCANS[2]:
            current_scan["type"] = SCANS[3]
        else:
            current_scan["type"] = SCANS[0]
        self.run()
    def draw_node(self, node, x, y, level=0):
        node_id, node_type, value, IO_cost,tuples,Q_type = node
        
        text = f"{node_type}: {value}\n IO: {IO_cost}, Tup:{tuples} \n Type: {Q_type}"
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
        options = ["Rotate Join Order ->", "<- Rotate Join Order"]
        btn = tk.Button(frame, text=options[0], command=lambda o=options[0]: self.rotate_join_order("left"))
        btn.pack(side=tk.LEFT, padx=5, pady=5)
        btn = tk.Button(frame, text=options[1], command=lambda o=options[0]: self.rotate_join_order("right"))
        btn.pack(side=tk.LEFT, padx=5, pady=5)

    def rotate_join_order(self, direction):
        """Shift the join_order array. Direction can be 'left' or 'right'."""
        if self.join_order:
            if direction == "left":
                self.join_order.append(self.join_order.pop(0))
            elif direction == "right":
                self.join_order.insert(0, self.join_order.pop())
            self.run()
    def run(self):
        """Runs the visualization setup and updates tree visualization."""
        self.create_tree_visualization()

    

# Set up the Tkinter window and visualize the tree


'''

root = tk.Tk()
root.title("Query Tree Visualization")
sql_query =  """
    SELECT 
        C.c_custkey AS customer_id,
        C.c_name AS customer_name,
        C.c_acctbal AS customer_balance,
        N.n_name AS nation_name,
        R.r_name AS region_name,
        S.s_name AS supplier_name,
        S.s_acctbal AS supplier_balance
    FROM customer C, nation N, region R, supplier S
    WHERE C.c_nationkey = N.n_nationkey
    AND N.n_regionkey = R.r_regionkey
    AND S.s_nationkey = N.n_nationkey
    AND C.c_acctbal > 1000
    """
modified_QEP_formatted=preprocess_query(sql_query)
tree, original_QEP_formatted=process_query_plan_full(sql_query)
visualizer = TreeVisualizer(root, original_QEP_formatted, use_dict_IO_tuples=True,disable_buttons=True,1)
#visualizer = TreeVisualizer(root, modified_QEP_formatted, use_dict_IO_tuples=False, disable_buttons=False,1)
root.mainloop()

'''