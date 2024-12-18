import tkinter as tk
from tkinter import ttk
from whatif import get_nodes_and_edges, build_query_tree,total_IO_cost
from preprocessing import process_query_plan_full,preprocess_query
from constants import query_input_1,JOINS,SCANS,FILTERS
import itertools
from functools import partial
#{'lineitem': 6001215, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'customer': 150000, 'supplier': 10000, 'region': 5, 'nation': 25}


class TreeVisualizer:
    def __init__(self, root,query_dict,use_dict_IO_tuples,disable_buttons,screen_ratio,Tuples,M):
        self.root = root
        self.nodes = None
        self.edges = None
        self.query_dict = query_dict
        self.use_dict_IO_tuples=use_dict_IO_tuples
        self.buttons = []
        self.join_order = list(range(len(query_dict["joins"])))
        self.original_join_order = list(range(len(query_dict["joins"])))
        #self.permutations = itertools.permutations(self.join_order)
        self.permutations = self.generate_valid_join_orders(query_dict["joins"])
        self.current_permutation=None
        self.disable_buttons = disable_buttons
        self.screen_ratio = screen_ratio
        self.Tuples = Tuples
        self.M=M
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
        self.canvas = tk.Canvas(self.frame, bg='white',width=canvas_width-200, height=canvas_height, scrollregion=(0, 0, 5000, 5000))
        self.h_scrollbar = ttk.Scrollbar(self.frame, orient="horizontal", command=self.canvas.xview)
        self.v_scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(xscrollcommand=self.h_scrollbar.set, yscrollcommand=self.v_scrollbar.set)

        # Position scrollbars: vertical on the left, horizontal at the bottom
        self.v_scrollbar.grid(row=0, column=0, sticky="ns")
        self.canvas.grid(row=0, column=1, sticky="nsew")
        self.h_scrollbar.grid(row=1, column=1, sticky="ew")
        
        # Set frame to resize with window
        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(1, weight=1)

    
        # Create an options frame below the canvas for buttons
        self.options_frame = tk.Frame(root, bg="lightgray", pady=10)
        self.options_frame.grid(row=2, column=0, sticky="ew")
        
        if self.disable_buttons is False:
            self.create_option_buttons(self.options_frame)  # Only create options once
            self.create_join_buttons()
            self.create_scan_buttons()
        self.node_positions = {}

        self.next_permutation()
        # Initial run to display the tree
        self.run()

    def create_tree_visualization(self):
        self.canvas.delete("all")

        query_tree,intermediate_relations = build_query_tree(self.query_dict, self.join_order,self.use_dict_IO_tuples,self.Tuples,self.M)
        
        self.nodes, self.edges = get_nodes_and_edges(query_tree)
        # Draw the root node
        start_x = 500
        start_y = 50
        self.draw_node(self.nodes[0], start_x, start_y,self.use_dict_IO_tuples)
        # Draw edges with updated arrow direction
        for edge in self.edges:
            parent_id, child_id = edge
            parent_pos = self.node_positions[parent_id]
            child_pos = self.node_positions[child_id]
            self.canvas.create_line(child_pos[0], child_pos[1] - 30,
                                    parent_pos[0], parent_pos[1] + 35, arrow=tk.LAST)
        # Add the statistics
        tree_IO_cost=total_IO_cost(query_tree)
        est_tuples = query_tree.get_tuples()
        # Remove old bottom label if it exists
        if hasattr(self, 'bottom_label') and self.bottom_label.winfo_exists():
            self.bottom_label.destroy()
        if len(intermediate_relations) > 1:
            invalid_text=self.canvas.create_text(
                500, 50,  # Position in the center of the canvas
                text="Invalid tree!", 
                font=("Arial", 40, "bold"), 
                fill="red",
                anchor="center"
                                    )
            # Get the bounding box of the text
            text_bbox = self.canvas.bbox(invalid_text)  # (x1, y1, x2, y2)

            # Add padding for the background rectangle
            padding = 10
            x1, y1, x2, y2 = text_bbox
            x1 -= padding
            y1 -= padding
            x2 += padding
            y2 += padding

            # Create the rectangle behind the text
            background = self.canvas.create_rectangle(
                x1, y1, x2, y2,
                fill="lightyellow",  # Background color
                outline="black"      # Optional outline color
            )
            
            # Ensure the rectangle is behind the text
            self.canvas.tag_lower(background, invalid_text)
        # Add the updated statistics as a new label
        if self.disable_buttons is True:
            mode = "Original"
            # Round the IO cost and tuples to 2 decimal places
            rounded_tree_IO_cost = round(tree_IO_cost, 2)
            rounded_est_tuples = round(est_tuples, 2)
            self.bottom_label = tk.Label(
                self.options_frame,
                text=f"{mode}: \nActual IO cost: {rounded_tree_IO_cost} \nActual tuples: {rounded_est_tuples}",
                anchor="e",
                bg="lightgray",
                font=("Arial", 10, "bold")
            )
        else:
            mode = "Modified"
            # Round the IO cost and tuples to 2 decimal places
            rounded_tree_IO_cost = round(tree_IO_cost, 2)
            rounded_est_tuples = round(est_tuples, 2)
            self.bottom_label = tk.Label(
                self.options_frame,
                text=f"{mode}: \nEstimated IO cost: {rounded_tree_IO_cost} \nEstimated tuples: {rounded_est_tuples}",
                anchor="e",
                bg="lightgray",
                font=("Arial", 10, "bold")
            )
        self.bottom_label.pack(fill=tk.X, padx=5, pady=1, side="right")
    def create_join_buttons(self):
        """Create buttons to change join types."""
        for idx, join in enumerate(self.query_dict["joins"]):
            join_label = f"J {join[0]['table']}={join[1]['table']}"
            btn = tk.Button(
                self.options_frame,
                text=join_label,
                command=lambda i=idx: self.update_join_type(i),
                padx=1,
                width=8, 
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
                width=8, 
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
    
    def _toggle_type(self, mode, index, options_list):
        """Helper function to toggle between types in a given list."""
        current_item = self.query_dict[mode][index]
        if(mode=="joins"):
            current_type = current_item[0]["type"]
            current_type = current_item[1]["type"]
        else:
            current_type = current_item["type"]
        
        # Find the current type's index and toggle to the next one, looping around
        current_index = options_list.index(current_type)
        new_index = (current_index + 1) % len(options_list)
        
        # Update the type
        if(mode=="joins"):
            current_item[0]["type"] = options_list[new_index]
            current_item[1]["type"] = options_list[new_index]
        else:
            current_item["type"] = options_list[new_index]

    def update_join_type(self, join_index):
        """Toggle the join type for the selected join."""
        self._toggle_type('joins', join_index, JOINS)
        self.run()

    def update_scan_types(self, mode, index):
        """Toggle the scan type for the selected scan."""
        self._toggle_type(mode, index, SCANS)
        self.run()
    
    def draw_node(self, node, x, y, use_dict_IO_tuples,level=0):
        node_id, node_type, value, IO_cost,tuples,Q_type = node
        if(use_dict_IO_tuples):
            text = f"{node_type}: {value}\n IO: {IO_cost}, Tup:{tuples} \n Type: {Q_type}"
        else:
            text = f"{node_type}: {value}\n Est IO: {IO_cost}, Est Tup:{tuples} \n Type: {Q_type}"
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
        anchor="center",
        )
        self.node_positions[node_id] = (x, y)
        # Draw each child node
        children = [edge[1] for edge in self.edges if edge[0] == node_id]
        if children:
            # Define child spacing for the horizontal and vertical positioning
            k = 1  # Custom level threshold for spacing adjustments
            if level < k:
                child_spacing = 450  # Wider horizontal spacing for shallow levels
            else:
                child_spacing = 250  # Narrower horizontal spacing for deeper levels
            
            total_width = (len(children) - 1) * child_spacing
            child_x = x - total_width // 2  # Center children horizontally around the parent

            # Calculate dynamic vertical spacing based on level
            if len(children) <= 1:
                
                vertical_spacing = 70 + (level * 12)  # Move single child slightly up
            else:
                vertical_spacing = 100 + (level * 25)  # Move multiple children further down

            for child_id in children:
                child_node = next(n for n in self.nodes if n[0] == child_id)
                # Recursively draw each child node with adjusted x and y, and increased level
                self.draw_node(child_node, child_x, y + vertical_spacing, use_dict_IO_tuples, level + 1)
                child_x += child_spacing  # Move child_x position for the next child



    def create_option_buttons(self, frame):
            options = ["Rotate Join Order ->", "<- Rotate Join Order"]
            
            btn = tk.Button(frame, text=options[0], 
                            command=lambda o=options[0]: self.next_permutation(),
                            font=("Arial", 8),  # Smaller font size
                            padx=2,  # Reduce horizontal padding
                            pady=2,  # Reduce vertical padding
                            width=15,  # Set a fixed smaller width
                            height=1   # Set a fixed smaller height
                            )
            btn.pack(side=tk.LEFT, padx=1, pady=1)
    def next_permutation(self):
        """Move to the next permutation in the list."""
        try:
            # Get the next permutation
            self.current_permutation = next(self.permutations)
            self.join_order = list(self.current_permutation)  # Update the current order
            self.run()
        except StopIteration:
            print("reset")
            self.reset_permutations()
            self.current_permutation = next(self.permutations)
            self.join_order = list(self.current_permutation)
            self.run()

    def reset_permutations(self):
        self.permutations = itertools.permutations(self.original_join_order)
        self.current_permutation = self.original_join_order
        
    def run(self):
        """Runs the visualization setup and updates tree visualization."""
        self.create_tree_visualization()

    def get_join_validity(self, joins, order):
        """
        Determine the validity between joins based on the tables involved for a given order.
        The order is valid if each join contains at least one table from the previous join's dependency.
        """
        dependencies = [[] for _ in range(len(joins))]
        
        # Iterate through each join in the given order
        for i, idx in enumerate(order):
            temp = []  # List to hold the tables in the current join
            
            # Collect all tables (aliases) from the current join
            for table_info in joins[idx]:
                temp.append(table_info["alias"])
            
            # Check validity
            if i > 0:  # If it's not the first join
                # Check if the previous dependency has at least one table from the current join
                if not any(table in dependencies[i-1] for table in temp):
                    return False  # If no table in the previous dependency matches the current temp, invalid order

            # Store the dependency for the current join (cumulative list of tables)
            if i == 0:
                dependencies[i] = temp  # For the first join, just add its tables
            else:
                dependencies[i] = dependencies[i-1] + temp  # Add the tables from the previous dependency and the current join

        return True


    def generate_valid_join_orders(self, joins):
        """
        Generate all valid join orders based on the given join operations.
        """
        # Get the table dependencies
        all_orders = itertools.permutations(range(len(joins)))
        
        # Return a generator that yields only valid join orders
        return (order for order in all_orders if self.get_join_validity(joins, order))
        

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