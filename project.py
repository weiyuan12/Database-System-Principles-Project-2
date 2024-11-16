
from preprocessing import process_query_plan_full,preprocess_query
import tkinter as tk
from tkinter import ttk
from interface import TreeVisualizer
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

tree, original_QEP_formatted=process_query_plan_full(sql_query)

modified_QEP_formatted=preprocess_query(sql_query)

# Root window setup

# Root window setup
root = tk.Tk()
root.title("Query Tree Visualization")
# Get screen width and height
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()


root.geometry(f"{screen_width}x{screen_height}")

# Create a scrollable canvas
canvas = tk.Canvas(root)
scrollbar = tk.Scrollbar(root, orient=tk.VERTICAL, command=canvas.yview)
scrollable_frame = tk.Frame(canvas)

# Configure scrolling
scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
)
canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

# Pack canvas and scrollbar
canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

# Create frames for each TreeVisualizer and add them vertically using grid
frame1 = tk.Frame(scrollable_frame, bg="white", relief=tk.SUNKEN, borderwidth=2)
frame1.grid(row=0, column=0, padx=5, pady=1, sticky="nsew")

frame2 = tk.Frame(scrollable_frame, bg="white", relief=tk.SUNKEN, borderwidth=2)
frame2.grid(row=1, column=0, padx=5, pady=1, sticky="nsew")

# Ensure the grid system expands to fill available space
scrollable_frame.grid_rowconfigure(0, weight=1)  
scrollable_frame.grid_rowconfigure(1, weight=1)  
scrollable_frame.grid_columnconfigure(0, weight=1)

# Function to dynamically set minsize based on canvas height
def set_min_size_based_on_canvas():
    # Get canvas height after layout
    canvas_height = canvas.winfo_height()
    
    # Set the minsize of rows based on canvas height
    min_row_height = canvas_height // 2  # Split the space between the two frames evenly
    scrollable_frame.grid_rowconfigure(0, minsize=min_row_height, weight=1)
    scrollable_frame.grid_rowconfigure(1, minsize=min_row_height, weight=1)

# Set minsize once the window is loaded and canvas size is available
root.after(100, set_min_size_based_on_canvas)

# Add TreeVisualizer instances to the respective frames
visualizer1 = TreeVisualizer(frame1, original_QEP_formatted, use_dict_IO_tuples=True, disable_buttons=True,screen_ratio=3)
visualizer2 = TreeVisualizer(frame2, modified_QEP_formatted, use_dict_IO_tuples=False, disable_buttons=False,screen_ratio=3)

# Run the application
root.mainloop()