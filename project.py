
from preprocessing import process_query_plan_full,preprocess_query
import tkinter as tk
from tkinter import scrolledtext  # Import scrolledtext for multi-line input
from tkinter import ttk
from interface import TreeVisualizer
from pgconn import query_row_counts,get_no_working_blocks
M=int(get_no_working_blocks())
Tuples=query_row_counts()
if M is None:
    M = 16384
if Tuples is None:
    Tuples = {'lineitem': 6001215, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'customer': 150000, 'supplier': 10000, 'region': 5, 'nation': 25}

sql_query =  '''
SELECT 
        c.c_name AS customer_name
    FROM 
        customer c
    WHERE 
        c.c_acctbal<1000
'''
sql_query2 =  """
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
    AND S.s_nationkey < 2000
    """
# Function to open the overlay input box
def open_sql_input_overlay():
    # Create a Toplevel window for the SQL input box
    overlay = tk.Toplevel(root)
    overlay.title("Enter SQL Query")
    overlay.geometry("600x400")  # Set the size of the overlay
    overlay.transient(root)  # Keep it on top of the main window
    overlay.grab_set()  # Block interaction with the main window

    # Add a label
    tk.Label(overlay, text="Enter your SQL Query:", font=("Arial", 12)).pack(pady=10)

    # Add a ScrolledText widget for multi-line SQL input
    query_input = scrolledtext.ScrolledText(overlay, wrap=tk.WORD, height=15, width=70, font=("Arial", 10))
    query_input.pack(pady=10, padx=10)

    # Function to process and recreate TreeVisualizers
    def submit_query():
        global sql_query  # Declare sql_query as global
        sql_query = query_input.get("1.0", tk.END).strip()  # Get the input SQL query
        overlay.destroy()  # Close the overlay

        # Process the SQL query
        tree, original_QEP_formatted = process_query_plan_full(sql_query)
        modified_QEP_formatted = preprocess_query(sql_query)
        for item in original_QEP_formatted["source"]:
            table_name = item["table"]
            if table_name in Tuples:
                Tuples[table_name] = item["tuples"]
        # Recreate the TreeVisualizers
        recreate_visualizers(original_QEP_formatted, modified_QEP_formatted)

    # Add a Submit button
    submit_button = tk.Button(overlay, text="Submit", command=submit_query)
    submit_button.pack(pady=10)

# Function to recreate TreeVisualizers
def recreate_visualizers(original_QEP, modified_QEP):
    # Clear existing frames
    for widget in frame1.winfo_children():
        widget.destroy()
    for widget in frame2.winfo_children():
        widget.destroy()

    # Recreate TreeVisualizers
    global visualizer1, visualizer2
    visualizer1 = TreeVisualizer(frame1, original_QEP, use_dict_IO_tuples=True, disable_buttons=True, screen_ratio=2.5, Tuples=Tuples, M=M)
    visualizer2 = TreeVisualizer(frame2, modified_QEP, use_dict_IO_tuples=False, disable_buttons=False, screen_ratio=2.5, Tuples=Tuples, M=M)

# Root window setup
root = tk.Tk()
root.title("Query Tree Visualization")

# Get screen dimensions
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

# Create frames for TreeVisualizers
frame1 = tk.Frame(scrollable_frame, bg="white", relief=tk.SUNKEN, borderwidth=2)
frame1.grid(row=0, column=0, padx=5, pady=1, sticky="nsew")

frame2 = tk.Frame(scrollable_frame, bg="white", relief=tk.SUNKEN, borderwidth=2)
frame2.grid(row=1, column=0, padx=5, pady=1, sticky="nsew")

# Configure grid resizing
scrollable_frame.grid_rowconfigure(0, weight=1)
scrollable_frame.grid_rowconfigure(1, weight=1)
scrollable_frame.grid_columnconfigure(0, weight=1)

# Function to dynamically set minimum size for rows
def set_min_size_based_on_canvas():
    canvas_height = canvas.winfo_height()
    min_row_height = canvas_height // 2  # Split space evenly
    scrollable_frame.grid_rowconfigure(0, minsize=min_row_height, weight=1)
    scrollable_frame.grid_rowconfigure(1, minsize=min_row_height, weight=1)

root.after(100, set_min_size_based_on_canvas)

tree, original_QEP_formatted = process_query_plan_full(sql_query)
modified_QEP_formatted = preprocess_query(sql_query)
for item in original_QEP_formatted["source"]:
    table_name = item["table"]
    if table_name in Tuples:
        Tuples[table_name] = item["tuples"]
visualizer1 = TreeVisualizer(frame1, original_QEP_formatted, use_dict_IO_tuples=True, disable_buttons=True, screen_ratio=3, Tuples=Tuples, M=M)
visualizer2 = TreeVisualizer(frame2, modified_QEP_formatted, use_dict_IO_tuples=False, disable_buttons=False, screen_ratio=2, Tuples=Tuples, M=M)

# Add a button to open the SQL input overlay
overlay_button = tk.Button(root, text="Enter SQL Query", command=open_sql_input_overlay)
overlay_button.pack(pady=10)

# Run the application
root.mainloop()