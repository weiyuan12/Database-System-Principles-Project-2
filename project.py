
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
root = tk.Tk()
root.title("Query Tree Visualization")
visualizer = TreeVisualizer(root, original_QEP_formatted,True)
root.mainloop()