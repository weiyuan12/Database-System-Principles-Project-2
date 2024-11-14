import psycopg2
from graphviz import Digraph
import tkinter as tk
from tkinter import ttk
import re
# Database connection parameters
host = "localhost"
port = "5433"       
dbname = "benchmark_tpc_h"  
user = "postgres"  
password = "password"

# List of tables
tables = [
    "lineitem", "orders", "part", "partsupp", 
    "customer", "supplier", "region", "nation"
]

# Function to get the number of rows in a table
def get_row_count(conn, table):
    with conn.cursor() as cur:
        # SQL query to count the rows
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        # Fetch the result
        count = cur.fetchone()[0]
        return count

def query_row_counts():
    try:
        # Establish the connection
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        # Iterate through each table and get the row count
        pg_stats={}
        for table in tables:
            row_count = get_row_count(conn, table)
            #print(f"Table {table} has {row_count} rows.")
            pg_stats[table] = row_count

        # Close the connection
        conn.close()
        return pg_stats

    except Exception as e:
        print(f"Error: {e}")



def get_execution_plan(query):
    try:
        # Establish the connection
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        with conn.cursor() as cur:
            # Use EXPLAIN to get the execution plan
            cur.execute(f"EXPLAIN ANALYSE {query}")
            # Fetch all lines of the plan
            plan = cur.fetchall()
            # Join the plan lines into a single string for easy viewing
            execution_plan = "\n".join([line[0] for line in plan])
            conn.close()
            return execution_plan
    except Exception as e:
        print(f"Error: {e}")


