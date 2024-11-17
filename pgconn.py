import psycopg2
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

def get_unique_count(table, key) -> int:
    '''
    Function to get the number of unique values in a column
    '''
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
            # SQL query to count the distinct values
            cur.execute(f"SELECT attname, n_distinct FROM pg_stats WHERE tablename='{table}' AND attname='{key}'")
            # Fetch the result
            count = cur.fetchone()[1]
            # If n_distinct is positive, it's an estimated number of distinct values.
            # If n_distinct is negative, it represents a fraction of the total rows (e.g., -0.1 means ~10% of the rows are unique).
            if count >0:
                conn.close()
                return count
            else:
                total_rows = get_row_count(conn, table)
                conn.close()
                return int(total_rows * abs(count))

    except Exception as e:
        print(f"Error: {e}")

def get_no_working_blocks() -> int:
    '''
    Function to get the number of working blocks in shared_buffers
    '''
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
            # SQL query to get the number of working blocks
            cur.execute("""
            SELECT
                setting AS shared_buffers_blocks,
                setting::bigint * current_setting('block_size')::bigint / (1024 * 1024) AS shared_buffers_size_mb
            FROM pg_settings
            WHERE name = 'shared_buffers';
            """)
            # Fetch the result
            result = cur.fetchone()[0]
            conn.close()
            return result
    except Exception as e:
        print(f"Error: {e}")

def get_blocks(table) -> int:
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
            # SQL query to get the number of blocks used by the table
            cur.execute(f"""
            SELECT
                pg_relation_size('{table}') AS table_size_bytes,
                pg_size_pretty(pg_relation_size('{table}')) AS table_size_pretty,
                current_setting('block_size')::int AS block_size_bytes,
                pg_relation_size('{table}') / current_setting('block_size')::int AS blocks_used
            FROM
                pg_class
            WHERE
                relname = '{table}';
            """)
            # Fetch the result
            result = cur.fetchone()
            conn.close()
            return result[3]
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    table = "lineitem"
    key = "l_extendedprice"
    print(f"Number of tuples in {table}: {query_row_counts()[table]}")
    print(f"Unique count of {table}: {get_unique_count(table, key)}")
    print(f"Number of working blocks: {get_no_working_blocks()}")
    print(f"Get blocks in {table} : {get_blocks(table)}")