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

class PgConn(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PgConn, cls).__new__(cls)
            cls._instance.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
        return cls._instance

    def get_connection(self):
        return self.conn
    
    def close_connection(self):
        self.conn.close()

    def get_row_count(self, table):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            return count
        
    def query_row_counts(self):
        pg_stats = {}
        for table in tables:
            row_count = self.get_row_count(table)
            pg_stats[table] = row_count
        return pg_stats

    def get_no_blocks(self, table):
        ''' returns 0 if table is small (less than 50)'''
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT relpages FROM pg_class WHERE relname = '{table}'")
            result = cur.fetchone()
            count = result[0] if result else None
            if count == 0:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                if count>0: return 1
            return count
        
    def explain_simple_join(self, table1, table2, key1, key2):
        format_test = """
        EXPLAIN ANALYZE
        SELECT *
        FROM {table1}
        JOIN {table2} ON {table1}.{key1} = {table2}.{key2};
        """
        query = format_test.format(table1=table1, table2=table2, key1=key1, key2=key2)
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            return result
        
    def get_execution_plan(self, query):
        try:
            with self.conn.cursor() as cur:
                # Use EXPLAIN to get the execution plan
                cur.execute(f"EXPLAIN ANALYZE {query}")
                # Fetch all lines of the plan
                plan = cur.fetchall()
                # Join the plan lines into a single string for easy viewing
                execution_plan = "\n".join([line[0] for line in plan])
                return execution_plan
        except Exception as e:
            print(f"Error: {e}")


# Function to get the number of rows in a table
def get_row_count(conn, table):
    'Deprecated function. Use PgConn class instead.'
    with conn.cursor() as cur:
        # SQL query to count the rows
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        # Fetch the result
        count = cur.fetchone()[0]
        return count

def query_row_counts():
    'Deprecated function. Use PgConn class instead.'
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



if __name__ == "__main__":
    pgconn = PgConn()
    pg_stats = pgconn.query_row_counts()
    print(pg_stats)
    for table in tables:
        print(f"Table {table} has {pgconn.get_no_blocks(table)} blocks.")

    print("Table invalid_name has", pgconn.get_no_blocks("invalid_name"), "blocks.")

    print("simple join plan:", pgconn.explain_simple_join("lineitem", "orders", "l_orderkey", "o_orderkey"))
    print("Execution plan for simple join:", pgconn.get_execution_plan("SELECT * FROM lineitem JOIN orders ON lineitem.l_orderkey = orders.o_orderkey"))