import re

def parse_tables_from_clause(from_clause):
    # Extract table names and aliases from the FROM clause
    tables = []
    for match in re.finditer(r"(\w+)\s+(\w+)?", from_clause, re.IGNORECASE):
        table_name = match.group(1)
        alias = match.group(2) if match.group(2) else table_name
        tables.append({"table": table_name, "alias": alias})
    return tables

def parse_projections(select_clause):
    # Extract columns specified in SELECT, ignoring aggregates and functions for simplicity
    projections = [col.strip() for col in select_clause.split(",")]
    if '*' in projections:
        return ["*"]  # Wildcard indicates all columns
    return projections

def parse_conditions(where_clause, tables):
    # Identify join conditions by finding table aliases within the WHERE clause
    conditions = []
    for table in tables:
        table_alias = table["alias"]
        join_pattern = rf"{table_alias}\.\w+\s*=\s*\w+\.\w+"
        matches = re.findall(join_pattern, where_clause)
        conditions.extend(matches)
    return conditions

def detect_access_method(where_clause, table_alias):
    """
    Simulates the detection of an access method. 
    Returns 'Index Scan' if an equality condition on a column is detected, 
    otherwise defaults to 'Sequential Scan'.
    """
    index_condition = None
    access_method = "Sequential Scan"  # Default to sequential scan

    # Pattern to match equality conditions, assuming these could use an index
    condition_pattern = rf"{table_alias}\.(\w+)\s*=\s*'?\w+'?"
    condition_match = re.search(condition_pattern, where_clause)
    
    if condition_match:
        access_method = "Index Scan"
        column_name = condition_match.group(1)
        index_condition = {
            "index": f"{table_alias}_{column_name}_idx",  # Simulated index name
            "column": f"{table_alias}.{column_name}",
            "operator": "=",
            "value": re.search(r"=\s*'?(\w+)'?", condition_match.group(0)).group(1)
        }
    
    return access_method, index_condition

def detect_join_type(conditions):
    """
    Simulate detection of join type based on conditions.
    Returns 'Hash Join', 'Sort-Merge Join', or 'Nested Loop Join'.
    """
    if any("=" in condition for condition in conditions):
        return "Hash Join"  # Assumes equality-based joins might use hash joins
    elif any("ORDER BY" in condition for condition in conditions):
        return "Sort-Merge Join"  # Assumes sorting could be used for merge joins
    return "Nested Loop Join"  # Default join type

def preprocess_query(sql_query):
    # Initialize metadata structure
    metadata = {
        "operation": "SELECT",
        "projections": [],
        "source": [],
        "join_conditions": [],
        "join_type": None
    }
    
    # Extract the main parts of the SQL query using regex
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql_query, re.IGNORECASE | re.DOTALL)
    from_match = re.search(r"FROM\s+(.*?)\s+(WHERE|GROUP|ORDER|$)", sql_query, re.IGNORECASE | re.DOTALL)
    where_match = re.search(r"WHERE\s+(.*?)(GROUP|ORDER|$)", sql_query, re.IGNORECASE | re.DOTALL)
    
    if select_match:
        select_clause = select_match.group(1).strip()
        metadata["projections"] = parse_projections(select_clause)
    
    if from_match:
        from_clause = from_match.group(1).strip()
        tables = parse_tables_from_clause(from_clause)
        
        for table in tables:
            table_alias = table["alias"]
            access_method, index_condition = detect_access_method(
                where_match.group(1).strip() if where_match else "", table_alias
            )
            table_metadata = {
                "table": table["table"],
                "alias": table_alias,
                "access_method": access_method,
                "condition": index_condition  # Include index condition if available
            }
            metadata["source"].append(table_metadata)
    
    if where_match:
        where_clause = where_match.group(1).strip()
        conditions = parse_conditions(where_clause, tables)
        metadata["join_conditions"] = conditions
        if conditions:
            metadata["operation"] += " + Join"  # Indicates join presence in operation
            metadata["join_type"] = detect_join_type(conditions)  # Add join type
    
    return metadata

# Example usage
if __name__ == "__main__":
    sql_query = """
    SELECT C.name, O.id FROM customer C, orders O WHERE C.c_custkey = O.o_custkey
    """
    metadata = preprocess_query(sql_query)
    print("Parsed Metadata:", metadata)
