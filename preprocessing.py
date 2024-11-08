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

def parse_conditions(where_clause, table_alias):
    """
    Parse specific conditions related to the table alias from the WHERE clause.
    """
    conditions = []
    # Match conditions related to the given table alias
    condition_pattern = rf"{table_alias}\.\w+\s*=\s*\w+\.\w+"
    matches = re.findall(condition_pattern, where_clause)
    conditions.extend(matches)
    return conditions

def preprocess_query(sql_query):
    # Initialize metadata structure
    metadata = {
        "operation": "SELECT",
        "projections": [],
        "source": []
    }
    
    # Extract the main parts of the SQL query using regex
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql_query, re.IGNORECASE | re.DOTALL)
    from_match = re.search(r"FROM\s+(.+?)\s*(WHERE|GROUP)", sql_query, re.IGNORECASE | re.DOTALL)
    where_match = re.search(r"WHERE\s+(.*?)(GROUP|ORDER|$)", sql_query, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_clause = select_match.group(1).strip()
        metadata["projections"] = parse_projections(select_clause)
    
    if from_match:
        from_clause = from_match.group(1).strip()
        tables = parse_tables_from_clause(from_clause)
        
        for table in tables:
            table_alias = table["alias"]
            # Each table gets its specific conditions from the WHERE clause
            conditions = parse_conditions(
                where_match.group(1).strip() if where_match else "", table_alias
            )
            table_metadata = {
                "table": table["table"],
                "alias": table_alias,
                "condition": conditions if conditions else None  # Include condition if available
            }
            metadata["source"].append(table_metadata)
    
    if where_match and tables:
        metadata["operation"] += " + Join"  # Indicates join presence in operation
    
    return metadata

# Example usage
if __name__ == "__main__":
    sql_query = """
    SELECT C.name, O.id FROM customer C, orders O WHERE C.c_custkey = O.o_custkey
    """
    metadata = preprocess_query(sql_query)
    print("Parsed Metadata:", metadata)
