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

def parse_joins(where_clause, tables):
    """
    Parse join conditions from the WHERE clause.
    """
    joins = []
    for table in tables:
        table_alias = table["alias"]
        # Match conditions for joins between table aliases
        condition_pattern = rf"{table_alias}\.\w+\s*=\s*\w+\.\w+"
        matches = re.findall(condition_pattern, where_clause)

        # Parse each condition to form the join structure
        for match in matches:
            # Split only once on '.' to avoid unpacking errors
            left_part, right_part = match.split("=", 1)
            left_alias, left_column = left_part.strip().split(".", 1)
            right_alias, right_column = right_part.strip().split(".", 1)

            join_pair = [
                {"table": next(t["table"] for t in tables if t["alias"] == left_alias), "alias": left_alias, "on": left_column},
                {"table": next(t["table"] for t in tables if t["alias"] == right_alias), "alias": right_alias, "on": right_column}
            ]
            joins.append(join_pair)
    return joins

def preprocess_query(sql_query):
    # Initialize metadata structure
    metadata = {
        "operation": "SELECT",
        "projections": [],
        "source": [],
        "joins": []
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
        metadata["source"] = tables

        # Extract joins if any join conditions exist
        if where_match:
            where_clause = where_match.group(1).strip()
            metadata["joins"] = parse_joins(where_clause, tables)
    
    # Add a suffix to operation if there are joins
    if metadata["joins"]:
        metadata["operation"] += " + Join"

    return metadata

# Example usage
if __name__ == "__main__":
    sql_query = """
    SELECT C.name, O.id FROM customer C, orders O WHERE C.c_custkey = O.o_custkey
    """
    metadata = preprocess_query(sql_query)
    print("Parsed Metadata:", metadata)

