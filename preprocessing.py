import re

def parse_tables_from_clause(from_clause):
    '''
    Extract table names and aliases in FROM clause
    - from_clause: The FROM clause of the raw query
    returns: tables (sources)
    '''
    tables = []
    for match in re.finditer(r"(\w+)\s+(\w+)?", from_clause, re.IGNORECASE):
        table_name = match.group(1)
        alias = match.group(2) if match.group(2) else table_name
        tables.append({"table": table_name, "alias": alias})
    return tables

def parse_projections(select_clause):
    '''
    Extract columns specified in SELECT
    - select_clause: The SELECT clause of the raw query
    returns: projections array
    '''
    projections = [col.strip() for col in select_clause.split(",")]
    if '*' in projections:
        return ["*"]  # Wildcard indicates all columns
    return projections

def parse_conditions(where_clause, tables):
    """
    Parse conditions from the WHERE clause and separate them into joins and selects.
    - tables: List of tables with their aliases
    returns: joins, selects array
    """
    joins = []
    selects = []
    
    # Find all conditions with comparison operators
    condition_pattern = r"(\w+\.\w+)\s*(=|>|<|>=|<=)\s*(\w+\.\w+|\w+)"
    matches = re.findall(condition_pattern, where_clause)

    for left_side, operator, right_side in matches:
        # Count dots to determine if the condition is a join or select
        left_dots = left_side.count(".")
        right_dots = right_side.count(".")

        if left_dots == 1 and right_dots == 1:
            # It's a join condition
            left_alias, left_column = left_side.split(".")
            right_alias, right_column = right_side.split(".")
            
            join_pair = [
                {"table": next(t["table"] for t in tables if t["alias"] == left_alias), "alias": left_alias, "on": left_column},
                {"table": next(t["table"] for t in tables if t["alias"] == right_alias), "alias": right_alias, "on": right_column}
            ]
            joins.append(join_pair)
        else:
            # It's a select condition (single table field comparison)
            if left_dots == 1:
                alias = left_side.split(".")[0]
            elif right_dots == 1:
                alias = right_side.split(".")[0]
            selects.append({
                "left": left_side,
                "operator": operator,
                "right": right_side,
                "alias": alias
            })
    return joins, selects

def preprocess_query(sql_query):
    '''
    Main functio to preprocess the query
    - sql_query: The SQL query string (assuming it has no aggregation)
    returns: preprocessed data
    '''
    metadata = {
        "operation": "SELECT",
        "projections": [],
        "source": [],
        "joins": [],
        "selects": []
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

        # Extract joins and selects if any conditions exist in WHERE clause
        if where_match:
            where_clause = where_match.group(1).strip()
            joins, selects = parse_conditions(where_clause, tables)
            metadata["joins"] = joins
            metadata["selects"] = selects
    
    # Add a suffix to operation if there are joins
    if metadata["joins"]:
        metadata["operation"] += " + Join"

    return metadata

# Example usage
if __name__ == "__main__":
    sql_query =  '''
    SELECT C.name, O.id, P.description, S.stock
    FROM customer C, orders O, product P, supplier S
    WHERE C.c_custkey = O.o_custkey 
    AND O.o_productkey = P.p_productkey 
    AND P.p_supplierkey = S.s_supplierkey
    AND C.age > 25
    AND P.price < 100
    AND S.rating > 4
    '''
    
    
    '''
    SELECT C.name, O.id, P.description 
    FROM customer C, orders O, product P 
    WHERE C.c_custkey = O.o_custkey 
      AND O.o_productkey = P.p_productkey 
      AND C.age > 25
      AND P.price < 100
    '''

    '''
    SELECT C.name, O.id FROM customer C, orders O WHERE C.c_custkey = O.o_custkey AND C.age > 25
    '''

    '''
    SELECT C.name, O.id, P.description, S.stock
    FROM customer C, orders O, product P, supplier S
    WHERE C.c_custkey = O.o_custkey 
    AND O.o_productkey = P.p_productkey 
    AND P.p_supplierkey = S.s_supplierkey
    AND C.age > 25
    AND P.price < 100
    AND S.rating > 4
    '''
    '''
    SELECT C.name
    FROM customer C
    WHERE C.age > 25;
    '''
    metadata = preprocess_query(sql_query)
    print("Parsed Metadata:", metadata)


