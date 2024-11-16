import re
from pgconn import query_row_counts,get_execution_plan
from constants import query_input_1
import json
from constants import JOINS,SCANS,FILTERS
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
        tables.append({"table": table_name, "alias": alias,'type':'Seq Scan'})
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
                {"table": next(t["table"] for t in tables if t["alias"] == left_alias), "alias": left_alias, "on": left_column,"type":"Hash"},
                {"table": next(t["table"] for t in tables if t["alias"] == right_alias), "alias": right_alias, "on": right_column,"type":"Hash"}
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
                "alias": alias,
                'type': SCANS[0]
            })
    return joins, selects

def preprocess_query(sql_query):
    '''
    Main function to preprocess the query
    - sql_query: The SQL query string (assuming it has no aggregation)
    returns: preprocessed data
    '''
    metadata = {
        "operation": "SELECT",
        "source": [],
        "joins": [],
        "selects": []
    }
    
    # Extract the main parts of the SQL query using regex
    #select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql_query, re.IGNORECASE | re.DOTALL)
    from_match = re.search(r"FROM\s+(.+?)\s*(WHERE|GROUP)", sql_query, re.IGNORECASE | re.DOTALL)
    where_match = re.search(r"WHERE\s+(.*?)(GROUP|ORDER|$)", sql_query, re.IGNORECASE | re.DOTALL)
    '''
    if select_match:
        select_clause = select_match.group(1).strip()
        metadata["projections"] = parse_projections(select_clause)
    '''
    
    
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


## WY: Parsing the output from EXPLAIN ANALYSE and Printing the tree top down in commandline (horizontal branches) -> need to be used print out in the gui maybe can store in txt file
def parse_execution_plan(plan):
    lines = plan.strip().splitlines()
    stack = []  # Stack to manage current position in the tree
    root = None  # The root of the tree
    node = None  # Current node being processed
    
    for line in lines:
        condition = None
        node_type=None
        indent_level = len(line) - len(line.lstrip())  # Calculate the level of indentation
        # Check for Hash Joins, Seq Scans, etc., and create nodes
        for join in JOINS:
            if join in line:
                node_type = join
                break
        if node_type is None:
            for scan in SCANS:
                if scan in line:
                    node_type = scan
                    break
        
        if node_type is not None:
            details = line.strip()
            new_node = {
                'type': node_type,
                'details': details,
                'children': []
            }
           
            if root == None:
                root = new_node
            else:
                if stack:
                    stack[-1]['children'].append(new_node)
            
            stack.append(new_node)
        
        elif node_type is None:
            # Handling filters or conditions
            for filter in FILTERS:
                if filter in line:
                    condition = line.split(":")[1].strip()
                    if stack:
                        stack[-1].setdefault('conditions', []).append(condition)
                    break
        # When moving up the tree (end of a node)
        elif line.strip() == "" or '->' in line and condition is None:
            stack.pop()

    return root
def print_tree(node, indent="", is_last=True, is_root=True):
    if not node:
        return
    
    # Construct the line for the current node
    if is_root:
        print(f"{indent}{node['type']}: {node['details']}")
    else:
        print(f"{indent}{'|' if not is_last else ' '}{' ' * (4 - len(indent))}------------------------")
        print(f"{indent}    {'|' if not is_last else ' '}{' ' * (4 - len(indent))}|")
        print(f"{indent}{' ' * (5 - len(indent))}{node['type']}: {node['details']}")

    # Print filter conditions if any
    if 'conditions' in node:
        for cond in node['conditions']:
            print(f"{indent}    {'|' if not is_last else ' '}{' ' * (4 - len(indent))}|")
            print(f"{indent}    {' ' * (5 - len(indent))}Filter Condition: {cond}")
    
    # Recursively print the children (if any)
    num_children = len(node.get('children', []))
    for i, child in enumerate(node.get('children', [])):
        is_last_child = i == num_children - 1
        print_tree(child, indent + ("    " if not is_last else " "), is_last_child, is_root=False)

def save_tree_to_file(node, filename, indent="", is_last=True, is_root=True, file=None):
    if not node:
        return
    
    # Create or append to file if this is the root call
    if is_root:
        file = open(filename, 'w', encoding='utf-8')
    
    # Prefix for child nodes
    child_prefix = "└──" if is_last else "├──"
    connection = "    " if is_last else "│   "
    
    # Construct the line for the current node
    if is_root:
        file.write(f"{node['type']}: {node['details']}\n")
    else:
        file.write(f"{indent}{child_prefix} {node['type']}: {node['details']}\n")

    # Write filter conditions if any
    if 'conditions' in node:
        for i, cond in enumerate(node['conditions']):
            is_last_condition = i == len(node['conditions']) - 1
            condition_prefix = "    │" if not is_last_condition or node.get('children') else "    └"
            file.write(f"{indent}{connection}{condition_prefix} Filter: {cond}\n")
    
    # Recursively write the children (if any)
    children = node.get('children', [])
    for i, child in enumerate(children):
        is_last_child = i == len(children) - 1
        save_tree_to_file(
            child, 
            filename, 
            indent + connection, 
            is_last_child, 
            is_root=False, 
            file=file
        )
    
    # Close the file if this is the root call
    if is_root:
        file.close()
def extract_cost_and_rows(details):
    """Extract cost and rows from the node details."""
    import re
    
    # Match patterns like "cost=529.08..92821.79 rows=49132944"
    match = re.search(r'cost=([\d.]+)\.\.([\d.]+) rows=(\d+)', details)
    if match:
        io_cost = float(match.group(1))  # Extract initial cost
        tuples_returned = int(match.group(3))  # Extract rows
        return io_cost, tuples_returned
    return None, None

def extract_table_info(details):
    """Extract table name and alias from node details"""
    import re
    
    # Match pattern like "Seq Scan on customer c" or "Seq Scan on customer"
    patterns = [
        r'(?:Index Scan using \w+ on|Index Only Scan using \w+ on|Seq Scan on|Hash Join on) (\w+)(?:\s+(\w+))?',
        r'Index Scan.*?on (\w+)(?:\s+(\w+))?'
    ]

    for pattern in patterns:
        match = re.search(pattern, details)
        if match:
            table = match.group(1)
            alias = match.group(2) if match.group(2) else table[0]
            return table, alias
    return None, None

def extract_join_condition(condition, is_index_join = False):
    """Extract table aliases and columns from join conditions"""
    if not condition:
        return None, None, None, None
    
    # Remove parentheses and split on equals
    condition = condition.replace('(', '').replace(')', '')
    print(condition)
    left, right = condition.split('=')
    
    # Extract table alias and column for left side

    if is_index_join == True:
        right = right.strip()
        right_parts = right.split('.')
        right_alias = right_parts[0].strip()
        right_col = right_parts[1].strip()
        return None, left.strip(), right_alias ,  right_col
    
    left = left.strip()
    left_parts = left.split('.')
    left_alias = left_parts[0].strip()
    left_col = left_parts[1].strip()
    
    # Extract table alias and column for right side
    right = right.strip()
    right_parts = right.split('.')
    right_alias = right_parts[0].strip()
    right_col = right_parts[1].strip()
    
    return left_alias, left_col, right_alias, right_col

def parse_execution_plan_to_dict(plan):
    """Parse execution plan and convert to structured dictionary format"""
    tree = parse_execution_plan(plan)  # Using your existing parser
    print(tree)
    result = {
        'operation': 'SELECT + Join',
        'source': [],
        'joins': [],
        'selects': []
    }
    
    def traverse_tree(node):
        if not node:
            return
        io_cost, tuples_returned = extract_cost_and_rows(node['details'])
        alias = None
        # Handle Seq Scan nodes (source tables)
        if node['type'] in SCANS:
            table, alias = extract_table_info(node['details'])
            if table:
                source_info = {
                    'table': table,
                    'alias': alias,
                    'type': node['type'],
                    'IO_cost': io_cost,
                    'tuples': tuples_returned
                }
                result['source'].append(source_info)
            
            # Handle filter conditions for selects
            if 'conditions' in node and node['type'] != "Index Scan":
                for condition in node['conditions']:
                    if '>' in condition or '<' in condition or '=' in condition:
                        parts = condition.split('>')
                        if len(parts) != 2:
                            parts = condition.split('<')
                        if len(parts) != 2:
                            parts = condition.split('=')
                        
                        if len(parts) == 2:
                            left, right = parts
                            select_info = {
                                'left': left.strip(),
                                'operator': '>',  # You might want to detect the actual operator
                                'right': right.strip().replace("'", ""),
                                'alias': alias,
                                'type': node['type'],
                                'IO_cost': io_cost,
                                'tuples': tuples_returned
                            }
                            result['selects'].append(select_info)
        print("----------", alias)
        # Handle Join nodes
        if node['type'] in JOINS:
            io_cost, tuples_returned = extract_cost_and_rows(node['details'])
            table, alias = extract_table_info(node['details'])
            print(alias)
            if 'conditions' in node:
                for condition in node['conditions']:
                    print(node['type'], condition)
                    if('AND' in condition or 'OR' in condition):
                        subconditions = re.split(r'\s+(?:AND|OR)\s+', condition, flags=re.IGNORECASE)
                        for subcondition in subconditions:
                            if node['type'] == "Index Scan":
                                left_alias, left_col, right_alias, right_col = extract_join_condition(subcondition, is_index_join=True)
                                left_alias = alias
                            else:
                                left_alias, left_col, right_alias, right_col = extract_join_condition(subcondition)
                            if left_alias or right_alias:
                                join_info = [
                                    {
                                        'table': next((s['table'] for s in result['source'] 
                                                    if s['alias'] == left_alias), left_alias),
                                        'alias': left_alias,
                                        'on': left_col,
                                        'type': node['type'],
                                        'IO_cost': io_cost,
                                        'tuples': tuples_returned
                                    },
                                    {
                                        'table': next((s['table'] for s in result['source'] 
                                                    if s['alias'] == right_alias), right_alias),
                                        'alias': right_alias,
                                        'on': right_col,
                                        'type': node['type'],
                                        'IO_cost': io_cost,
                                        'tuples': tuples_returned
                                    }
                                ]
                                result['joins'].append(join_info)
                    elif '>' in condition or '<' in condition or '=' in condition:
                        if node['type'] == "Index Scan":
                            left_alias, left_col, right_alias, right_col = extract_join_condition(condition, is_index_join=True)
                            left_alias = alias
                            print(left_alias, left_col, right_alias, right_col)
                        else:
                            left_alias, left_col, right_alias, right_col = extract_join_condition(condition)
                        if left_alias or right_alias:
                            join_info = [
                                {
                                    'table': next((s['table'] for s in result['source'] 
                                                if s['alias'] == left_alias), left_alias),
                                    'alias': left_alias,
                                    'on': left_col,
                                    'type': node['type'],
                                    'IO_cost': io_cost,
                                    'tuples': tuples_returned
                                },
                                {
                                    'table': next((s['table'] for s in result['source'] 
                                                if s['alias'] == right_alias), right_alias),
                                    'alias': right_alias,
                                    'on': right_col,
                                    'type': node['type'],
                                    'IO_cost': io_cost,
                                    'tuples': tuples_returned
                                }
                            ]
                            print(join_info)
                            result['joins'].append(join_info)
        
        # Traverse children
        for child in node.get('children', []):
            traverse_tree(child)
    
    traverse_tree(tree)
    return result

def process_query_plan_full(sql_query):
    """Process query plan and return both tree visualization and structured format"""
    plan = get_execution_plan(sql_query)
    
    # Generate tree visualization
    tree = parse_execution_plan(plan)

    with open("original_plan.txt", "w", encoding='utf-8') as f:
        f.write(plan)
    save_tree_to_file(tree, "query_plan_tree.txt")
    
    # Generate structured dictionary format
    structured_format = parse_execution_plan_to_dict(plan)
    
    # Save structured format to file
    with open("query_plan_structured.json", "w", encoding='utf-8') as f:
        json.dump(structured_format, f, indent=2)
    
    return tree, structured_format
    
# Example usage
if __name__ == "__main__":
    sql_query =  """
    SELECT 
        c.c_name AS customer_name,
        o.o_orderkey AS order_id,
        o.o_orderdate AS order_date,
        p.p_name AS part_name,
        s.s_name AS supplier_name,
        n.n_name AS nation_name,
        r.r_name AS region_name,
        l.l_quantity AS quantity,
        l.l_extendedprice AS extended_price
    FROM 
        customer c,
        orders o,
        lineitem l,
        part p,
        partsupp ps,
        supplier s,
        nation n,
        region r
    WHERE 
        c.c_custkey = o.o_custkey
        AND o.o_orderkey = l.l_orderkey
        AND l.l_partkey = p.p_partkey
        AND l.l_suppkey = s.s_suppkey
        AND p.p_partkey = ps.ps_partkey
        AND ps.ps_suppkey = s.s_suppkey
        AND c.c_nationkey = n.n_nationkey
        AND s.s_nationkey = n.n_nationkey
        AND n.n_regionkey = r.r_regionkey
        AND p.p_retailprice < 1000
    """
    """
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

    '''
    SELECT o.o_orderkey, o.o_orderdate, c.c_name, n.n_name AS nation, r.r_name AS region
    FROM orders o, customer c, nation n, region r
    WHERE o.o_custkey = c.c_custkey
    AND c.c_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND C.age > 25;    
    '''

    '''
    SELECT 
        c.c_name AS customer_name,
        o.o_orderkey AS order_id,
        o.o_orderdate AS order_date,
        p.p_name AS part_name,
        s.s_name AS supplier_name,
        n.n_name AS nation_name,
        r.r_name AS region_name,
        l.l_quantity AS quantity,
        l.l_extendedprice AS extended_price
    FROM 
        customer c,
        orders o,
        lineitem l,
        part p,
        partsupp ps,
        supplier s,
        nation n,
        region r
    WHERE 
        c.c_custkey = o.o_custkey
        AND o.o_orderkey = l.l_orderkey
        AND l.l_partkey = p.p_partkey
        AND l.l_suppkey = s.s_suppkey
        AND p.p_partkey = ps.ps_partkey
        AND ps.ps_suppkey = s.s_suppkey
        AND c.c_nationkey = n.n_nationkey
        AND s.s_nationkey = n.n_nationkey
        AND n.n_regionkey = r.r_regionkey
        AND p.p_retailprice < 1000
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
    #metadata = preprocess_query(sql_query)
    #print("Parsed Metadata:", metadata)
   

    #plan = get_execution_plan(sql_query)
    #print(plan)
    #tree = parse_execution_plan(plan)

    tree, structured_format = process_query_plan_full(sql_query)
    print_tree(tree)
    print(structured_format)
    print(json.dumps(structured_format, indent=2))

   
# Example EXPLAIN output and SELECT clause




