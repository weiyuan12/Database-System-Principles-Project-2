import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Where, Token
from sqlparse.tokens import Keyword, DML, Whitespace, Punctuation, Wildcard, Name, Literal, Operator

def parse_join_condition(condition_str):
    """
    Parse a join condition string into structured components.
    Example: "c.id = o.customer_id" becomes:
    {
        'table_1': 'c',
        'attribute_1': 'id',
        'table_2': 'o',
        'attribute_2': 'customer_id'
    }
    """
    # Clean up the condition string and normalize spaces
    condition_str = ' '.join(condition_str.split())
    
    # Remove any AND/OR operators
    parts = [p.strip() for p in condition_str.split() if p.upper() not in ('AND', 'OR')]
    
    # Find the equals sign
    equals_pos = -1
    for i, part in enumerate(parts):
        if part == '=':
            equals_pos = i
            break
            
    if equals_pos == -1:
        raise ValueError("No equals sign found in join condition")
    
    # Get left and right parts
    left_side = ''.join(parts[:equals_pos]).replace('.', ' ').strip()
    right_side = ''.join(parts[equals_pos + 1:]).replace('.', ' ').strip()
    
    # Parse left side
    left_parts = left_side.split()
    if len(left_parts) == 2:
        table_1, attribute_1 = left_parts
    else:
        table_1, attribute_1 = '', left_side
    
    # Parse right side
    right_parts = right_side.split()
    if len(right_parts) == 2:
        table_2, attribute_2 = right_parts
    else:
        table_2, attribute_2 = '', right_side
    
    return {
        'table_1': table_1.strip(),
        'attribute_1': attribute_1.strip(),
        'table_2': table_2.strip(),
        'attribute_2': attribute_2.strip()
    }

def parse_query(sql):
    query_structure = {
        'select': [],
        'tables': [],
        'join_condition': [],
        'where': []
    }
    
    parsed = sqlparse.parse(sql)[0]
    
    current_clause = None
    current_item = None
    join_condition = None
    
    for token in parsed.flatten():
        if token.is_whitespace:
            continue
            
        if token.ttype is DML or token.ttype is Keyword:
            upper_val = token.value.upper()
            if upper_val in ('SELECT', 'FROM', 'WHERE', 'JOIN', 'ON'):
                if upper_val == 'ON':
                    current_clause = 'JOIN_CONDITION'
                else:
                    current_clause = upper_val
                continue
        
        if current_clause == 'SELECT':
            if token.ttype is Wildcard:
                query_structure['select'].append('*')
            elif token.ttype is Name or token.ttype is Literal:
                name = token.value
                if current_item:
                    current_item += f".{name}"
                    query_structure['select'].append(current_item)
                    current_item = None
                else:
                    current_item = name
            elif token.value == '.':
                continue
            elif token.value == ',':
                if current_item:
                    query_structure['select'].append(current_item)
                    current_item = None
                
        elif current_clause == 'FROM':
            if token.ttype is Name:
                if token.value.upper() not in ('FROM', 'WHERE', 'JOIN', 'ON'):
                    name = token.value
                    if current_item:
                        query_structure['tables'].append({
                            'table': current_item,
                            'alias': name
                        })
                        current_item = None
                    else:
                        current_item = name
            elif token.value == ',':
                if current_item:
                    query_structure['tables'].append({
                        'table': current_item,
                        'alias': current_item
                    })
                    current_item = None

        elif current_clause == 'JOIN_CONDITION':
            if token.value.upper() not in ('ON', 'AND', 'OR'):
                if join_condition is None:
                    join_condition = token.value
                else:
                    join_condition += f" {token.value}"
            
            # Process the join condition when we hit AND/OR or at the end of the clause
            if token.value.upper() in ('AND', 'OR') and join_condition:
                structured_condition = parse_join_condition(join_condition)
                query_structure['join_condition'].append(structured_condition)
                join_condition = None
                    
        elif current_clause == 'WHERE':
            if token.is_whitespace or token.value == '.':
                continue
                
            if current_item is None:
                current_item = token.value
            else:
                current_item += f" {token.value}"
                
            if token.value in ('AND', 'OR'):
                if current_item:
                    condition = current_item.replace('AND', '').replace('OR', '').strip()
                    parts = condition.split()
                    if len(parts) >= 3:  # Changed from 4 to handle cases without table alias
                        if '.' in parts[0]:
                            table, attribute = parts[0].split('.')
                        else:
                            table = ''
                            attribute = parts[0]
                        
                        operator = None
                        value = None
                        for i, part in enumerate(parts):
                            if part in ['=', '>', '<', '>=', '<=', '!=']:
                                operator = part
                                value = ' '.join(parts[i+1:]).strip("'\"")
                                break
                        
                        query_structure['where'].append({
                            'table': table.strip(),
                            'attribute': attribute.strip(),
                            'operator': operator,
                            'value': value
                        })
                    current_item = None
    
    # Handle any remaining items
    if current_item:
        if current_clause == 'FROM':
            query_structure['tables'].append({
                'table': current_item,
                'alias': current_item
            })
        elif current_clause == 'WHERE':
            condition = current_item.strip()
            parts = condition.split()
            if len(parts) >= 3:  # Changed from 4 to handle cases without table alias
                if '.' in parts[0]:
                    table, attribute = parts[0].split('.')
                else:
                    table = ''
                    attribute = parts[0]
                
                operator = None
                value = None
                for i, part in enumerate(parts):
                    if part in ['=', '>', '<', '>=', '<=', '!=']:
                        operator = part
                        value = ' '.join(parts[i+1:]).strip("'\"")
                        break
                
                query_structure['where'].append({
                    'table': table.strip(),
                    'attribute': attribute.strip(),
                    'operator': operator,
                    'value': value
                })
        elif current_clause == 'SELECT':
            query_structure['select'].append(current_item)

    # Handle remaining join condition
    if join_condition:
        structured_condition = parse_join_condition(join_condition)
        query_structure['join_condition'].append(structured_condition)
    
    return query_structure

print("\nTesting Updated Parser:")
test_query = """
SELECT c.name, o.order_date 
FROM Customers c 
JOIN Orders o
ON c.id = o.customer_id 
WHERE c.country='USA' AND o.total > 100
"""

result = parse_query(test_query)
print("\nParsed Query Structure:")
for key, value in result.items():
    print(f"{key}: {value}")