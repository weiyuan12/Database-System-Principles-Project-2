from example import query_input_1
from pgconn import query_row_counts
Tuples={'lineitem': 6001215, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'customer': 150000, 'supplier': 10000, 'region': 5, 'nation': 25}

class QueryNode:
    '''
    QueryNode class
    - node_type: the type of the node (e.g. Source, Projection, Selection, Join)
    - value: the value of the node (e.g. table name, condition)
    - children: the list of child nodes
    - alias (optional params for Join): the alias of the node
    - id: the unique identifier of the node (auto-incremented, to prevent collisions)
    '''
    _id_counter = 1
    def __init__(self, node_type, value=None):
        self.node_type = node_type  
        self.value = value 
        self.children = []
        self.alias = []
        ## This part returns the tuples
        self.tuples=0
        self.IO_cost=0
        self.Q_type=None
        self.id = QueryNode._id_counter
        QueryNode._id_counter += 1

    def add_child(self, child_node):
        self.children.append(child_node)

    def add_alias(self, alias):
        self.alias.append(alias)

    def set_tuples(self, tuples):
        self.tuples=tuples
    
    def set_IO_cost(self,IO_cost):
        self.tuples=IO_cost

    def set_Type(self, Q_type):
        self.type=Q_type

    def get_alias(self):
        return self.alias
    
    def get_children(self):
        return self.children
    
    def get_tuples(self):
        return self.tuples
    
    def get_IO_cost(self):
        return self.IO_cost
    
    def get_Q_type(self):
        return self.Q_type
    
    def get_node_type(self):
        return self.node_type
    def __repr__(self, level=0):
        indent = "  " * level
        repr_str = f"{indent}{self.node_type}: {self.value} (ID: {self.id})\n"
        for child in self.children:
            repr_str += child.__repr__(level + 1)
        return repr_str
    
def get_db_metrics():
    '''
    Greg implement here:
    - select (index) 0,1,2 type of join
    - return: {
    join:""
    type:"string"
    cost:0
    }

    '''
    return query_row_counts()

def select_and_project(query_dict,source_alias,source_table):

    '''
    Select then project a source
    - query_dict: the processed query from preprocessing.py
    - source_alias: the alias of the source to be selected then projected
    returns: selection node
    '''
    
    #projections = []
    selections = []
    source_node=QueryNode("Source",source_alias)
    source_node.set_tuples(Tuples.get(source_table) )
    # Check for Selections (range queries)
    selection_node=None
    #projection_node=None
    # selection
    for m in range(len(query_dict["selects"])):
        selection_alias=query_dict["selects"][m]["alias"]
        if(source_alias == selection_alias):
            selections.append(query_dict["selects"][m]["left"]+query_dict["selects"][m]["operator"]+query_dict["selects"][m]["right"])
            selection_node=QueryNode("Selection",selections)
            selection_node.add_child(source_node)

    '''
    
    join_hashmap={}
    if len(query_dict["joins"])>0:
        join_hashmap=generate_join_hashmap(query_dict)

    for j in range(len(query_dict["projections"])):
        projections_alias=query_dict["projections"][j][0]
        if(source_alias == projections_alias):
            #  Get the column
            column=query_dict["projections"][j].split(".")[-1]
            #  column not in join_hashmap then append to the projections
            if(projections_alias in join_hashmap and column not in join_hashmap[projections_alias]):
                projections.append(column)
            # Add whatever is in the hashmap to the current projections
            if(projections_alias in join_hashmap):
                final_projections= projections+join_hashmap[projections_alias]
            else:
                final_projections= projections
            projection_node=QueryNode("Projection",final_projections)
            if selection_node is not None:
                projection_node.add_child(selection_node)
            else:
                projection_node.add_child(source_node)
    '''
    #return selection_node,projection_node
    return selection_node

def join_tables(query_dict,join_index,current_intermediate_relations):
    '''
    Join 2 tables from the bottom up
    - query_dict: the processed query from preprocessing.py
    - join_index: the index of the join to be performed in the query_dict
    - intermediate_relations: the array of checkpoint nodes (intermediate relations)
    returns: a new root, current set of intermediate relations
    '''
    # Build the tree bottom up (start with the source) (On^2)
    top_level_sources=[]
    join_node=None
    # Retrieve the alias of the node to join
    join_alias_1=query_dict["joins"][join_index][0]["alias"]
    join_alias_2=query_dict["joins"][join_index][1]["alias"]
    join_table_1=query_dict["joins"][join_index][0]["table"]
    join_table_2=query_dict["joins"][join_index][1]["table"]
    
    # list through intermediate_relations to find a as many possible join nodes that matches the joins, if they exist, join them
    intermediate_relations=[]
    for cp in current_intermediate_relations[:]:
        if(len(intermediate_relations)>1):
            break
        if join_alias_1 in cp.get_alias():
            intermediate_relations.append(cp)
        elif join_alias_2 in cp.get_alias():
            intermediate_relations.append(cp)
    # Remove items that made it to intermediate_relations from current_intermediate_relations
    updated_intermediate_relations = [cp for cp in current_intermediate_relations if cp not in intermediate_relations]
    # 1. Logic for 2 intermediate relations
    if len(intermediate_relations)==2:
        join=join_alias_1 + "." + query_dict["joins"][join_index][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][join_index][1]["on"]
        join_node = QueryNode("Join", join)
        aliases=[]
        for k in intermediate_relations:
            join_node.add_child(k)
            aliases=aliases+k.get_alias()
        print(aliases)
        join_node.add_alias(aliases)
        root=join_node
        updated_intermediate_relations.append(root)
        return root,updated_intermediate_relations
    
    # 2. Logic for 1 or 0 intermediate relations
    # if there is only 1 intermediate relation, it becomes the sole checkpoint, (Only 1 source has been joined)
    if(len(intermediate_relations)==1):
        checkpoint=intermediate_relations[0]
        # Updated intermediate_relations
        updated_intermediate_relations = [cp for cp in current_intermediate_relations if cp not in intermediate_relations]
    else:
        checkpoint= None
   
    if checkpoint and checkpoint.get_node_type() != "Join":
        raise ValueError("Checkpoint must be a Join node")
    
    # Loop through all sources to find the sources from these joins
    join_aliases=[join_alias_1,join_alias_2]
    join_tables=[join_table_1,join_table_2]
    for i in range(len(join_aliases)):
        # define the source
        source_alias=join_aliases[i]
        source_table=join_tables[i]
        # Perform selection and projection on the source if there is no checkpoint
        if checkpoint == None or (checkpoint and source_alias not in checkpoint.get_alias()):
            source_node=QueryNode("Source",source_alias)
            source_node.set_tuples(Tuples.get(source_table) )
            # Check for Selections (range queries)
            '''
            
            selection_node,projection_node=select_and_project(query_dict,source_alias,source_table)
                        
            if projection_node is not None:
                top_level_sources.append(projection_node)
            if selection_node is not None:
                top_level_sources.append(selection_node)
            else:
                top_level_sources.append(source_node)
            '''
            selection_node=select_and_project(query_dict,source_alias,source_table)
                    
            if selection_node is not None:
                top_level_sources.append(selection_node)
            else:
                top_level_sources.append(source_node)
    # Join all top_level_sources
    if(source_alias==join_alias_1):
        join=join_alias_1 + "." + query_dict["joins"][join_index][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][join_index][1]["on"]
        join_node = QueryNode("Join", join)

    elif(source_alias==join_alias_2):
        join=join_alias_1 + "." + query_dict["joins"][join_index][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][join_index][1]["on"]
        join_node = QueryNode("Join", join)

    for k in top_level_sources:
        join_node.add_child(k)
    
    # if there is a checkpoint add the aliases to it if they do not exist, and then add the checkpoint as the child
    if checkpoint is not None:
        curr_list_of_aliases=checkpoint.get_alias()

        for alias in curr_list_of_aliases:
            join_node.add_alias(alias)
        if join_alias_1 not in curr_list_of_aliases:
            join_node.add_alias(join_alias_1)
        if join_alias_2 not in curr_list_of_aliases:
            join_node.add_alias(join_alias_2)
        join_node.add_child(checkpoint)

    # add both aliases to the join node
    else:
        join_node.add_alias(join_alias_1)
        join_node.add_alias(join_alias_2)
    
    root=join_node
    updated_intermediate_relations.append(root)
    return root,updated_intermediate_relations

def build_query_tree(query_dict,join_order):
    '''
    Main function to build the query tree
    - query_dict: the processed query from preprocessing.py
    returns: next_top, the top of the query tree
    '''
    ## Used to store all intermediate relations
    intermediate_relations=[]
    # Cond #1 no join
    if(len(query_dict["joins"])==0 and len(query_dict["source"])==1):
        source_alias= query_dict["source"][0]["alias"]
        source_table= query_dict["source"][0]["table"]
        '''
        selection_node,projection_node=select_and_project(query_dict,source_alias,source_table)
                    
        if projection_node is not None:
            return projection_node
        elif selection_node is not None:
            return selection_node
        else:
            return QueryNode("Source",source_alias)
        '''
        selection_node=select_and_project(query_dict,source_alias,source_table)
        if selection_node is not None:
            return selection_node
        else:
            return QueryNode("Source",source_alias)

    # Cond #2 There are joins
    for i in join_order:
        # get the current top of the join as the checkpoint
        next_top, updated_intermediate_relations = join_tables(query_dict,i,intermediate_relations)
        intermediate_relations=updated_intermediate_relations
    
    return next_top

def get_nodes_and_edges(node):
    '''
    Get the nodes and edges of the query tree
    - node: QueryNode
    returns: nodes, edges
    '''
    nodes = []
    edges = []

    # Recurse to collect the nodes
    def traverse(node):
        nodes.append((node.id, node.node_type, node.value, node.IO_cost, node.tuples, node.Q_type))
        for child in node.children:
            edges.append((node.id, child.id))
            traverse(child)
    
    traverse(node)
    return nodes, edges

def generate_join_hashmap(query_input):
    '''
    Generates hashmap of joins and important columns
    - query_input: the query dict
    returns hashmap of joins
    '''
    hashmap = {}
    
    for join in query_input.get('joins', []):
        for segment in join:
            table_alias = segment['alias']
            on_condition = segment['on']
            
            # Add the 'on' condition to the corresponding table alias in the dictionary
            if table_alias not in hashmap:
                hashmap[table_alias] = []
            hashmap[table_alias].append(on_condition)
    
    return hashmap
# default is [0,1,2,3,4....,n-1] for n-1 joins
join_order=list(range(len(query_input_1["joins"])))
'''
# Build the query tree
query_tree = build_query_tree(query_input_3,join_order)

# Retrieve nodes and edges
nodes, edges = get_nodes_and_edges(query_tree)
'''

items=get_db_metrics()
print(items)

'''

# Print the query tree structure
print("Query Tree Structure:")
print(query_tree)

# Print the nodes and edges
print("\nNodes:")
for node_id, node_type, value in nodes:
    print(f"Node ID: {node_id}, Type: {node_type}, Value: {value}")

print("\nEdges:")
for parent_id, child_id in edges:
    print(f"Parent ID: {parent_id} -> Child ID: {child_id}")
'''


#{'lineitem': 6001215, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'customer': 150000, 'supplier': 10000, 'region': 5, 'nation': 25}
