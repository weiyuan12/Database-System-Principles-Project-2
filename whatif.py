from constants import query_input_1
from pgconn import get_blocks,get_unique_count
from constants import SCANS,JOINS,FILTERS
import math

class QueryNode:
    '''
    QueryNode class
    - node_type: the type of the node (e.g. Source, Projection, Selection, Join)
    - value: the value of the node (e.g. table name, condition)
    - children: the list of child nodes
    - alias (optional params for Join): the alias of the node
    - tuples: tuples for that node
    - IO_cost: IO cost for that node
    - Q_type: The query type (choose from SCANS, JOINS and FILTERS depending on the query type)
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
        self.Q_type="None"
        self.id = QueryNode._id_counter
        QueryNode._id_counter += 1

    def add_child(self, child_node):
        self.children.append(child_node)

    def add_alias(self, alias):
        self.alias.append(alias)

    def set_tuples(self, tuples):
        self.tuples=tuples
    
    def set_IO_cost(self,IO_cost):
        self.IO_cost=IO_cost

    def set_Q_Type(self, Q_type):
        self.Q_type=Q_type

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
    
def set_source_IO(query_dict_itm,table_name):
    '''
    query_dict_itm: the object from the query dict
    node: selection or source node
    mode: source or selects
    '''
    number_of_blocks=get_blocks(table_name)
    
    selectivity = 0.5 # default selectivity value
    matching_blocks = int(number_of_blocks * selectivity)
    if query_dict_itm['type'] in SCANS:
        if query_dict_itm['type']==SCANS[0]:
            ## seq
            return number_of_blocks
        else:
            ## bitmap and index
            bitmap_index_cost = int(math.log2(number_of_blocks))
            return bitmap_index_cost + matching_blocks
    else:
        # Hash
        return number_of_blocks
    
def set_selection_tuples(query_dict_itm,node,table_name,columns):
    '''
    Get the number of tuples from a relation
    - query_dict_itm: the selection obj from the query dict
    - node: the selection node
    
    '''

    #number_of_blocks=get_blocks(table_name)
    number_of_tuples=0
    # selection means that there is only 1 child
    child = node.get_children()[0]
    number_of_child_tuples=child.get_tuples()
    number_of_tuples=number_of_child_tuples
    for column in columns:
        
        V=get_unique_count(table_name,column)
        if V is None:
            V=number_of_tuples
        
        if query_dict_itm['operator']=="<" or query_dict_itm['operator']==">":
            # divide by 3
            number_of_child_tuples=number_of_child_tuples/3
        elif query_dict_itm['operator']=="=":
            number_of_child_tuples=number_of_child_tuples/V
        elif query_dict_itm['operator']=="!=":
            number_of_child_tuples=number_of_child_tuples(V-1)/V
    return number_of_child_tuples
   
def set_join_tuple_and_IO(query_dict_itm,join_node,M):
    '''
    Get the join tuple and IO
    - query_dict_itm: the join obj from the query dict
    - join_node: the join node
    Parse outer relation to be smaller
    '''    
    def get_relation_data(query_item, child_node):
        """
        Get relational_data
        """
        tuples = child_node.get_tuples()
        blocks = get_blocks(query_item['table'])
        unique_count = get_unique_count(query_item['table'], query_item['on'])
        if unique_count is None:  # If it's a key, set unique count to the number of tuples
            unique_count = tuples
        return tuples, blocks, unique_count

    # Determine which child is outer or inner based on alias matching
    if query_dict_itm[0]['alias'] in join_node.get_children()[0].get_alias():
        outer_child, inner_child = 0, 1
    else:
        outer_child, inner_child = 1, 0

    # Fetch data for both relations
    tuples_1, block_1, V_1 = get_relation_data(query_dict_itm[outer_child], join_node.get_children()[outer_child])
    tuples_2, block_2, V_2 = get_relation_data(query_dict_itm[inner_child], join_node.get_children()[inner_child])
    
    # Hash
    if(query_dict_itm[0]['type']==JOINS[0]):
        n_IO= 3 * (block_1 + block_2)
    # Nested loop
    elif query_dict_itm[0]['type']==JOINS[1]:
        if block_1< block_2:
            n_IO=block_1 + (block_1 * block_2)/(M-1)
        else:
            n_IO=block_2 + (block_1 * block_2)/(M-1)

    # Merge
    elif query_dict_itm[0]['type']==JOINS[2]:
        n_IO = block_1 + block_2
    # Index/Index only
    else:
        if block_1< block_2:
            n_IO=block_1 + (tuples_1 * block_2)/V_2
        else:
            n_IO=block_2 + (tuples_2 * block_1)/V_1
    ## Est tuples:
    tuples = (tuples_1 * tuples_2) / max(V_1,V_2)
    return n_IO,tuples
    
def select_and_project(query_dict,source_alias,source_table,scan_type,use_dict_IO_tuples,Tuples):

    '''
    Select a source
    - query_dict: the processed query from preprocessing.py
    - source_alias: the alias of the source to be selected then projected
    returns: selection node
    '''
    
    #projections = []
    selections = []
    source_node=QueryNode("Source",source_alias)
    # If cannot get from source table, then use alias
    if use_dict_IO_tuples==False:
        tuples_value = Tuples.get(source_table)
        if tuples_value is not None:
            source_node.set_tuples(tuples_value)
            
            for i in range(len(query_dict['source'])):
                if query_dict['source'][i]['alias'].lower()==(source_alias.lower()):
                    IO_cost = set_source_IO(query_dict['source'][i],source_table)
                    source_node.set_IO_cost(IO_cost)
                    break
        else:
            for key in Tuples:
                if key.lower().startswith(source_alias.lower()):
                    source_node.set_tuples(Tuples[key])
                    # key match
                    for i in range(len(query_dict['source'])):
                        if query_dict['source'][i]['alias'].lower()==(source_alias.lower()):
                            #print("alias")
                            IO_cost = set_source_IO(query_dict['source'][i],source_table)
                            source_node.set_IO_cost(IO_cost)
                    break
    else:
        for i in range(len(query_dict["source"])):
            if query_dict["source"][i]["alias"].lower().startswith(source_alias.lower()):
                source_node.set_tuples(query_dict["source"][i]["tuples"])
                source_node.set_IO_cost(float(query_dict["source"][i]["IO_cost"]))
                
    source_node.set_Q_Type(scan_type)
    # Check for Selections (range queries)
    selection_node=None
    #projection_node=None
    # selection (if there is)
    columns=[]
    for m in range(len(query_dict["selects"])):
        selection_alias=query_dict["selects"][m]["alias"]
        if(source_alias == selection_alias):
            selections.append(query_dict["selects"][m]["left"]+query_dict["selects"][m]["operator"]+query_dict["selects"][m]["right"])
            selection_node=QueryNode("Selection",selections)
            selection_node.add_child(source_node)
            selection_node.set_Q_Type(query_dict["selects"][m]["type"])
            if "(" in query_dict["selects"][m]["left"]:
                columns.append(query_dict["selects"][m]["left"].split('(')[1])
            elif "(" in query_dict["selects"][m]["right"]:
                columns.append(query_dict["selects"][m]["right"].split('(')[1])
            elif "." in query_dict["selects"][m]["left"]:
                columns.append(query_dict["selects"][m]["left"].split('.')[1])
            elif "." in query_dict["selects"][m]["right"]:
                columns.append(query_dict["selects"][m]["right"].split('.')[1])
    if selection_node is not None:
        if use_dict_IO_tuples:
            selection_node.set_tuples(query_dict["selects"][m]["tuples"])
            # !!scan is done in the first step:
            selection_node.set_IO_cost(0)
        
        else: #use our own estimation
            # !!scan is done in the first step:
            selection_node.set_IO_cost(0)
            tuples=set_selection_tuples(query_dict["selects"][m], selection_node,source_table,columns)
            selection_node.set_tuples(tuples)
    
    return selection_node

def join_tables(query_dict,join_index,current_intermediate_relations,use_dict_IO_tuples,Tuples,M):
    '''
    Join 2 tables from the bottom up
    - query_dict: the processed query from preprocessing.py
    - join_index: the index of the join to be performed in the query_dict
    - intermediate_relations: the array of checkpoint nodes (intermediate relations)
    - use_dict_IO_tuples: whether to use the dictionary of IO costs and tuples
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
    # 1. Logic for joining 2 intermediate relations
    if len(intermediate_relations)==2:
        print("intermediate_relations 2")
        join=join_alias_1 + "." + query_dict["joins"][join_index][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][join_index][1]["on"]
        join_node = QueryNode("Join", join)
        join_node.set_Q_Type(query_dict["joins"][join_index][0]["type"])
        aliases=[]
        for k in intermediate_relations:
            join_node.add_child(k)
            aliases=aliases+k.get_alias()
        
        join_node.add_alias(aliases)
        if use_dict_IO_tuples:
            join_node.set_tuples(query_dict["joins"][join_index][0]["tuples"])
            join_node.set_IO_cost(query_dict["joins"][join_index][0]["IO_cost"])
        else:
            IO, tuples = set_join_tuple_and_IO(query_dict["joins"][join_index],join_node,M)
            join_node.set_IO_cost(IO)
            join_node.set_tuples(tuples)
            
        root=join_node
        updated_intermediate_relations.append(root)
        
        return root,updated_intermediate_relations
    
    # 2. Logic for joining 1 or 0 intermediate relations with a source/selection relation
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

        for x in query_dict["source"]:
            if x["alias"]==source_alias:
                source_Q_type=x["type"]
                break
            else: source_Q_type = "None"
        # Perform selection and projection on the source if there is no checkpoint
        if checkpoint == None or (checkpoint and source_alias not in checkpoint.get_alias()):
            source_node=QueryNode("Source",source_alias)
            if use_dict_IO_tuples==False:
                tuples_value = Tuples.get(source_table)
                if tuples_value is not None:
                    source_node.set_tuples(tuples_value)
                    for i in range(len(query_dict['source'])):
                        if query_dict['source'][i]['alias'].lower()==(source_alias.lower()):
                            #print("alias")
                            IO_cost = set_source_IO(query_dict['source'][i],source_table)
                            source_node.set_IO_cost(IO_cost)
                            break
                else:
                    for key in Tuples:
                        if key.lower().startswith(source_alias.lower()):
                            source_node.set_tuples(Tuples[key])
                            for i in range(len(query_dict['source'])):
                                if query_dict['source'][i]['alias'].lower()==(source_alias.lower()):
                                    #print("alias")
                                    IO_cost = set_source_IO(query_dict['source'][i],source_table)
                                    source_node.set_IO_cost(IO_cost)
                            break
            else:
                for i in range(len(query_dict["source"])):
                    if query_dict["source"][i]["alias"].lower().startswith(source_alias.lower()):
                        source_node.set_tuples(query_dict["source"][i]["tuples"])
                        source_node.set_IO_cost(query_dict["source"][i]["IO_cost"])
            source_node.set_Q_Type(source_Q_type)
            # Check for Selections (range queries)
            selection_node=select_and_project(query_dict,source_alias,source_table,source_Q_type,use_dict_IO_tuples,Tuples)
                    
            if selection_node is not None:
                top_level_sources.append(selection_node)
            else:
                top_level_sources.append(source_node)
    # Join all top_level_sources
    if(source_alias==join_alias_1):
        join=join_alias_1 + "." + query_dict["joins"][join_index][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][join_index][1]["on"]
        join_node = QueryNode("Join", join)
        join_node.set_Q_Type(query_dict["joins"][join_index][0]["type"])
    elif(source_alias==join_alias_2):
        join=join_alias_1 + "." + query_dict["joins"][join_index][0]["on"] + " = " +join_alias_2+ "." +query_dict["joins"][join_index][1]["on"]
        join_node = QueryNode("Join", join)
        join_node.set_Q_Type(query_dict["joins"][join_index][0]["type"])

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
    if use_dict_IO_tuples:
        join_node.set_tuples(query_dict["joins"][join_index][0]["tuples"])
        join_node.set_IO_cost(query_dict["joins"][join_index][0]["IO_cost"])
    else:
        IO, tuples = set_join_tuple_and_IO(query_dict["joins"][join_index],join_node,M)
        join_node.set_IO_cost(IO)
        join_node.set_tuples(tuples)
    root=join_node
    updated_intermediate_relations.append(root)
    return root,updated_intermediate_relations

def build_query_tree(query_dict,join_order,use_dict_IO_tuples,Tuples,M):
    '''
    Main function to build the query tree
    - query_dict: the processed query from preprocessing.py
    - join_order: the order of joins to be performed
    - use_dict_IO_tuples: whether to use the dictionary of IO costs and tuples
    returns: next_top, the top of the query tree
    '''
    ## Used to store all intermediate relations
    intermediate_relations=[]
    # Cond #1 no join
    if(len(query_dict["joins"])==0 and len(query_dict["source"])==1):
        source_alias= query_dict["source"][0]["alias"]
        source_table= query_dict["source"][0]["table"]
        source_Q_type=query_dict["source"][0]["type"]
       
        selection_node=select_and_project(query_dict,source_alias,source_table,source_Q_type,use_dict_IO_tuples,Tuples)
        if selection_node is not None:
            return selection_node
        else:
            return QueryNode("Source",source_alias)

    # Cond #2 There are joins
    for i in join_order:
        # get the current top of the join as the checkpoint
        next_top, updated_intermediate_relations = join_tables(query_dict,i,intermediate_relations,use_dict_IO_tuples,Tuples,M)
        intermediate_relations=updated_intermediate_relations
        print(intermediate_relations)
        #print(next_top)
    if (len(intermediate_relations)>1):
        
        print("more than 1 intermeidate relations, trying to find the perfect join")
        
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

def total_IO_cost(root):
    '''
    Calculate the total IO cost of the query tree by traversing
    - nodes: the list of nodes in the query tree
    returns: total IO cost
    '''
    total_cost = 0
    def compute_cost(node):
        nonlocal total_cost
        total_cost += node.get_IO_cost()
        for child in node.get_children():
            compute_cost(child)
    
    compute_cost(root)
    return total_cost
# default is [0,1,2,3,4....,n-1] for n-1 joins
join_order=list(range(len(query_input_1["joins"])))

#{'lineitem': 6001215, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'customer': 150000, 'supplier': 10000, 'region': 5, 'nation': 25}
