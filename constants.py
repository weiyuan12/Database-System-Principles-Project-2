JOINS=['Hash Join','Nested Loop','Merge Join', "Index Scan"]
SCANS=['Seq Scan','Bitmap Heap Scan','Index Scan', 'Index Only Scan']
FILTERS = ['Filter','Hash Cond','Index Cond']
query_input_1 = {
    'operation': 'SELECT + Join',
    'source': [
        {
            'table': 'customer',
            'alias': 'c',
            'type': 'Seq Scan',
        },
        {
            'table': 'supplier',
            'alias': 's',
            'type': 'Seq Scan'
        },
        {
            'table': 'nation',
            'alias': 'n',
            'type': 'Seq Scan'
        },
        {
            'table': 'region',
            'alias': 'r',
            'type': 'Seq Scan'
        }
    ],
    'joins': [
        
        [
                {
                    'table': 'supplier',
                    'alias': 's',
                    'on': 's_nationkey',
                    'type': 'Hash Join'
                },
                {
                    'table': 'nation',
                    'alias': 'n',
                    'on': 'n_nationkey',
                    'type': 'Hash Join'
                }
            ],
        
        
            [
                {
                    'table': 'nation',
                    'alias': 'n',
                    'on': 'n_regionkey',
                    'type': 'Hash Join'
                },
                {
                    'table': 'region',
                    'alias': 'r',
                    'on': 'r_regionkey',
                    'type': 'Hash Join'
                }
            ],
    
        
            [
                {
                    'table': 'customer',
                    'alias': 'c',
                    'on': 'c_nationkey',
                    'type': 'Hash Join'
                },
                {
                    'table': 'nation',
                    'alias': 'n',
                    'on': 'n_nationkey',
                    'type': 'Hash Join'
                }
            ],
        
    ],
    'selects': [
        {
            'left': 'c.c_acctbal',
            'operator': '>',
            'right': '1000',
            'alias': 'c',
            'type': 'Seq Scan'
        }
    ]
}
