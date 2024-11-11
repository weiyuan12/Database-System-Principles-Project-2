query_input = {
    "operation": "SELECT + Join",
    ## Indicates which alisa to project
    "projections": [
        "C.name",
        "O.id"
    ],
    ## each item indicates a kind of join
    "source": [
        {
            "table": "customer",
            "alias": "C",
        },
        {
            "table": "orders",
            "alias": "O",
        }
    ],
    "joins":[
        [
        {   "table": "customer",
            "alias":"C",
            "on":"c_custkey"
        },
        {   "table": "orders",
            "alias":"O",
            "on":"o_custkey"
        }]
    ],
    'selects': []
}

query_input_2 = {
    'operation': 'SELECT + Join',
    'projections': [
        'C.name',
        'O.id'
    ],
    'source': [
        {
            'table': 'customer',
            'alias': 'C'
        },
        {
            'table': 'orders',
            'alias': 'O'
        }
    ],
    'joins': [
        [
            {
                'table': 'customer',
                'alias': 'C',
                'on': 'c_custkey'
            },
            {
                'table': 'orders',
                'alias': 'O',
                'on': 'o_custkey'
            }
        ]
    ],
    'selects': [
        {
            'left': 'C.age',
            'operator': '>',
            'right': '25',
            'alias': 'C'
        }
    ]
}

query_input_3={
    'operation': 'SELECT + Join',
    'projections': [
        'C.name',
        'O.id',
        'P.description'
    ],
    'source': [
        {
            'table': 'customer',
            'alias': 'C'
        },
        {
            'table': 'orders',
            'alias': 'O'
        },
        {
            'table': 'product',
            'alias': 'P'
        }
    ],
    'joins': [
        [
            {
                'table': 'customer',
                'alias': 'C',
                'on': 'c_custkey'
            },
            {
                'table': 'orders',
                'alias': 'O',
                'on': 'o_custkey'
            }
        ],
        [
            {
                'table': 'orders',
                'alias': 'O',
                'on': 'o_productkey'
            },
            {
                'table': 'product',
                'alias': 'P',
                'on': 'p_productkey'
            }
        ]
    ],
    'selects': [
        {
            'left': 'C.age',
            'operator': '>',
            'right': '25',
            'alias': 'C'
        },
        {
            'left': 'P.price',
            'operator': '<',
            'right': '100',
            'alias': 'P'
        }
    ]
}

query_input_4={
    'operation': 'SELECT + Join',
    'projections': [
        'C.name',
        'O.id',
        'P.description',
        'S.stock'
    ],
    'source': [
        {'table': 'customer', 'alias': 'C'},
        {'table': 'orders', 'alias': 'O'},
        {'table': 'product', 'alias': 'P'},
        {'table': 'supplier', 'alias': 'S'}
    ],
    'joins': [
        [
            {'table': 'customer', 'alias': 'C', 'on': 'c_custkey'},
            {'table': 'orders', 'alias': 'O', 'on': 'o_custkey'}
        ],
        [
            {'table': 'orders', 'alias': 'O', 'on': 'o_productkey'},
            {'table': 'product', 'alias': 'P', 'on': 'p_productkey'}
        ],
        [
            {'table': 'product', 'alias': 'P', 'on': 'p_supplierkey'},
            {'table': 'supplier', 'alias': 'S', 'on': 's_supplierkey'}
        ]
    ],
    'selects': [
        {'left': 'C.age', 'operator': '>', 'right': '25', 'alias': 'C'},
        {'left': 'P.price', 'operator': '<', 'right': '100', 'alias': 'P'},
        {'left': 'S.rating', 'operator': '>', 'right': '4', 'alias': 'S'}
    ]
}
