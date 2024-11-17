JOINS=['Hash Join','Nested Loop','Merge Join', "Index Scan", "Index Only Scan"]
SCANS=['Seq Scan','Bitmap Heap Scan','Index Scan']
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


query_input_2={
  "operation": "SELECT + Join",
  "source": [
    {
      "table": "lineitem",
      "alias": "l",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 2500487
    },
    {
      "table": "part",
      "alias": "p",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 2099
    },
    {
      "table": "orders",
      "alias": "o",
      "type": "Index Scan",
      "IO_cost": 0.43,
      "tuples": 1
    },
    {
      "table": "customer",
      "alias": "c",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 62500
    },
    {
      "table": "supplier",
      "alias": "s",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 10000
    },
    {
      "table": "nation",
      "alias": "n",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 160
    },
    {
      "table": "region",
      "alias": "r",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 160
    }
  ],
  "joins": [
    [
      {
        "table": "nation",
        "alias": "n",
        "on": "n_regionkey",
        "type": "Hash Join",
        "IO_cost": 11098.78,
        "tuples": 2515
      },
      {
        "table": "region",
        "alias": "r",
        "on": "r_regionkey",
        "type": "Hash Join",
        "IO_cost": 11098.78,
        "tuples": 2515
      }
    ],
    [
      {
        "table": "customer",
        "alias": "c",
        "on": "c_nationkey",
        "type": "Hash Join",
        "IO_cost": 11085.18,
        "tuples": 2515
      },
      {
        "table": "nation",
        "alias": "n",
        "on": "n_nationkey",
        "type": "Hash Join",
        "IO_cost": 11085.18,
        "tuples": 2515
      }
    ],
    [
      {
        "table": "lineitem",
        "alias": "l",
        "on": "l_suppkey",
        "type": "Hash Join",
        "IO_cost": 11071.58,
        "tuples": 2515
      },
      {
        "table": "supplier",
        "alias": "s",
        "on": "s_suppkey",
        "type": "Hash Join",
        "IO_cost": 11071.58,
        "tuples": 2515
      }
    ],
    [
      {
        "table": "customer",
        "alias": "c",
        "on": "c_nationkey",
        "type": "Hash Join",
        "IO_cost": 11071.58,
        "tuples": 2515
      },
      {
        "table": "supplier",
        "alias": "s",
        "on": "s_nationkey",
        "type": "Hash Join",
        "IO_cost": 11071.58,
        "tuples": 2515
      }
    ],
    [
      {
        "table": "orders",
        "alias": "o",
        "on": "o_custkey",
        "type": "Hash Join",
        "IO_cost": 10598.58,
        "tuples": 62975
      },
      {
        "table": "customer",
        "alias": "c",
        "on": "c_custkey",
        "type": "Hash Join",
        "IO_cost": 10598.58,
        "tuples": 62975
      }
    ],
    [
      {
        "table": "lineitem",
        "alias": "l",
        "on": "l_partkey",
        "type": "Hash Join",
        "IO_cost": 5391.9,
        "tuples": 62975
      },
      {
        "table": "part",
        "alias": "p",
        "on": "p_partkey",
        "type": "Hash Join",
        "IO_cost": 5391.9,
        "tuples": 62975
      }
    ],
    [
      {
        "table": "orders",
        "alias": "o",
        "on": "o_orderkey",
        "type": "Index Scan",
        "IO_cost": 0.43,
        "tuples": 1
      },
      {
        "table": "lineitem",
        "alias": "l",
        "on": "l_orderkey",
        "type": "Index Scan",
        "IO_cost": 0.43,
        "tuples": 1
      }
    ]
  ],
  "selects": [
    {
      "left": "(p_retailprice",
      "operator": ">",
      "right": "1000",
      "alias": "p",
      "type": "Seq Scan",
      "IO_cost": 0.0,
      "tuples": 2099
    }
  ]
}