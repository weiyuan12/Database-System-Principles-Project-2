# Database-System-Principles-Project-2"


## Running application
Ensure that the postgres database is up and running before running the application.
### Run the project.py to launch the application

```bash
python project.py
```
### Running SQL queries
Due to the nature of our query parser, the SQL queries passed into interface.py need to follow a standard syntax. Join operations are done using the WHERE CLAUSE instead of the JOIN keyword. If there are syntax errors, there will be no output. Below is an example of a acceptable SQL query. More examples are found in project.py
Note: it is best done with 4 or less joins due to the tree size
```bash
SELECT 
        c.c_name AS customer_name,
        o.o_orderkey AS order_id,
        o.o_orderdate AS order_date,
        p.p_name AS part_name,
        s.s_name AS supplier_name,
        l.l_quantity AS quantity,
        l.l_extendedprice AS extended_price
    FROM 
        customer c,
        orders o,
        lineitem l,
        part p,
        partsupp ps,
        supplier s
    WHERE 
        c.c_custkey = o.o_custkey
        AND o.o_orderkey = l.l_orderkey
        AND l.l_partkey = p.p_partkey
        AND l.l_suppkey = s.s_suppkey
        AND p.p_partkey = ps.ps_partkey
        AND p.p_retailprice < 1000
```

## Installing dependencies
```bash
pip install -r requirements.txt
or 
pip install psycopg2
```

## Set Up TPC-H and Postgres
Prerequisites:
    - Windows OS
    - Make: https://www.gnu.org/software/make/#download
    - MinGW (windows): https://sourceforge.net/projects/mingw/
    Then add the make and MINGW to PATH: for eg: "C:\MinGW\bin" and "C:\Program Files (x86)\GnuWin32\bin"

1. Installation (not needed if .o files already exist)
```bash 
cd dbgen
make -f makefile.suite
```

2. Generate tables (in tbl format)
```bash
dbgen -s <size (In GB) of data>
```

eg:
```bash
dbgen -s 1
```

3. Start pdadmin and the database

- Start
```bash
docker-compose up -d
```

4. Run the pgadmin
pgadmin:
- username: postgres@admin.com
- password: password
connect to db
- host: postgres_benchmark
- port: 5432
- username: postgres
- password: password

5. If pgadmin not working, run pgadmin from commandline
```bash
psql -h localhost -p 5433 -U postgres
password: password
\l
\c benchmark_tpc_h
```


6. Restarting
- force recreate containers 
```bash
docker-compose up -d --force-recreate
```

- Stop the services
```bash
docker-compose down
```

- remove volumes
```bash
docker system prune
```

- remove containers
```bash
docker rm -f postgres_db pgadmin
```

## Repository Info
1. pgconn.py - To connect to the postgresdb
2. interface.py - To display interface
3. preprocessing.py - Preprocesses the input SQL and the postges QEP to display on interface.py
4. whatif.py - calculations and contraints

### Intermediate files (generated at runtime)
5. generated_our_QEP_structure.json - Intermediate JSON dump of preprocessing.py, representing SQL in data structure format that interface.py uses
6. generated_postgres_plan.txt - Output of calling EXPLAIN ANALYSE on SQL query using postgres
7. generated_postgres_query_plan_tree.txt - Intermediate output from preprocessing.py, showing a tree format of  generated_postgres_plan.txt
8. generated_postgres_query_plan_structured.json - Intermediate JSON dump of preprocessing.py, representing generated_postgres_plan.txt in data structure format that interface.py uses