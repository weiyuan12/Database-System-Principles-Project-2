# Database-System-Principles-Project-2"


## Running application
Ensure that the postgres database is up and running before running the application.
### Run the interface.py to launch the application

```bash
python project.py
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