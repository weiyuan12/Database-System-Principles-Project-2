"# Database-System-Principles-Project-2" 


### Run the interface.py to launch the application

```bash
python interface.py
```


### TPC-H
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


5. Restarting
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