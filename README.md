sbs2python.py
=============

Python script to put data from BaseSation (SBS-1) output into a PostgreSQL Database.

Requirements
------------
- Python3
- PostgreSQL 12 or higher
  - PostGIS Extension

Setup
-----

- Install Package psycopg2 for python3 (`pip3 install -r requirements.txt` or your favorite way).
- Setup database using sample `create_db.sql`
- If use other configuration set related variable in your terminal: 
```bash
export SBS_HOST=127.0.0.1
export SBS_PORT=5432
export SBS_DATABASE=sbs
export SBS_USER=sbs
export SBS_PASSWORD=sbs
```

Start
-----

```
nc 127.0.0.1 30003 |  python3 sbs2pgsql.py
```