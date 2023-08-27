sbs_pymodes.py
=============

Python script to put data from dump1090 output (Beast-Mode) into a PostgreSQL Database.

Requirements
------------
- Python3
- PostgreSQL 15 or higher
  - PostGIS Extension

Setup
-----

- Install Package psycopg2 for python3 (`pip3 install -r requirements.txt` or your favorite way).
- Setup database using sample `create_db.sql`
- If use other configuration set related variable in your terminal: 
```bash
# Database Parameter
export SBS_DB_HOST=127.0.0.1
export SBS_DB_PORT=5432
export SBS_DB_DATABASE=sbs
export SBS_DB_USER=sbs
export SBS_DB_PASSWORD=sbs
# Connection to dump1090 port for beast-mode
export SBS_HOST=127.0.0.1
export SBS_PORT=30005
```

Start
-----

```
python3 sbs_pymodes.py
```
