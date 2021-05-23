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
# Database Parameter
export SBS_DB_HOST=127.0.0.1
export SBS_DB_PORT=5432
export SBS_DB_DATABASE=sbs
export SBS_DB_USER=sbs
export SBS_DB_PASSWORD=sbs
# BaseStation Connection
export SBS_HOST=127.0.0.1
export SBS_HOST=30003
```

Start
-----

```
python3 sbs2pgsql.py
```

To sum the single messages you can call `process_flights.sql` to process flights older than 1 hour and create a line per flight in table `processedFlights`:
```
export PGPASSWORD=sbs
psql -h 127.0.0.1 -U sbs -f process_flights.sql
```
