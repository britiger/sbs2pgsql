import sys
import os
import psycopg2 as dbaccess

class migrate_fam:
    db = None
    cur = None
    db_fam = None
    cur_fam = None


    def __init__(self):
        self.db_init()

    def db_init(self):
        try:
            self.db = dbaccess.connect (host=os.getenv('SBS_DB_HOST', '127.0.0.1'), port=os.getenv('SBS_DB_PORT', '5432'), database=os.getenv('SBS_DB_DATABASE', 'sbs'), user=os.getenv('SBS_DB_USER', 'sbs'), password=os.getenv('SBS_DB_PASSWORD', 'sbs'))
            self.cur = self.db.cursor()
        except:
            print ("Unable to connect to the sbs database")
            sys.exit(1)

        try:
            self.db_fam = dbaccess.connect (host=os.getenv('FAM_DB_HOST', '127.0.0.1'), port=os.getenv('FAM_DB_PORT', '5432'), database=os.getenv('FAM_DB_DATABASE', 'flightairmap'), user=os.getenv('FAM_DB_USER', 'flightairmap'), password=os.getenv('FAM_DB_PASSWORD', 'flightairmap'))
            self.cur_fam = self.db_fam.cursor()
        except:
            print ("Unable to connect to the fam database")
            sys.exit(1)

    def insert_from_fam(self, entry):
        query = 'INSERT INTO process2 (hexident, callsign, flightfromdate, flighttodate, msg) VALUES (%s,%s,%s,%s, 0)'
        data = (entry['icao'],entry['callsign'],entry['t_from'],entry['t_to'])
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add airport: '+ str(e))
            self.db.rollback()
    
    def entry_exists(self, entry):
        query = "SELECT * FROM process2 WHERE hexident=%s AND (flightfromdate, flighttodate) OVERLAPS (%s, %s)"
        data = (entry['icao'],entry['t_from'],entry['t_to'])
        try:
            self.cur.execute(query, data)
            if self.cur.fetchone():
                return True
            else:
                return False
        except Exception as e:
            print('Failed to query process: '+ str(e))
            self.db.rollback()
            return True

    def run(self):
        max_rows = 100

        # fetch only rows with first and last date
        # Times are utc
        # TODO: Limit to rows last 2hrs
        self.cur_fam.execute("SELECT modes, ident, date, last_seen FROM spotter_output WHERE last_seen IS NOT NULL ORDER BY last_seen DESC")

        while 1:
            rows = self.cur_fam.fetchmany(max_rows)
            if len(rows) == 0:
                break
            for r in rows:
                entry = {
                    'icao': r[0],
                    'callsign': r[1] if r[1] != '' else None,
                    't_from': str(r[2])+'+00:00',
                    't_to': str(r[3])+'+00:00'
                }
                if not self.entry_exists(entry):
                    print(str(r))
                    self.insert_from_fam(entry)

migrate_fam = migrate_fam()
migrate_fam.run()
