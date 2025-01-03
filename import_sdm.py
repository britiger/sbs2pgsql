import sys
import os
import psycopg2 as dbaccess
import csv
import sqlite3

SDM_ROOT_DIR='/home/clorenz/src/standing-data/'

# TODO: 
#  - Check Aircarft + Airline
#    - if diff on basics (registration / model + name) 
#    - archive it

class sdm_import:
    db = None
    cur = None

    def __init__(self):
        self.db_init()

    def db_init(self):
        try:
            self.db = dbaccess.connect (host=os.getenv('SBS_DB_HOST', '127.0.0.1'), port=os.getenv('SBS_DB_PORT', '5432'), database=os.getenv('SBS_DB_DATABASE', 'sbs'), user=os.getenv('SBS_DB_USER', 'sbs'), password=os.getenv('SBS_DB_PASSWORD', 'sbs'))
            self.cur = self.db.cursor()
        except:
            print ("Unable to connect to the database")
            sys.exit(1)

    def make_empty_null(self, array):
        return [v if v != '' else None for v in array]

    def insert_sdm_airline(self, entry):
        query = 'INSERT INTO sdm_airlines (code, name, icao, iata, PositioningFlightPattern, CharterFlightPattern) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name, iata = EXCLUDED.iata, icao = EXCLUDED.icao, PositioningFlightPattern=EXCLUDED.PositioningFlightPattern, CharterFlightPattern=EXCLUDED.CharterFlightPattern'
        data = (entry[0],entry[1],entry[2],entry[3],entry[4],entry[5])
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add airline: '+ str(e))
            self.db.rollback()

    def insert_sdm_codeblock(self, entry):
        query = 'INSERT INTO sdm_codeblocks (Start, Finish, Count, Bitmask, SignificantBitmask, IsMilitary, CountryISO2) VALUES (%s, %s, %s, %s, %s, %s, %s)'
        data = (entry[0],entry[1],entry[2],entry[3],entry[4],entry[5],entry[6])

        self.cur.execute(query, data)

    def insert_aircraft(self, entry, table="sdm"):
        query = 'INSERT INTO ' + table + '_aircrafts (ICAO, Registration, ModelICAO, Manufacturer, Model, ManufacturerAndModel, IsPrivateOperator, Operator, AirlineCode, SerialNumber, YearBuilt) VALUES (%s, %s, %s, %s, %s, %s, %s::boolean, %s, %s, %s, %s) ON CONFLICT (icao) DO UPDATE SET Registration=EXCLUDED.Registration, ModelICAO=EXCLUDED.ModelICAO, Manufacturer=EXCLUDED.Manufacturer, Model=EXCLUDED.Model, ManufacturerAndModel=EXCLUDED.ManufacturerAndModel, IsPrivateOperator=EXCLUDED.IsPrivateOperator, Operator=EXCLUDED.Operator, AirlineCode=EXCLUDED.AirlineCode, SerialNumber=EXCLUDED.SerialNumber, YearBuilt=EXCLUDED.YearBuilt'
        data = (entry[0],entry[1],entry[2],entry[3],entry[4],entry[5],entry[6],entry[7],entry[8],entry[9],entry[10])
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add aircraft: '+ str(e))
            print(str(entry))
            self.db.rollback()

    def insert_sdm_airport(self, entry):
        query = 'INSERT INTO sdm_airports (Code, Name, ICAO, IATA, Location, CountryISO2, Latitude, Longitude, AltitudeFeet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (code) DO UPDATE SET Name=EXCLUDED.Name, ICAO=EXCLUDED.ICAO, IATA=EXCLUDED.IATA, Location=EXCLUDED.Location, CountryISO2=EXCLUDED.CountryISO2, Latitude=EXCLUDED.Latitude, Longitude=EXCLUDED.Longitude, AltitudeFeet=EXCLUDED.AltitudeFeet'
        data = (entry[0],entry[1],entry[2],entry[3],entry[4],entry[5],entry[6],entry[7],entry[8])
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add airport: '+ str(e))
            self.db.rollback()

    def insert_sdm_route(self, entry):
        query = 'INSERT INTO sdm_routes (callsign, code, num, AirlineCode, AirportCodes) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (callsign) DO UPDATE SET code=EXCLUDED.code, num=EXCLUDED.num, AirlineCode=EXCLUDED.AirlineCode, AirportCodes=EXCLUDED.AirportCodes'
        data = (entry[0],entry[1],entry[2],entry[3],entry[4])
        self.cur.execute(query, data)

    def insert_sdm_country(self, entry):
        query = 'INSERT INTO sdm_countries (ISO, name) VALUES (%s, %s)'
        data = (entry[0],entry[1])

        self.cur.execute(query, data)

    def read_sdm_airlines(self):
        airline_file=SDM_ROOT_DIR+'airlines/schema-01/airlines.csv'

        with open(airline_file, 'r', newline='') as infile:
            reader = csv.reader(infile)
            next(reader, None) 
            for r in reader:
                if len(r[0]) == 3:
                    self.insert_sdm_airline(self.make_empty_null(r))

    def read_sdm_aircrafts(self):
        aircraft_dir=SDM_ROOT_DIR+'aircraft/schema-01/'

        for root, dirs, files in os.walk(aircraft_dir):
            for name in files:
                if not name.endswith('.csv'):
                    continue
                aircraft_file=os.path.join(root, name)
                print("Process " + aircraft_file + " ...")
                with open(aircraft_file, 'r', newline='') as infile:
                    reader = csv.reader(infile)
                    next(reader, None) 
                    for r in reader:
                        self.insert_aircraft(self.make_empty_null(r))

    def read_basestation_aircrafts(self):
        use_flag_code = False
        concat_man_type = True
        bs_con = sqlite3.connect("/home/clorenz/Downloads/Basestation.sqb")
        bs_cur = bs_con.cursor()

        query = 'TRUNCATE TABLE basestation_aircarfts;'
        self.cur.execute(query)

        flag_code = "OperatorFlagCode" if use_flag_code else "''"
        concat_man = "Manufacturer || ' ' ||" if concat_man_type else ""

        for r in bs_cur.execute("SELECT ModeS, Registration, ICAOTypeCode, Manufacturer, Type, " + concat_man + " Type, (RegisteredOwners='Private'), RegisteredOwners, " + flag_code + ", SerialNo, YearBuilt FROM Aircraft WHERE ICAOTypeCode NOT LIKE '-%'"):
            self.insert_aircraft(self.make_empty_null(r), "basestation")
        bs_con.close()

    def update_mat_views(self):
        query = 'REFRESH MATERIALIZED VIEW aircrafts;'
        self.cur.execute(query)
        self.db.commit()

    def read_sdm_airports(self):
        airport_dir=SDM_ROOT_DIR+'airports/schema-01/'

        for root, dirs, files in os.walk(airport_dir):
            for name in files:
                if not name.endswith('.csv'):
                    continue
                airport_file=os.path.join(root, name)
                print("Process " + airport_file + " ...")
                with open(airport_file, 'r', newline='') as infile:
                    reader = csv.reader(infile)
                    next(reader, None) 
                    for r in reader:
                        self.insert_sdm_airport(self.make_empty_null(r))

    def read_sdm_routes(self):
        route_dir=SDM_ROOT_DIR+'routes/schema-01/'

        for root, dirs, files in os.walk(route_dir):
            for name in files:
                if not name.endswith('.csv'):
                    continue
                route_file=os.path.join(root, name)
                print("Process " + route_file + " ...")
                try:
                    with open(route_file, 'r', newline='') as infile:
                        reader = csv.reader(infile)
                        next(reader, None) 
                        for r in reader:
                            if len(r[1]) == 3:
                                self.insert_sdm_route(self.make_empty_null(r))
                        self.db.commit()
                except Exception as e:
                    print('Failed to add routes: '+ str(e))
                    self.db.rollback()

    def read_sdm_codeblocks(self):
        codeblock_file=SDM_ROOT_DIR+'code-blocks/schema-01/code-blocks.csv'
        
        query = 'TRUNCATE TABLE sdm_codeblocks;'
        self.cur.execute(query)

        with open(codeblock_file, 'r', newline='') as infile:
            reader = csv.reader(infile)
            next(reader, None)
            try:
                for r in reader:
                    self.insert_sdm_codeblock(self.make_empty_null(r))
                self.db.commit()
            except Exception as e:
                print('Failed to add codeblock: '+ str(e))
                self.db.rollback()

    def read_sdm_countries(self):
        country_file=SDM_ROOT_DIR+'countries/schema-01/countries.csv'
        
        query = 'TRUNCATE TABLE sdm_countries;'
        self.cur.execute(query)

        with open(country_file, 'r', newline='') as infile:
            reader = csv.reader(infile)
            next(reader, None)
            try:
                for r in reader:
                    self.insert_sdm_country(self.make_empty_null(r))
                self.db.commit()
            except Exception as e:
                print('Failed to add country: '+ str(e))
                self.db.rollback()

import_sdm = sdm_import()
import_sdm.read_sdm_airlines()
import_sdm.read_sdm_airports()
import_sdm.read_sdm_codeblocks()
import_sdm.read_sdm_countries()
import_sdm.read_sdm_aircrafts()
import_sdm.read_sdm_routes()
#import_sdm.read_basestation_aircrafts()
import_sdm.update_mat_views()
