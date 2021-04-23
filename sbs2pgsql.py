import sys
import os
import psycopg2 as dbaccess
from netcat import SBSConnection
from sbs1 import parse as sbs1_parse

try:
    db = dbaccess.connect (host=os.getenv('SBS_DB_HOST', '127.0.0.1'), port=os.getenv('SBS_DB_PORT', '5432'), database=os.getenv('SBS_DB_DATABASE', 'sbs'), user=os.getenv('SBS_DB_USER', 'sbs'), password=os.getenv('SBS_DB_PASSWORD', 'sbs'))
except:
    print ("Unable to connect to the database")
    sys.exit(1)

connection = SBSConnection(ip=os.getenv('SBS_HOST', '127.0.0.1'), port=os.getenv('SBS_PORT', '30003'))

cur = db.cursor()

while True:
    line = connection.read_line()
    parsed = sbs1_parse(line)
    if parsed:
        query = "INSERT INTO messages (MessageType, TransmissionType, SessionID, AircraftID, HexIdent, FlightID, MessageGenerated, MessageLogged, Callsign, Altitude, GroundSpeed, Track, Latitude, Longitude, VerticalRate, Squawk, Alert, Emergency, SPI, OnGround) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        data = (parsed['messageType'], parsed['transmissionType'], parsed['sessionID'], parsed['aircraftID'], parsed['icao24'], parsed['flightID'], parsed['generatedDate'], parsed['loggedDate'], parsed['callsign'], parsed['altitude'], parsed['groundSpeed'], parsed['track'], parsed['lat'], parsed['lon'], parsed['verticalRate'], parsed['squawk'], parsed['alert'], parsed['emergency'], parsed['spi'], parsed['onGround'])
        cur.execute(query, data)
        db.commit()
