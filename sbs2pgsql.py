import sys
import os
import psycopg2 as dbaccess
from sbs1 import parse as sbs1_parse

try:
    db = dbaccess.connect (host=os.getenv('SBS_HOST', '127.0.0.1'), port=os.getenv('SBS_PORT', '5432'), database=os.getenv('SBS_DATABASE', 'sbs'), user=os.getenv('SBS_USER', 'sbs'), password=os.getenv('SBS_PASSWORD', 'sbs'))
except:
    print ("Unable to connect to the database")
    sys.exit(1)

cur = db.cursor()

for line in sys.stdin:
    parsed = sbs1_parse(line)
    if parsed:
        query = "INSERT INTO messages (MessageType, TransmissionType, SessionID, AircraftID, HexIdent, FlightID, MessageGenerated, MessageLogged, Callsign, Altitude, GroundSpeed, Track, Latitude, Longitude, VerticalRate, Squawk, Alert, Emergency, SPI, OnGround) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        data = (parsed['messageType'], parsed['transmissionType'], parsed['sessionID'], parsed['aircraftID'], parsed['icao24'], parsed['flightID'], parsed['generatedDate'], parsed['loggedDate'], parsed['callsign'], parsed['altitude'], parsed['groundSpeed'], parsed['track'], parsed['lat'], parsed['lon'], parsed['verticalRate'], parsed['squawk'], parsed['alert'], parsed['emergency'], parsed['spi'], parsed['onGround'])
        cur.execute(query, data)
        db.commit()
