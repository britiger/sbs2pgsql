import sys
import os
import argparse
import csv
import psycopg2 as dbaccess

parser = argparse.ArgumentParser(description="Import Aircraft CSV into Database")
parser.add_argument("-O", "--opensky-file", help="File from opensky-network.org")
parser.add_argument("-F", "--flightaware-file", help="File from Fligthaware")
parser.add_argument("-C", "--custom-file", help="CSV-File, uses first 4 colums as HexIdent, Registration, IcaoType, IcaoOperator")
parser.add_argument("--replace", help="Replace existing data", action="store_true")
parser.add_argument("--replace-complete", help="Replace existing data if complete", action="store_true")
parser.add_argument("--clean", help="Remove all existing data", action="store_true")
args = parser.parse_args()

def addAircraft(hex, reg, icaotype, icaooper, filename):
    global added, skipped

    if icaotype == 'UNDEFINED' or icaotype == 'ground':
        icaotype = ''

    if not reg:
        print(hex + " Invalid Registration")
        skipped += 1
        return
    elif len(icaotype) > 4:
        print(hex + " Invalid ICAO Type: " + icaotype)
        skipped += 1
        return
    elif icaooper is not None and len(icaooper) > 3:
        print(hex + " Invalid ICAO Operator: " + icaooper)
        skipped += 1
        return
    elif len(hex) != 6:
        print(hex + " Invalid 24bit Address")
        skipped += 1
        return
    # print(hex + "|" + reg + "|"+ icaotype+ "|"+ icaooper)

    if args.replace or (args.replace_complete and icaotype is not None and icaotype != '' and icaooper is not None and icaooper != ''):
        upsertAircraft(hex, reg, icaotype, icaooper, filename)
        return

    query = "INSERT INTO Aircrafts (HexIdent, Registration, IcaoType, IcaoOperator, SourceFile) VALUES (UPPER(%s),UPPER(%s),UPPER(%s),UPPER(%s),%s) ON CONFLICT DO NOTHING RETURNING (SELECT Registration FROM Aircrafts WHERE HexIdent=UPPER(%s)) AS OldReg;"
    data = (hex, reg, icaotype, icaooper, filename, hex)
    cur.execute(query, data)

    res = cur.fetchone()
    if res and res[0] is None:
        added += 1
    else:
        skipped += 1

def upsertAircraft(hex, reg, icaotype, icaooper, filename):
    global added, updated, skipped
    query = "INSERT INTO Aircrafts (HexIdent, Registration, IcaoType, IcaoOperator, SourceFile) VALUES (UPPER(%s),UPPER(%s),UPPER(%s),UPPER(%s),%s) ON CONFLICT (HexIdent) DO UPDATE SET Registration=UPPER(EXCLUDED.Registration), IcaoType=UPPER(EXCLUDED.IcaoType), IcaoOperator=UPPER(EXCLUDED.IcaoOperator), SourceFile=EXCLUDED.SourceFile RETURNING (SELECT Registration FROM Aircrafts WHERE HexIdent=UPPER(%s)) AS OldReg;"
    data = (hex, reg, icaotype, icaooper, filename, hex)
    cur.execute(query, data)

    oldreg = cur.fetchone()[0]
    if oldreg is None:
        added += 1
    elif reg.upper() != oldreg:
        print(hex + " '" + oldreg + "' => '" + reg + "'")
        updated += 1
    else:
        updated += 1


try:
    db = dbaccess.connect (host=os.getenv('SBS_DB_HOST', '127.0.0.1'), port=os.getenv('SBS_DB_PORT', '5432'), database=os.getenv('SBS_DB_DATABASE', 'sbs'), user=os.getenv('SBS_DB_USER', 'sbs'), password=os.getenv('SBS_DB_PASSWORD', 'sbs'))
except:
    print ("Unable to connect to the database")
    sys.exit(1)

cur = db.cursor()
added = 0
updated = 0
skipped = 0

if args.clean:
    print("Cleanup database ...")
    query = "TRUNCATE Aircrafts;"
    cur.execute(query)

if args.flightaware_file:
    print ("Add Data from FlightAware File ...")
    with open(args.flightaware_file, 'r', newline='') as infile:
        reader = csv.reader(infile)
        next(reader, None) 
        for r in reader:
            addAircraft(r[0],r[1],r[2], None, args.flightaware_file)

if args.opensky_file:
    print ("Add Data from OpenSky Network File ...")
    with open(args.opensky_file, 'r', newline='') as infile:
        reader = csv.reader(infile)
        next(reader, None) 
        for r in reader:
            addAircraft(r[0],r[1],r[5],r[11], args.opensky_file)


if args.custom_file:
    print ("Add Data from Custom File ...")
    with open(args.custom_file, 'r', newline='') as infile:
        reader = csv.reader(infile)
        next(reader, None) 
        for r in reader:
            addAircraft(r[0],r[1],r[2],r[3], args.custom_file)

db.commit()

print ("Added: " + str(added) + " - Updated: " + str(updated) + " - Skipped: " + str(skipped))
