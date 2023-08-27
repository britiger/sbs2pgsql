import signal
import math
import sys
import os
import psycopg2 as dbaccess
import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient
import datetime
import pytz

# define your custom class by extending the TcpClient
#   - implement your handle_messages() methods
class ADSBClient(TcpClient):
    db = None
    cur = None

    max_age = 5
    cnt_parsed = 0

    stat_parsed = 0
    stat_saved = 0
    stat_relabel = 0
    stat_skip = 0
    stat_ts = datetime.datetime.now(tz=pytz.UTC)

    active_dict = dict()

    CATEGORY_CHAR = [None, None, 'C', 'B', 'A']

    def __init__(self, host, port, rawtype):
        super(ADSBClient, self).__init__(host, port, rawtype)
        self.db_init()

    def stop(self):
        super().stop()
        self.write_all()

    def db_init(self):
        try:
            self.db = dbaccess.connect (host=os.getenv('SBS_DB_HOST', '127.0.0.1'), port=os.getenv('SBS_DB_PORT', '5432'), database=os.getenv('SBS_DB_DATABASE', 'sbs'), user=os.getenv('SBS_DB_USER', 'sbs'), password=os.getenv('SBS_DB_PASSWORD', 'sbs'))
            self.cur = self.db.cursor()
        except:
            print ("Unable to connect to the database")
            sys.exit(1)

    def check_old(self):
        for icao in list(self.active_dict):
            age = (datetime.datetime.now(tz=pytz.UTC) - self.active_dict[icao]['lastProcess']).total_seconds() / 60.0
            if age > self.max_age:
                # final
                print ("Old " + icao + " " + " Callsign:" + str(self.active_dict[icao]['callsign']) + " " + str(self.active_dict[icao]['lastProcess']) + " " + str(self.active_dict[icao]['msgCnt']) + " Mgs - CRC:" + str(self.active_dict[icao]['crc']))
                self.insert_db(icao)
                del self.active_dict[icao]
        age = (datetime.datetime.now(tz=pytz.UTC) - self.stat_ts).total_seconds() / 60.0
        if age > self.max_age:
            self.insert_stat()

    def write_all(self):
        print("Closing, Write all data ...")
        for icao in list(self.active_dict):
            self.insert_db(icao)
        self.insert_stat()

    def insert_db(self, icao, initial=False):
        if self.active_dict[icao]['msgCnt'] < 2:
            print ("Skip " + icao + " low messages")
            self.stat_skip += 1
            return

        linestring = None
        callsign = None
        if len(self.active_dict[icao]['positions']) > 1:
            linestring = "SRID=4326;LINESTRING Z (" + ", ".join(self.active_dict[icao]['positions']) + ")"
        if self.active_dict[icao]['callsign']:
            callsign = self.active_dict[icao]['callsign'].strip('_')

        # Don't add last seen if not final insert
        todate = self.active_dict[icao]['lastProcess'] if not initial else None

        query = "INSERT INTO process2 (hexident, callsign, flightfromdate, flighttodate, geom, msg, crc, category) VALUES (%s, %s, %s, %s, %s, %s, %s,%s) ON conflict (hexident, flightfromdate) DO UPDATE SET callsign=EXCLUDED.callsign, flighttodate=EXCLUDED.flighttodate, geom=EXCLUDED.geom, msg=EXCLUDED.msg, crc=EXCLUDED.crc, category=EXCLUDED.category RETURNING flightuuid"
        data = (self.active_dict[icao]['icao'],callsign,self.active_dict[icao]['firstSeen'],todate, linestring ,self.active_dict[icao]['msgCnt'],self.active_dict[icao]['crc'],self.active_dict[icao]['category'])
        try:
            self.cur.execute(query, data)
            self.db.commit()
            if initial:                
                self.active_dict[icao]['flightuuid'] = self.cur.fetchone()[0]
            else:
                self.stat_saved += 1
        except Exception as e:
            print('Failed to add data: '+ str(e))
            self.db.rollback()

    def insert_stat(self):
        query = "INSERT INTO stats (parsed, saved, relabel, skip, active, ts_from, ts_to) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        data = (self.stat_parsed,self.stat_saved,self.stat_relabel,self.stat_skip,len(self.active_dict),self.stat_ts,datetime.datetime.now(tz=pytz.UTC))
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add stats: '+ str(e))
            self.db.rollback()

        self.stat_parsed = 0
        self.stat_saved = 0
        self.stat_relabel = 0
        self.stat_skip = 0
        self.stat_ts = datetime.datetime.now(tz=pytz.UTC)

    def distance(self, origin, destination):
        """
        Calculate the Haversine distance.

        Parameters
        ----------
        origin : tuple of float
            (lat, long)
        destination : tuple of float
            (lat, long)

        Returns
        -------
        distance_in_km : float

        Examples
        --------
        >>> origin = (48.1372, 11.5756)  # Munich
        >>> destination = (52.5186, 13.4083)  # Berlin
        >>> round(distance(origin, destination), 1)
        504.2
        """
        lat1, lon1 = origin
        lat2, lon2 = destination
        radius = 6371  # km

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
            math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
            math.sin(dlon / 2) * math.sin(dlon / 2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        d = radius * c

        return d

    def get_category(self, tc, cat):
        if tc < 2 or tc > 4:
            return None
        if cat < 0 or cat > 7:
            return None
        return self.CATEGORY_CHAR[tc] + str(cat)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) < 13:  # wrong data length (28)
                continue

            df = pms.df(msg)

#            if df != 17 and df != 4 and df != 20 and df != 5 and df != 21 and df != 18 :  # not ADSB
#                continue

            #if pms.crc(msg) !=0:  # CRC fail
            #    continue

            self.cnt_parsed += 1
            self.stat_parsed += 1

            icao = pms.icao(msg)
            tc = pms.adsb.typecode(msg)
            crc = pms.crc(msg)
            # print (str(crc))

            if icao == None:
                print ("icao was empty.")
                continue

            if 1 <= tc <= 4:
                callsign = pms.adsb.callsign(msg)
                cat = pms.adsb.category(msg)
                category = self.get_category(tc, cat)
            else:
                callsign = None
                category = None

            if df == 5 or df == 21:
                try:
                    squawk = pms.common.idcode(msg)
                except:
                    continue
            else:
                squawk = None

            is_new = True

            if icao in self.active_dict:
                # existing
                if self.active_dict[icao]['callsign'] is None or callsign is None or callsign == self.active_dict[icao]['callsign']:
                    self.active_dict[icao]['msgCnt'] += 1
                    self.active_dict[icao]['lastProcess'] = datetime.datetime.now(tz=pytz.UTC)
                    if callsign:
                        self.active_dict[icao]['callsign'] =  callsign
                    if category:
                        self.active_dict[icao]['category'] =  category
                    is_new = False

                    if 5 <= tc <= 18 and crc == 0:
                        # TypeCode with Position Info
                        oe_flag = pms.adsb.oe_flag(msg)
                        self.active_dict[icao]["t"+str(oe_flag)] = ts
                        self.active_dict[icao]["m"+str(oe_flag)] = msg

                        altitude = pms.adsb.altitude(msg)
                        latlon = None
                        latlonS = None
                        if ("tpos" in self.active_dict[icao]) and (ts - self.active_dict[icao]["tpos"] < 180):
                            # use single message decoding
                            try:
                                latlonS = pms.adsb.position_with_ref(msg, self.active_dict[icao]["lat"], self.active_dict[icao]["lon"])
                            except:
                                latlonS = None
                        if (icao in self.active_dict) and ("t0" in self.active_dict[icao]) and ("t1" in self.active_dict[icao]) and (abs(self.active_dict[icao]["t0"] - self.active_dict[icao]["t1"]) < 10):
                            try:
                                latlon = pms.adsb.position(
                                    self.active_dict[icao]["m0"],
                                    self.active_dict[icao]["m1"],
                                    self.active_dict[icao]["t0"],
                                    self.active_dict[icao]["t1"],
                                    self.active_dict[icao]["lat"],
                                    self.active_dict[icao]["lon"])
                            except:
                                latlon = None

                        # Position Checks
                        if latlon and self.distance((52,13),latlon) > 450:
                            print ("=======")
                            print (icao + " latlon "+str(latlon)+" high dist " + str(self.distance((52,13),latlon)))
                            print ("=======")
                            latlon = None
                        if latlonS and self.distance((52,13),latlonS) > 450:
                            print ("=======")
                            print (icao + " latlonS "+str(latlonS)+" high dist " + str(self.distance((52,13),latlonS)))
                            print ("=======")
                            latlonS = None
                        if self.active_dict[icao]['tpos']:
                            # distance from last pos
                            # 300 m/s ~>1000km/h = 0.3km/s
                            # add 5km for short-dist jitter
                            maxdiff = 0.3 * (ts - self.active_dict[icao]['tpos']) + 5
                            if latlon and self.distance((self.active_dict[icao]["lat"],self.active_dict[icao]["lon"]),latlon) > maxdiff:
                                print ("=======")
                                print (icao + " latlon "+str(latlon)+" high travel " + str(maxdiff) + " dist " + str(self.distance((self.active_dict[icao]["lat"],self.active_dict[icao]["lon"]),latlon)))
                                print ("=======")
                                latlon = None
                            if latlonS and self.distance((self.active_dict[icao]["lat"],self.active_dict[icao]["lon"]),latlonS) > maxdiff:
                                print ("=======")
                                print (icao + " latlonS "+str(latlonS)+" high travel " + str(maxdiff) + " dist " + str(self.distance((self.active_dict[icao]["lat"],self.active_dict[icao]["lon"]),latlonS)))
                                print ("=======")
                                latlonS = None

                        if not latlon and latlonS:
                            latlon = latlonS
                        if latlon:
                            self.active_dict[icao]["lat"], self.active_dict[icao]["lon"] = latlon
                            self.active_dict[icao]["tpos"] = ts
                        if latlon and altitude:
                            self.active_dict[icao]["positions"] = self.active_dict[icao]["positions"] + [str (self.active_dict[icao]["lon"]) + " " + str (self.active_dict[icao]["lat"]) + " " + str(altitude)]
                else:
                    # Changed Callsign
                    print ("Relabel Callsign: " + str(df) + " " + self.active_dict[icao]['callsign'] + ' ' + callsign + ' CRC: ' + str(crc))
                    self.insert_db(icao)
                    del self.active_dict[icao]
                    is_new = True

            if is_new:
                # new
                ac = {
                    "icao": icao,
                    "firstSeen": datetime.datetime.now(tz=pytz.UTC),
                    "lastProcess": datetime.datetime.now(tz=pytz.UTC),
                    "msgCnt": 1,
                    "callsign": callsign,
                    "positions": [],
                    "t0": 0,
                    "m0": None,
                    "t1": 0,
                    "m1": None,
                    "lat": None,
                    "lon": None,
                    "tpos": 0,
                    "crc": False,
                    "category": category
                }
                self.active_dict[icao] = ac
                print ("New " + str(df) + " " + str(tc) + " " + icao)

            if crc == 0:
                self.active_dict[icao]["crc"] = True

            if self.active_dict[icao]["callsign"] != None and not 'flightuuid' in self.active_dict[icao]:
                print("First add " + icao)
                self.insert_db(icao, True)

            if (self.cnt_parsed % 5000) == 0:
                print('Messages parsed: ' + str(self.cnt_parsed) + " - Active: " + str(len(self.active_dict)))
                self.check_old()

# run new client, change the host, port, and rawtype if needed
client = ADSBClient(host=os.getenv('SBS_HOST', '127.0.0.1'), port=30005, rawtype='beast')

def signal_handler(sig, frame):
    client.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

client.run()
