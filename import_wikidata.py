import sys
import os
import psycopg2 as dbaccess
import requests

class wikidata_import:
    db = None
    cur = None

    WIKIDATA_URL='https://query.wikidata.org/sparql'

    def __init__(self):
        self.db_init()

    def db_init(self):
        try:
            self.db = dbaccess.connect (host=os.getenv('SBS_DB_HOST', '127.0.0.1'), port=os.getenv('SBS_DB_PORT', '5432'), database=os.getenv('SBS_DB_DATABASE', 'sbs'), user=os.getenv('SBS_DB_USER', 'sbs'), password=os.getenv('SBS_DB_PASSWORD', 'sbs'))
            self.cur = self.db.cursor()
        except:
            print ("Unable to connect to the database")
            sys.exit(1)

    def request_query(self, query):
        r = requests.get(self.WIKIDATA_URL, params = {'format': 'json', 'query': query})
        return r.json()

    def insert_airline(self, entry):
        query = 'INSERT INTO wikidata_airlines (wikidata, Name, ICAO, IATA, callsign, country, website, websiteLanguage, logoURL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (wikidata) DO UPDATE SET Name=EXCLUDED.Name, ICAO=EXCLUDED.ICAO, IATA=EXCLUDED.IATA, callsign=EXCLUDED.callsign, country=EXCLUDED.country, website=EXCLUDED.website, websiteLanguage=EXCLUDED.websiteLanguage, logoURL=EXCLUDED.logoURL'
        data = (entry['wikidata'],entry['name'],entry['icao'],entry['iata'],entry['callsign'],entry['country'],entry['website'],entry['websiteLanguage'],entry['logoURL'])
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add airline: '+ str(e))
            print(entry)
            self.db.rollback()

    def delete_airline(self, wikidata):
        query = 'DELETE FROM wikidata_airlines WHERE wikidata=%s'
        data = (wikidata,)
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to delete airline: '+ str(e))
            print(wikidata)
            self.db.rollback()

    def insert_airport(self, entry):
        query = 'INSERT INTO wikidata_airports (wikidata, Name, ICAO, IATA, website, websiteLanguage, logoURL, imageURL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (wikidata) DO UPDATE SET Name=EXCLUDED.Name, ICAO=EXCLUDED.ICAO, IATA=EXCLUDED.IATA, website=EXCLUDED.website, websiteLanguage=EXCLUDED.websiteLanguage, logoURL=EXCLUDED.logoURL, imageURL=EXCLUDED.imageURL'
        data = (entry['wikidata'],entry['name'],entry['icao'],entry['iata'],entry['website'],entry['websiteLanguage'],entry['logoURL'],entry['imageURL'])
        try:
            self.cur.execute(query, data)
            self.db.commit()
        except Exception as e:
            print('Failed to add airport: '+ str(e))
            print(entry)
            self.db.rollback()

    def load_airports(self):
        query="""
SELECT DISTINCT ?item ?itemLabel ?icao ?iata ?websiteLabel ?langcode ?logo ?image WHERE {
  ?item p:P239 _:anyValueP239.
  MINUS { ?item wdt:P576|wdt:P582|wdt:P730 ?inactive. }
  ?item wdt:P239 ?icao.
  FILTER( !wikibase:isSomeValue(?icao) )
  OPTIONAL { ?item wdt:P154 ?logo. }
  OPTIONAL { ?item wdt:P238 ?iata. }
  OPTIONAL { ?item wdt:P18 ?image. }
  OPTIONAL { ?item p:P856 [ ps:P856 ?website ; pq:P407 ?lang ]. ?lang wdt:P218 ?langcode }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,de,fr,es". }
}
ORDER BY ?item ?itemLabel ?icao ?iata ?websiteLabel ?langcode ?logo ?image
"""
        data = self.request_query(query)
        i = {}
        for item in data['results']['bindings']:
            wikidata = item['item']['value'].split('/')[-1]

            if len(i) > 1 and i['wikidata'] != wikidata:
                self.insert_airport(i)
                i = {}

            i['wikidata'] = wikidata
            i['name'] = item['itemLabel']['value'] if 'xml:lang' in item['itemLabel'] else None
            i['icao'] = item['icao']['value']
            i['iata'] = item['iata']['value'] if 'iata' in item else None
            # TODO: Website perfer english websites
            i['website'] = item['websiteLabel']['value'] if 'websiteLabel' in item else None
            i['websiteLanguage'] = item['langcode']['value'] if 'langcode' in item else None
            i['logoURL'] = item['logo']['value'] if 'logo' in item else None
            i['imageURL'] = item['image']['value'] if 'image' in item else None

        self.insert_airport(i)

    def load_airlines(self):
        query="""
SELECT DISTINCT ?item ?itemLabel ?icao ?iata ?callsign ?countryCode ?inactive ?logo ?websiteLabel ?langcode
WHERE {
  {
    ?item wdt:P31 wd:Q1057026. # Frachtflug
  } UNION {
    ?item wdt:P31 wd:Q61883. # Luftwaffe
  } UNION {
    ?item wdt:P31 wd:Q607958. # Charter
  } UNION {
    ?item wdt:P31 wd:Q46970. # Fluggeselschaft
  } UNION {
    ?item p:P230 _:anyValueP230. # Welche mit Icao-Code
  } UNION {
    ?item p:P432 _:anyValueP432. # Welche mit Rufzeichen
  }
        
  OPTIONAL { ?item wdt:P154 ?logo. }
  OPTIONAL { ?item wdt:P230 ?icao. }
  OPTIONAL { ?item wdt:P229 ?iata. }
  OPTIONAL { ?item wdt:P432 ?callsign. }
  OPTIONAL { ?item wdt:P576 ?inactive. }
  OPTIONAL { ?item wdt:P17 ?country. ?country p:P297 [ ps:P297 ?countryCode ]. }
  OPTIONAL { ?item p:P856 [ ps:P856 ?website ; pq:P407 ?lang ]. ?lang wdt:P218 ?langcode }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,de,fr,es". }
}
ORDER BY ?item ?itemLabel ?icao ?iata ?callsign ?countryCode ?inactive ?logo ?websiteLabel ?langcode
"""
        data = self.request_query(query)
        i = {}
        for item in data['results']['bindings']:
            wikidata = item['item']['value'].split('/')[-1]

            if len(i) > 1 and i['wikidata'] != wikidata:
                self.insert_airline(i)
                i = {}

            if 'inactive' in item:
                self.delete_airline(wikidata)
                i = {}
                continue

            i['wikidata'] = wikidata
            i['name'] = item['itemLabel']['value'] if 'xml:lang' in item['itemLabel'] else None
            i['icao'] = item['icao']['value'] if 'icao' in item else None
            i['iata'] = item['iata']['value'] if 'iata' in item else None
            i['callsign'] = item['callsign']['value'] if 'callsign' in item else None
            i['country'] = item['countryCode']['value'] if 'countryCode' in item else None
            # TODO: Website perfer english websites
            i['website'] = item['websiteLabel']['value'] if 'websiteLabel' in item else None
            i['websiteLanguage'] = item['langcode']['value'] if 'langcode' in item else None
            i['logoURL'] = item['logo']['value'] if 'logo' in item else None

        self.insert_airline(i)

import_wikidata = wikidata_import()
# import_wikidata.load_airports()
import_wikidata.load_airlines()
