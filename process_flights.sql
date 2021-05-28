-- Clean Database
WITH caList AS (SELECT hexident, count(position) as cp, count(*) ca, count(callsign) cc
FROM messages
GROUP BY hexident
HAVING count(position)=0 AND count(callsign)=0 AND count(*)=1)
DELETE FROM messages WHERE messages.hexident IN (SELECT hexident FROM caList);
--
BEGIN;
WITH 
summ AS
	(SELECT hexident, callsignflight, min(addedtodb) minaddedtodb, max(addedtodb) maxaddedtodb, min(Altitude)
	FROM summaryFlight
	WHERE hexident IN (SELECT hexident FROM UnprocessedFlights) GROUP BY hexident, callsignflight)
INSERT INTO ProcessedFlights (HexIdent, Callsign, flightFromDate, flightToDate, minAltitude, geom)
SELECT *, ST_MakeLine(
	ARRAY(SELECT position FROM messages m WHERE m.hexident=sf.hexident AND m.addedtodb>=minaddedtodb AND m.addedtodb<=maxaddedtodb AND position IS NOT NULL ORDER BY addedtodb) 
) FROM summ sf;

UPDATE messages m SET flightuuid = (SELECT FlightUUID FROM ProcessedFlights pf WHERE pf.hexident=m.hexident AND m.addedtodb<=pf.flightToDate AND m.addedtodb>=pf.flightFromDate)
WHERE flightuuid IS NULL AND hexident IN (SELECT hexident FROM UnprocessedFlights);
COMMIT;