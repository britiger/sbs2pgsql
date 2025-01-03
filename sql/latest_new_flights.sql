SELECT flightuuid, hexident, get_code_country(hexident) AS country, a.registration,  a.modelicao, manufacturerandmodel, p.callsign, r.airportcodes, al.name, al.icao, msg, category, flightfromdate, flighttodate, is_military(hexident) AS military 
FROM process2 p 
    LEFT JOIN aircrafts a ON p.hexident=a.icao 
    LEFT JOIN sdm_routes r ON p.callsign=r.callsign
    LEFT JOIN sdm_airlines al ON is_callsign(p.callsign) AND SUBSTRING(p.callsign,1,3)=al.icao
WHERE flightfromdate BETWEEN NOW() - INTERVAL '24 HOURS' AND NOW()
ORDER BY flightfromdate DESC;