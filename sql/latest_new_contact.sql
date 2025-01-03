WITH newhex AS (SELECT hexident, get_code_country(hexident) AS country, max(flighttodate) AS lastseen, min(flightfromdate) AS firstseen, count(*) cnt, sum(msg) msgs 
                FROM process2 
                GROUP BY hexident 
                ORDER BY min(flightfromdate) DESC LIMIT 1000)
SELECT hexident, country, registration, manufacturerandmodel, is_military(hexident), firstseen, cnt, msgs 
    FROM newhex u LEFT JOIN aircrafts a ON u.hexident=a.icao 
    WHERE firstseen BETWEEN NOW() - INTERVAL '48 HOURS' AND NOW()
    ORDER BY firstseen DESC;