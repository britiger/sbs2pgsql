CREATE USER sbs PASSWORD 'sbs';
CREATE DATABASE sbs OWNER sbs;

\c sbs

CREATE EXTENSION postgis;
CREATE EXTENSION "uuid-ossp";

SET SESSION ROLE sbs;

CREATE TABLE messages (
    MessageType varchar(3),
    TransmissionType smallint,
    SessionID smallint,
    AircraftID smallint,
    HexIdent varchar(6),
    FlightID integer,
    MessageGenerated timestamp,
    MessageLogged timestamp,
    Callsign varchar(8),
    Altitude integer,
    GroundSpeed numeric(5, 1),
    Track int,
    Latitude numeric(8, 5),
    Longitude numeric(8, 5),
    VerticalRate int,
    Squawk int,
    Alert boolean,
    Emergency boolean,
    SPI boolean,
    OnGround boolean,
    AddedToDB timestamp DEFAULT NOW(),
    FlightUUID uuid DEFAULT NULL,
    Position geometry GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(Longitude, Latitude),4326)) STORED
);
CREATE INDEX msg_idx_hex ON messages(HexIdent);
CREATE INDEX msg_idx_fl ON messages(FlightUUID);

CREATE OR REPLACE VIEW UnprocessedFlights AS
SELECT hexident, max(addedtodb) AS newestMsg
FROM messages
WHERE FlightUUID IS NULL
GROUP BY hexident
HAVING max(addedtodb)<(NOW()-'01:00:00'::interval)
ORDER BY max(addedtodb);

CREATE TABLE processedFlights (
    FlightUUID uuid DEFAULT uuid_generate_v4(),
    HexIdent varchar(6),
    Callsign varchar(8),
    flightFromDate timestamp,
    flightToDate timestamp,
    minAltitude integer,
    geom geometry
);
CREATE INDEX pro_idx_hex ON processedFlights (HexIdent);
CREATE UNIQUE INDEX pro_uniq ON processedFlights (HexIdent, flightFromDate);
CREATE INDEX pro_idx_date ON processedFlights (flightFromDate,flightToDate);

-- Functions:
CREATE FUNCTION coalesce_agg_sfunc(state text, value text) RETURNS text AS
$$
    SELECT coalesce(value, state);
$$ LANGUAGE SQL;

CREATE AGGREGATE coalesce_agg(text) (
    SFUNC = coalesce_agg_sfunc,
    STYPE  = text);

-- Contain all old flights to process
CREATE OR  REPLACE VIEW summaryFlight AS 
WITH mess AS
    (SELECT *,
        addedtodb - lag(addedtodb,1) OVER (PARTITION BY hexident ORDER BY addedtodb) AS diff,
        lag(callsign, 1) OVER (PARTITION BY hexident ORDER BY addedtodb) AS last_callsign
        FROM messages WHERE FlightUUID IS NULL),
    tdiff AS
    (SELECT *, sum(CASE WHEN diff IS NULL
                     OR diff>interval '1 hour' THEN 1 ELSE NULL END) OVER (PARTITION BY hexident ORDER BY addedtodb) AS tperiod
        FROM mess),
    ccol AS
         (SELECT *, coalesce_agg(callsign) OVER (PARTITION BY hexident ORDER BY addedtodb) AS col_call
            FROM tdiff WHERE tperiod=1),
    cchange AS 
        (SELECT *, lag(col_call, 1) OVER (PARTITION BY hexident ORDER BY addedtodb) AS col_lag
            FROM ccol),
    cdiff AS
        (SELECT *, sum(CASE WHEN col_call<>col_lag THEN 1 ELSE 0 END) OVER (PARTITION BY hexident ORDER BY addedtodb) AS cperiod
        FROM cchange),
    alldiff AS
        (SELECT *, (tperiod+cperiod) AS period, max(callsign) OVER (PARTITION BY hexident, tperiod, cperiod) AS callsignFlight FROM cdiff)
    
SELECT *
    FROM alldiff
    WHERE period = 1;

CREATE TABLE Aircrafts (
    HexIdent varchar(6) NOT NULL,
    Registration varchar(25),
    IcaoType varchar(4),
    IcaoOperator varchar(4),
    SourceFile TEXT

);
CREATE UNIQUE INDEX airc_idx_hex ON Aircrafts (HexIdent);
