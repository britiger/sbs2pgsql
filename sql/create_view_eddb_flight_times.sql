-- create views to show flights in night

CREATE OR REPLACE VIEW view_eddb_night_flights AS
    SELECT *,
        (flightfromdate AT TIME ZONE 'Europe/Berlin') AS flightfromdatelocal,
        (flighttodate AT TIME ZONE 'Europe/Berlin') AS flighttodatelocal,
        CASE WHEN startalt < endalt THEN 'starting' ELSE 'landing' END AS flighttype,
        eddb_timeslot((flightfromdate AT TIME ZONE 'Europe/Berlin')::time) AS starttimeslot,
        eddb_timeslot((flighttodate AT TIME ZONE 'Europe/Berlin')::time) AS endtimeslot
    FROM process2alt
    WHERE minalt < 3500 AND
        (
            (flightfromdate AT TIME ZONE 'Europe/Berlin')::time > '22:00' OR
            (flighttodate AT TIME ZONE 'Europe/Berlin')::time < '6:00'
        );

CREATE OR REPLACE FUNCTION eddb_timeslot(flighttime time) RETURNS varchar(15)
AS
$$
BEGIN
  -- Begrenzte Nachflüge
  IF flighttime > '22:00' AND flighttime < '23:30' THEN
    RETURN 'limited';
  ELSIF flighttime > '05:30' AND flighttime < '06:00' THEN
    RETURN 'limited';
  -- Verspätet/Verfüht
  ELSIF flighttime > '23:30' THEN
    RETURN 'delayed';
  ELSIF flighttime > '05:00' AND flighttime < '05:30' THEN
    RETURN 'early';
  -- Absolutes Verbot
  ELSIF flighttime < '05:00' THEN
    RETURN 'noflight';
  ELSE
    RETURN 'ok';
  END IF;
END;
$$ IMMUTABLE LANGUAGE PLPGSQL PARALLEL SAFE;

CREATE TABLE IF NOT EXISTS sdm_airlines (
    code varchar(3) NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    icao char(3),
    iata char(2),
    PositioningFlightPattern varchar(255),
    CharterFlightPattern varchar(255),
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW(),
    validuntil timestamp with time zone DEFAULT 'infinity'::timestamp
);
