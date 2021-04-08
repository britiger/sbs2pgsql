CREATE USER sbs PASSWORD 'sbs';
CREATE DATABASE sbs OWNER sbs;

\c sbs

CREATE EXTENSION postgis;

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
    Position geometry GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(Longitude, Latitude),4326)) STORED
);