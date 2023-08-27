CREATE USER sbs PASSWORD 'sbs';
CREATE DATABASE sbs OWNER sbs;

\c sbs

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

SET SESSION ROLE sbs;

CREATE TABLE IF NOT EXISTS process2 (
    FlightUUID uuid DEFAULT uuid_generate_v4(),
    HexIdent char(6),
    Callsign varchar(8),
    flightFromDate timestamp with time zone,
    flightToDate timestamp with time zone,
    msg int,
    category char(2),
    crc boolean,
    geom geometry(LineStringZ, 4326),
    CONSTRAINT process2_pkey PRIMARY KEY (FlightUUID)
);
CREATE INDEX IF NOT EXISTS pro2_idx_hex ON process2 (HexIdent);
CREATE UNIQUE INDEX IF NOT EXISTS pro2_uniq ON process2 (HexIdent, flightFromDate);
CREATE INDEX IF NOT EXISTS pro2_idx_date ON process2 (flightFromDate,flightToDate);

CREATE TABLE IF NOT EXISTS stats
(
    ts_from timestamp with time zone NOT NULL,
    ts_to timestamp with time zone NOT NULL DEFAULT now(),
    parsed int,
    saved int,
    skip int,
    relabel int,
    active int,
    CONSTRAINT stats_pkey PRIMARY KEY (ts_to)
);

CREATE OR REPLACE FUNCTION is_callsign(character varying) RETURNS boolean
AS
$$
    SELECT $1 ~ '^[A-Z]{3}[0-9]';
$$ IMMUTABLE LANGUAGE SQL PARALLEL SAFE;

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

CREATE INDEX IF NOT EXISTS sdm_airlines_icao ON sdm_airlines (icao);

CREATE TABLE IF NOT EXISTS sdm_codeblocks (
    start char(6),
    finish char(6),
    count int,
    bitmask char(6),
    SignificantBitmask char(6),
    IsMilitary boolean,
    CountryISO2 char(2)
);
CREATE INDEX IF NOT EXISTS sdm_codeblocks_start ON sdm_codeblocks(start);
CREATE INDEX IF NOT EXISTS sdm_codeblocks_finish ON sdm_codeblocks(finish);

CREATE TABLE IF NOT EXISTS sdm_countries (
    ISO char(2) PRIMARY KEY,
    name varchar(64)
);

CREATE OR REPLACE FUNCTION is_military(character varying) RETURNS boolean
AS
$$
    SELECT IsMilitary FROM sdm_codeblocks WHERE start<=$1 AND finish>=$1 ORDER BY start DESC LIMIT 1;
$$ IMMUTABLE LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION is_military(character varying) RETURNS boolean
AS
$$
    SELECT IsMilitary FROM sdm_codeblocks WHERE start<=$1 AND finish>=$1 ORDER BY start DESC LIMIT 1;
$$ IMMUTABLE LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_code_country(character varying) RETURNS char(2)
AS
$$
    SELECT CountryISO2 FROM sdm_codeblocks WHERE start<=$1 AND finish>=$1 ORDER BY start DESC LIMIT 1;
$$ IMMUTABLE LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION is_charter(character varying) RETURNS boolean
AS
$$
    SELECT SUBSTRING($1,4,5) ~ (SELECT charterflightpattern FROM sdm_airlines WHERE icao=SUBSTRING($1,1,3));
$$ IMMUTABLE LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION is_positioning(character varying) RETURNS boolean
AS
$$
    SELECT SUBSTRING($1,4,5) ~ (SELECT positioningflightpattern FROM sdm_airlines WHERE icao=SUBSTRING($1,1,3));
$$ IMMUTABLE LANGUAGE SQL PARALLEL SAFE;

CREATE TABLE IF NOT EXISTS sdm_aircrafts(
    ICAO char(6) PRIMARY KEY,
    Registration varchar(16),
    ModelICAO char(4),
    Manufacturer varchar(128),
    Model varchar(128),
    ManufacturerAndModel varchar(255),
    IsPrivateOperator boolean,
    Operator varchar(255),
    AirlineCode char(3),
    SerialNumber varchar(128),
    YearBuilt smallint,
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW(),
    validuntil timestamp with time zone DEFAULT 'infinity'::timestamp
);

CREATE TABLE IF NOT EXISTS sdm_routes(
    Callsign varchar(8) PRIMARY KEY,
    Code varchar(3),
    Num varchar(6),
    AirlineCode varchar(3),
    AirportCodes varchar(128),
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW(),
    validuntil timestamp with time zone DEFAULT 'infinity'::timestamp
);

CREATE TABLE IF NOT EXISTS sdm_airports(
    Code varchar(4) PRIMARY KEY,
    Name varchar(255),
    ICAO char(4),
    IATA char(3),
    Location varchar(128),
    CountryISO2 char(2),
    Latitude float,
    Longitude float,
    AltitudeFeet smallint,
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sdm_airports_icao ON sdm_airports(ICAO);
CREATE INDEX IF NOT EXISTS sdm_airports_iata ON sdm_airports(IATA);

CREATE TABLE IF NOT EXISTS basestation_aircrafts(
    ICAO char(6) PRIMARY KEY,
    Registration varchar(16),
    ModelICAO char(4),
    Manufacturer varchar(128),
    Model varchar(128),
    ManufacturerAndModel varchar(255),
    IsPrivateOperator boolean,
    Operator varchar(255),
    AirlineCode char(3),
    SerialNumber varchar(128),
    YearBuilt smallint,
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW(),
    validuntil timestamp with time zone DEFAULT 'infinity'::timestamp
);

CREATE TABLE IF NOT EXISTS wikidata_airports (
    wikidata varchar(15) PRIMARY KEY,
    Name varchar(255),
    ICAO char(4) NOT NULL,
    IATA char(3),
    website TEXT,
    websiteLanguage char(2),
    logoURL TEXT,
    imageURL TEXT,
    ingnoreRow boolean,
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wikidata_airlines (
    wikidata varchar(15) PRIMARY KEY,
    name varchar(255),
    icao char(3),
    iata char(2),
    callsign varchar(64),
    country char(2),
    website TEXT,
    websiteLanguage char(2),
    logoURL TEXT,
    ingnoreRow boolean,
    added timestamp with time zone DEFAULT NOW(),
    updated timestamp with time zone DEFAULT NOW()
);

-- VIEWS
CREATE MATERIALIZED VIEW IF NOT EXISTS aircrafts AS
    SELECT * FROM sdm_aircrafts
    UNION
    SELECT * FROM basestation_aircrafts WHERE icao NOT IN (SELECT icao FROM sdm_aircrafts);

-- Update TRIGGER
CREATE OR REPLACE FUNCTION check_sdm_routes_updates() 
RETURNS TRIGGER 
AS $$
BEGIN
    IF OLD.AirportCodes IS DISTINCT FROM NEW.AirportCodes THEN
        NEW.updated := NOW();
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER sdm_routes_updates BEFORE UPDATE ON sdm_routes FOR EACH ROW EXECUTE FUNCTION check_sdm_routes_updates();

CREATE OR REPLACE FUNCTION check_sdm_airports_updates() 
RETURNS TRIGGER 
AS $$
BEGIN
    IF OLD.name IS DISTINCT FROM NEW.name 
        OR OLD.icao IS DISTINCT FROM NEW.icao 
        OR OLD.iata IS DISTINCT FROM NEW.iata 
        OR OLD.Location IS DISTINCT FROM NEW.Location
        OR OLD.Latitude IS DISTINCT FROM NEW.Latitude
        OR OLD.Longitude IS DISTINCT FROM NEW.Longitude
        OR OLD.AltitudeFeet IS DISTINCT FROM NEW.AltitudeFeet
        OR OLD.CountryISO2 IS DISTINCT FROM NEW.CountryISO2
        THEN
            NEW.updated := NOW();
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER sdm_airports_updates BEFORE UPDATE ON sdm_airports FOR EACH ROW EXECUTE FUNCTION check_sdm_airports_updates();

CREATE OR REPLACE FUNCTION check_sdm_aircrafts_updates() 
RETURNS TRIGGER 
AS $$
BEGIN
    IF OLD.Registration IS DISTINCT FROM NEW.Registration 
        OR OLD.ModelICAO IS DISTINCT FROM NEW.ModelICAO 
        OR OLD.Manufacturer IS DISTINCT FROM NEW.Manufacturer 
        OR OLD.Model IS DISTINCT FROM NEW.Model
        OR OLD.ManufacturerAndModel IS DISTINCT FROM NEW.ManufacturerAndModel
        OR OLD.IsPrivateOperator IS DISTINCT FROM NEW.IsPrivateOperator
        OR OLD.Operator IS DISTINCT FROM NEW.Operator
        OR OLD.AirlineCode IS DISTINCT FROM NEW.AirlineCode
        THEN
            NEW.updated := NOW();
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER sdm_aircrafts_updates BEFORE UPDATE ON sdm_aircrafts FOR EACH ROW EXECUTE FUNCTION check_sdm_aircrafts_updates();

CREATE OR REPLACE FUNCTION check_sdm_airlines_updates() 
RETURNS TRIGGER 
AS $$
BEGIN
    IF OLD.name IS DISTINCT FROM NEW.name 
        OR OLD.icao IS DISTINCT FROM NEW.icao 
        OR OLD.iata IS DISTINCT FROM NEW.iata 
        OR OLD.PositioningFlightPattern IS DISTINCT FROM NEW.PositioningFlightPattern
        OR OLD.CharterFlightPattern IS DISTINCT FROM NEW.CharterFlightPattern
        THEN
            NEW.updated := NOW();
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER sdm_airlines_updates BEFORE UPDATE ON sdm_airlines FOR EACH ROW EXECUTE FUNCTION check_sdm_airlines_updates();

CREATE OR REPLACE FUNCTION check_wikidata_airports_updates() 
RETURNS TRIGGER 
AS $$
BEGIN
    IF OLD.name IS DISTINCT FROM NEW.name 
        OR OLD.icao IS DISTINCT FROM NEW.icao 
        OR OLD.iata IS DISTINCT FROM NEW.iata 
        OR OLD.website IS DISTINCT FROM NEW.website
        OR OLD.websiteLanguage IS DISTINCT FROM NEW.websiteLanguage
        OR OLD.logoURL IS DISTINCT FROM NEW.logoURL
        OR OLD.imageURL IS DISTINCT FROM NEW.imageURL
        THEN
            NEW.updated := NOW();
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER wikidata_airports_updates BEFORE UPDATE ON wikidata_airports FOR EACH ROW EXECUTE FUNCTION check_wikidata_airports_updates();

CREATE OR REPLACE FUNCTION check_wikidata_airlines_updates() 
RETURNS TRIGGER 
AS $$
BEGIN
    IF OLD.name IS DISTINCT FROM NEW.name 
        OR OLD.icao IS DISTINCT FROM NEW.icao 
        OR OLD.iata IS DISTINCT FROM NEW.iata 
        OR OLD.callsign IS DISTINCT FROM NEW.callsign
        OR OLD.country IS DISTINCT FROM NEW.country
        OR OLD.website IS DISTINCT FROM NEW.website
        OR OLD.websiteLanguage IS DISTINCT FROM NEW.websiteLanguage
        OR OLD.logoURL IS DISTINCT FROM NEW.logoURL
        THEN
            NEW.updated := NOW();
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER wikidata_airlines_updates BEFORE UPDATE ON wikidata_airlines FOR EACH ROW EXECUTE FUNCTION check_wikidata_airlines_updates();
