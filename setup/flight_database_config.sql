-- DROP DATABASE flight
-- Create database if don't exist
SELECT 'CREATE DATABASE flight' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'flight')\gexec

-- Connect to database 
\c flight

CREATE SCHEMA IF NOT EXISTS flight;

CREATE TABLE IF NOT EXISTS flight.search (
    "searchId" BIGINT PRIMARY KEY,
    "searchTime" TIMESTAMP NOT NULL,
    "operationalSearchTime" TIMESTAMP NOT NULL,
    "flightDay" DATE NOT NULL,
    "originCode" CHAR(3) NOT NULL,
    "destinationCode" CHAR(3) NOT NULL,
    "insertionTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flight.flight (
    "searchId" BIGINT PRIMARY KEY,
    "legId" VARCHAR(50) NOT NULL,
    "travelDuration" VARCHAR NOT NULL,
    "duration" VARCHAR NOT NULL,
    "durationInSeconds" VARCHAR NOT NULL,
    "elapsedDays" VARCHAR NOT NULL,
    "isNonStop" BOOLEAN NOT NULL,
    "departureTimeRaw" VARCHAR NOT NULL,
    "departureTimeZoneOffsetSeconds" VARCHAR NOT NULL,
    "arrivalTimeRaw" VARCHAR NOT NULL,
    "arrivalTimeZoneOffsetSeconds" VARCHAR NOT NULL,
    "flightNumber" VARCHAR NOT NULL,
    "stops" VARCHAR NOT NULL,
    "airlineCode" VARCHAR NOT NULL,
    "equipmentCode" VARCHAR NOT NULL,
    "arrivalAirportLatitude" VARCHAR NOT NULL,
    "arrivalAirportLongitude" VARCHAR NOT NULL,
    "departureAirportLatitude" VARCHAR NOT NULL,
    "departureAirportLongitude" VARCHAR NOT NULL,
    "arrivalAirportCode" VARCHAR NOT NULL,
    "departureAirportCode" VARCHAR NOT NULL,
    "insertionTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY ("searchId") REFERENCES flight.search("searchId")
);

CREATE TABLE IF NOT EXISTS flight.fare (
    "searchId" BIGINT PRIMARY KEY,
    "legId" VARCHAR(32) NOT NULL,
    "fareBasisCode" VARCHAR NOT NULL,
    "isBasicEconomy" BOOLEAN NOT NULL,
    "isRefundable" BOOLEAN NOT NULL,
    "isFreeChangeAvailable" BOOLEAN NOT NULL,
    "taxes" DECIMAL(10,2) NOT NULL,
    "fees" DECIMAL(10,2) NOT NULL,
    "showFees" BOOLEAN NOT NULL,
    "currency" CHAR(3) NOT NULL,
    "baseFare" DECIMAL(10,2) NOT NULL,
    "totalFare" DECIMAL(10,2) NOT NULL,
    "numberOfTickets" INTEGER NOT NULL,
    "freeCancellationBy" TIMESTAMP,
    "hasSeatMap" VARCHAR NOT NULL,
    "providerCode" VARCHAR,
    "seatsRemaining" INTEGER NOT NULL,
    "insertionTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY ("searchId") REFERENCES flight.search("searchId")
);

CREATE TABLE IF NOT EXISTS flight.airport (
    "airportCode" CHAR(3) PRIMARY KEY,
    "airportLatitude" DECIMAL(10,6) NOT NULL,
    "airportLongitude" DECIMAL(10,6) NOT NULL,
    "city" VARCHAR(100),
    "insertionTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flight.airline (
    "airlineCode" CHAR(2) PRIMARY KEY,
    "airlineName" VARCHAR(100) NOT NULL,
    "externalAirlineCode" VARCHAR(2),
    "operatingAirlineName" VARCHAR(100),
    "insertionTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flight.equipment (
    "equipmentCode" CHAR(3) PRIMARY KEY,
    "equipmentDescription" VARCHAR(100) NOT NULL,
    "insertionTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Show tables of schema flight
\dt flight.*

\q