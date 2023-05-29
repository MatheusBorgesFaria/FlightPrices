-- Connect to database 
\c flight

-- Check the indexes that existed before 
SELECT * FROM pg_indexes WHERE schemaname = 'flight';


-- search table
CREATE UNIQUE INDEX IF NOT EXISTS
search_pkey ON flight.search USING btree ("searchId");

CREATE INDEX IF NOT EXISTS
"operationalSearchTime_index" ON flight.search
USING btree ("operationalSearchTime");

CREATE INDEX IF NOT EXISTS
"originCode_index" ON flight.search USING btree ("originCode");

CREATE INDEX IF NOT EXISTS
"destinationCode_index" ON flight.search USING btree ("destinationCode");


-- flight table
CREATE UNIQUE INDEX IF NOT EXISTS
flight_pkey ON flight.flight USING btree ("searchId");

CREATE INDEX IF NOT EXISTS
"legId_index" ON flight.flight USING btree ("legId");


-- fare table
CREATE UNIQUE INDEX IF NOT EXISTS
fare_pkey ON flight.fare USING btree ("searchId");

CREATE INDEX IF NOT EXISTS
"legId_index" ON flight.fare USING btree ("legId");

CREATE INDEX IF NOT EXISTS
"baseFare_index" ON flight.fare USING btree ("baseFare");

CREATE INDEX IF NOT EXISTS
"totalFare_index" ON flight.fare USING btree ("totalFare");


-- Check the indexes that now exist
SELECT * FROM pg_indexes WHERE schemaname = 'flight';

\q