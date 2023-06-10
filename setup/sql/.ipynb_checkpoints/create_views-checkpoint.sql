\c flight

-- Check existing views
SELECT table_name
FROM information_schema.tables
WHERE table_type = 'VIEW' AND table_schema = 'flight';

-- View of the average and standard deviation of the price of each flight segment
CREATE VIEW flight.price_normalization_view AS
SELECT "originCode", "destinationCode",
        stddev("totalFare") AS standard_deviation,
        AVG("totalFare") AS average
FROM (
    SELECT S."originCode", S."destinationCode", F."totalFare"
    FROM flight.fare F
        JOIN flight.search S
        ON F."searchId" = S."searchId"
) subquery
GROUP BY "originCode", "destinationCode";


-- Check existing views
SELECT table_name
FROM information_schema.tables
WHERE table_type = 'VIEW' AND table_schema = 'flight';

\q