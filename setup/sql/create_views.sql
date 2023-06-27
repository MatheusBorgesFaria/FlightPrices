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


-- View of normalized prices
CREATE VIEW flight.normalized_fares_view AS
SELECT F."searchId", F."legId", F."totalFare",
    (F."totalFare" - V."average") / V."standard_deviation" AS "normalizedTotalFare"
FROM (
    SELECT FF."searchId", FF."legId", FF."totalFare"
    FROM flight.fare FF
)  F
JOIN (
    SELECT FS."searchId", FS."originCode", FS."destinationCode"
    FROM flight.search FS
) S ON F."searchId" = S."searchId"
JOIN flight.price_normalization_view V
    ON S."originCode" = V."originCode" AND S."destinationCode" = V."destinationCode";


-- Check existing views
SELECT table_name
FROM information_schema.tables
WHERE table_type = 'VIEW' AND table_schema = 'flight';

\q