\c flight


CREATE OR REPLACE FUNCTION calculate_normalized_fares()
RETURNS TABLE ("searchId" BIGINT, "legId" text,
               "totalFare" DOUBLE PRECISION,
               "normalizedTotalFare" DOUBLE PRECISION) AS $$
BEGIN
    RETURN QUERY
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
END;
$$ LANGUAGE plpgsql;

\q