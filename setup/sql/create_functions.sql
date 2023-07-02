-- Connect to database 
\c flight


CREATE FUNCTION get_time_of_day(datetime_val TIMESTAMP)
RETURNS VARCHAR(10)
AS
$$
DECLARE time_of_day VARCHAR(10);
BEGIN
    IF EXTRACT(HOUR FROM datetime_val) >= 0 AND EXTRACT(HOUR FROM datetime_val) < 6 THEN
        time_of_day := 'overnight';
    ELSIF EXTRACT(HOUR FROM datetime_val) >= 6 AND EXTRACT(HOUR FROM datetime_val) < 12 THEN
        time_of_day := 'morning';
    ELSIF EXTRACT(HOUR FROM datetime_val) >= 12 AND EXTRACT(HOUR FROM datetime_val) < 18 THEN
        time_of_day := 'afternoon';
    ELSE
        time_of_day := 'evening';
    END IF;
    
    RETURN time_of_day;
END;
$$
LANGUAGE plpgsql;


\q