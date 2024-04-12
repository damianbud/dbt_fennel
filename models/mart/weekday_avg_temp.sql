WITH forecast_day_data AS (
    SELECT 
        CAST(extracted_data -> 'forecast' -> 'forecastday' -> 0 ->> 'date' AS DATE) AS date,
        CAST(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgtemp_c' AS NUMERIC) AS avg_temp_c
    FROM weather_raw -- Replace this with the actual table name
),
weekday_temps AS (
    SELECT
        EXTRACT(DOW FROM date) AS weekday,
        avg_temp_c
    FROM forecast_day_data
),
avg_weekday_temps AS (
    SELECT
        CASE 
            WHEN weekday = 0 THEN 'Sunday'
            WHEN weekday = 1 THEN 'Monday'
            WHEN weekday = 2 THEN 'Tuesday'
            WHEN weekday = 3 THEN 'Wednesday'
            WHEN weekday = 4 THEN 'Thursday'
            WHEN weekday = 5 THEN 'Friday'
            WHEN weekday = 6 THEN 'Saturday'
        END AS weekday_name,
        AVG(avg_temp_c) AS average_temperature
    FROM weekday_temps
    GROUP BY weekday
    ORDER BY weekday
)
SELECT * 
FROM avg_weekday_temps;
