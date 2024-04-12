WITH forecast_day_data AS (
    SELECT 
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 ->> 'date')::DATE AS date,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgtemp_c')::NUMERIC AS avg_temp_c
    FROM {{ source('staging', 'weather_raw') }}
),
weekday_avg_temps AS (
    SELECT
        EXTRACT(ISODOW FROM date) AS weekday,
        AVG(avg_temp_c) AS average_temperature
    FROM forecast_day_data
    GROUP BY EXTRACT(ISODOW FROM date)
)
SELECT 
    CASE 
        WHEN weekday = 1 THEN 'Monday'
        WHEN weekday = 2 THEN 'Tuesday'
        WHEN weekday = 3 THEN 'Wednesday'
        WHEN weekday = 4 THEN 'Thursday'
        WHEN weekday = 5 THEN 'Friday'
        WHEN weekday = 6 THEN 'Saturday'
        WHEN weekday = 7 THEN 'Sunday'
    END AS weekday_name,
    average_temperature
FROM weekday_avg_temps
ORDER BY weekday
