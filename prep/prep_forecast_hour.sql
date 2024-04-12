WITH forecast_hour_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}
),
add_features AS (
    SELECT *,
        date_time::time AS time, -- Convert date_time to only time (hours:minutes:seconds)
        TO_CHAR(date_time, 'HH24:MI') AS hour, -- Extract time in hours:minutes format
        TO_CHAR(date_time, 'Month') AS month_of_year, -- Extract the full name of the month from date_time
        TO_CHAR(date_time, 'Day') AS day_of_week -- Extract the full name of the weekday from date_time
    FROM forecast_hour_data
)
SELECT *
FROM add_features
