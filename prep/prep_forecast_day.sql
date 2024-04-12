WITH forecast_day_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_day')}}
),
add_features AS (
    SELECT *,
        DATE_PART('day', date) AS day_of_month, -- Extract the day of the month from the date
        TO_CHAR(date, 'Month') AS month_of_year, -- Extract the month name from the date
        DATE_PART('year', date) AS year, -- Extract the year from the date
        TO_CHAR(date, 'Day') AS day_of_week, -- Extract the name of the weekday from the date
        DATE_PART('week', date) AS week_of_year, -- Extract the week number of the year from the date
        TO_CHAR(date, 'YYYY-"W"IW') AS year_and_week -- Combine year and calendar week into format '2024-W43'

    FROM forecast_day_data
)
SELECT *
FROM add_features