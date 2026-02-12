/* ---- Step 1 - Setting up the enviroment ----*/

USE ROLE ACCOUNTADMIN; 
USE WAREHOUSE COMPUTE_WH;
USE DATABASE WEATHER_SOURCE_LLC_FROSTBYTE;
USE SCHEMA WEATHER_SOURCE_LLC_FROSTBYTE.ONPOINT_ID;


/* ---- Step 2 - Dataset Discovery (Schemas, Tables) ----*/

SHOW OBJECTS IN SCHEMA WEATHER_SOURCE_LLC_FROSTBYTE.ONPOINT_ID;

/* ---- Step 3 - Table Structure Analysis ----*/

DESCRIBE TABLE ONPOINT_ID.forecast_day;

SELECT * 
FROM WEATHER_SOURCE_LLC_FROSTBYTE.ONPOINT_ID.forecast_day
LIMIT 1000;

/* ---- Step 4 - Initial Data Quality Checks ----*/

-- Check the total number of rows in the table
SELECT COUNT(*) AS rows_nmbr FROM onpoint_id.forecast_day;

-- Check non-null values for selected key columns
SELECT 
    COUNT(postal_code) AS postal_non_null,
    COUNT(city_name) AS city_non_null,
    COUNT(country) As country_non_null
FROM onpoint_id.forecast_day;

/* ---- Step 5 - Business Questions & SQL Analysis ----*/


--[1] Which cities provide the most complete weather data for trend analysis in the last year?
-- Before building long-term weather trend models, we want to identify cities with the most complete and reliable weather records over the past year, so our analyses and forecasts are based on consistent data rather than gaps or missing observations.
-- Find top 10 locations have the most frequent weather records over 2025

SELECT 
    city_name,
    country,
    COUNT(*) AS record_count
FROM onpoint_id.history_day
WHERE date_valid_std BETWEEN '2025-01-01' AND '2025-12-31'
GROUP BY city_name, country
ORDER BY record_count DESC
LIMIT 10;

-- [2] How consistent is weather data coverage over time across the most monitored locations in the last 2 years?
-- We want to understand whether our most frequently monitored locations have stable weather data coverage over time, ensuring that year-over-year comparisons and trend analyses are not affected by irregular data collection.
-- Analyze weather data coverage consistency over the last 2 years for the top 10 country–city combinations by total observations

WITH yearly_counts AS (
    -- First filtering the necessary date range and aggregate the yearly observation counts, this step will reduce data volume and improves performance
    SELECT
        country,
        city_name,
        EXTRACT(YEAR FROM date_valid_std) AS obs_year,
        COUNT(*) AS yearly_obs
    FROM onpoint_id.history_day
    WHERE date_valid_std >= '2024-01-01'
      AND date_valid_std < '2026-01-01'
    GROUP BY country, city_name, obs_year
),
top_locations AS (
    -- Identifying the top 10 country–city combinations based on total observations across the selected years
    SELECT
        country,
        city_name
    FROM yearly_counts
    GROUP BY country, city_name
    ORDER BY SUM(yearly_obs) DESC
    LIMIT 10
)
-- Joining back to yearly data to get the observation trend per year
SELECT
    y.country,
    y.city_name,
    y.obs_year,
    y.yearly_obs AS observation_count
FROM yearly_counts y
INNER JOIN top_locations t
    ON y.country = t.country
   AND y.city_name = t.city_name
ORDER BY y.country, y.city_name, y.obs_year;



-- [3] How do average monthly temperatures vary in high-activity cities in Europe and North America, and how could this influence seasonal menu planning in the last year?
-- Our food truck operates in multiple high-traffic cities across Europe and North America. By analyzing how monthly temperatures vary in these locations, we can adjust seasonal menus (hot vs. cold items) and staffing plans to better match local climate conditions.

-- I divided this investogation into two separate queries to reduce data volume and improve performance; the results from both queries I will compore in the further visualization step

-- Finding the average monthly temperature change in the high-activity cities of Europe region in 2025
SELECT DISTINCT
    CASE -- Normalizing city names to handle multiple variations
        WHEN city_name ILIKE '%paris%' THEN 'Paris'
        WHEN city_name ILIKE '%nice%' AND country = 'FR' THEN 'Nice' --added one more filter to ensure that the results only will be for Nice, FR since there is Nice locations in PL and GB as well
        WHEN city_name ILIKE '%berlin%' THEN 'Berlin'
        WHEN city_name ILIKE '%hamburg%' THEN 'Hamburg'
        WHEN city_name ILIKE '%munich%' 
            OR city_name ILIKE '%muenchen%' THEN 'Munich'
        WHEN city_name ILIKE '%madrid%' THEN 'Madrid'
        WHEN city_name ILIKE '%barcelona%' THEN 'Barcelona'
        WHEN city_name ILIKE '%stockholm%' THEN 'Stockholm'
        WHEN city_name ILIKE '%london%' THEN 'London'
        WHEN city_name ILIKE '%krakow%' 
            OR city_name ILIKE '%kraków%' THEN 'Krakow'
        WHEN city_name ILIKE '%warszawa%'
            OR city_name ILIKE '%warsaw%' THEN 'Warsaw'
        ELSE NULL
    END AS city,
        country, 
    EXTRACT(MONTH FROM date_valid_std) AS month,
    COUNT(*) AS observation_count,
    ROUND(AVG((avg_temperature_air_2m_f - 32) * 5.0 / 9), 1) AS avg_monthly_temp_c --converting measurments from Fahrenheit to Celsius
FROM onpoint_id.history_day
WHERE date_valid_std BETWEEN '2025-01-01' AND '2025-12-31'
AND country IN ('FR', 'DE', 'PL', 'ES', 'GB', 'SE')
AND city IS NOT NULL
GROUP BY city, country, month
ORDER BY city, month;


-- Finding the average monthly temperature change in the high-activity cities of North America region in 2025
SELECT DISTINCT
    CASE 
        WHEN city_name ILIKE '%denver%' THEN 'Denver'
        WHEN city_name ILIKE '%san mateo%' THEN 'San Mateo'
        WHEN city_name ILIKE '%seattle%' THEN 'Seattle'
        WHEN city_name ILIKE '%boston%' THEN 'Boston'
        WHEN city_name ILIKE '%new york%' THEN 'New York'
        WHEN city_name ILIKE '%toronto%' THEN 'Toronto'
        WHEN city_name ILIKE '%vancouver%' THEN 'Vancouver'
        WHEN city_name ILIKE '%montreal%' THEN 'Montreal'
        ELSE NULL
    END AS city,
        country,
    EXTRACT(MONTH FROM date_valid_std) AS month,
    COUNT(*) AS observation_count,
    ROUND(AVG((avg_temperature_air_2m_f - 32) * 5.0 / 9), 1) AS avg_monthly_temp_c --converting measurments from Fahrenheit to Celsius
FROM onpoint_id.history_day
WHERE date_valid_std BETWEEN '2025-01-01' AND '2025-12-31'
AND country IN ('US', 'CA')
AND city IS NOT NULL
GROUP BY city, country, month
ORDER BY city, month;


-- [4] Which locations experienced extreme winter conditions during this holiday season in the US, and how does it compare with the previous six years?
-- Holiday-season operations are highly sensitive to extreme winter weather. We want to identify locations that experienced unusually harsh winter conditions this season and compare them with prior years to assess operational risk, supply delays, and the need for seasonal inventory adjustments.

WITH top_high_winter_loc_2025 AS (
    -- Top cities in December 2025
    SELECT
        city_name,
        ROUND(SUM(tot_snowfall_in) * 2.54, 2) AS total_snowfall_cm, -- converting values from inches to centimeters
        ROUND(AVG(tot_snowdepth_in) * 2.54, 2) AS avg_snow_depth_cm
    FROM onpoint_id.history_day
    WHERE country = 'US'
      AND EXTRACT(YEAR FROM date_valid_std) = 2025
      AND EXTRACT(MONTH FROM date_valid_std) = 12
    GROUP BY city_name
    ORDER BY total_snowfall_cm DESC
    LIMIT 10
)

SELECT
    h.city_name,
    TO_VARCHAR(h.date_valid_std, 'YYYY-MM') AS month_year,
    ROUND(SUM(h.tot_snowfall_in) * 2.54, 2) AS total_snowfall_cm,
    ROUND(AVG(h.tot_snowdepth_in) * 2.54, 2) AS avg_snow_depth_cm
FROM onpoint_id.history_day h
JOIN top_high_winter_loc_2025 t
  ON h.city_name = t.city_name
WHERE h.country = 'US'
  AND EXTRACT(MONTH FROM h.date_valid_std) = 12
  AND EXTRACT(YEAR FROM h.date_valid_std) BETWEEN 2019 AND 2025
GROUP BY h.city_name, month_year
ORDER BY total_snowfall_cm DESC;


-- [5] Which European capital cities have the highest short-term precipitation risk that may reduce foot traffic over the next week?
-- Our team is planning short-term deployments in major European capital cities. By identifying cities with a high probability of precipitation in the coming week, we can adjust schedules, staffing, and inventory to minimize revenue loss due to reduced foot traffic.

WITH city_normalized AS (
    SELECT
    -- Normalize city names to handle multiple textual variations (for example, Paris stored as 'Paris 01', 'Paris 05', etc.) and setting the rest values as null for further investigation
        CASE
            WHEN city_name ILIKE 'paris%' THEN 'Paris' 
            WHEN city_name ILIKE 'berlin%' THEN 'Berlin'
            WHEN city_name ILIKE 'warszawa%' THEN 'Warsaw'
            WHEN city_name ILIKE 'madrid%' THEN 'Madrid'
            WHEN city_name ILIKE 'stockholm%' THEN 'Stockholm'
            WHEN city_name ILIKE 'london%' THEN 'London'
            ELSE NULL
        END AS city,
        country,
        probability_of_precipitation_pct,
        date_valid_std
    FROM onpoint_id.forecast_day
)

SELECT
    city,
    country,
    ROUND(AVG(probability_of_precipitation_pct), 1) AS avg_probability_pct
FROM city_normalized
-- filtered out non-target locations by excluding NULL values since cities outside the CASE logic were set to NULL
WHERE city IS NOT NULL
    -- Selecting date range - next week
 AND date_valid_std BETWEEN CURRENT_DATE() AND DATEADD(DAY, 7, CURRENT_DATE())
  -- Limit analysis to selected European capital cities since there are Paris, Berlin locations in the US as well
  AND country IN ('FR', 'DE', 'PL', 'ES', 'GB', 'SE')
GROUP BY city, country
ORDER BY avg_probability_pct DESC;



-- [6] Which cities across selected countries are forecasted to experience adverse weather conditions (rain, snow, strong wind, or extreme temperatures) over the next weekend?
-- Our operations span multiple countries including the US, several EU countries, Japan, and Australia. By analyzing short-term forecasts at the city level across key variables (precipitation, snow, wind, temperature, humidity), we can identify locations where adverse weather may disrupt outdoor operations and plan deployment and staffing accordingly.

WITH city_extremes AS (
    SELECT
        city_name,
        country,
        date_valid_std,
        tot_precipitation_in,
        tot_snowfall_in,
        avg_wind_speed_10m_mph,
        avg_temperature_air_2m_f,
        avg_temperature_feelslike_2m_f,
        avg_humidity_relative_2m_pct,
        avg_cloud_cover_tot_pct,
        avg_radiation_solar_total_wpm2,
        (tot_precipitation_in + tot_snowfall_in) AS storm_score, -- the combination of possible rain or snow i alliased as storm score
        ROW_NUMBER() OVER ( ---- added rank per city based on storm score
            PARTITION BY city_name 
            ORDER BY (tot_precipitation_in + tot_snowfall_in) DESC,
                     avg_wind_speed_10m_mph DESC,
                     ABS(avg_temperature_air_2m_f - 65) DESC -- 65F set as comfortable, bigger difference = more extreme
        ) AS rn
    FROM onpoint_id.forecast_day
    WHERE country IN ('US','FR','DE','PL','ES','GB','SE','JP','AU')
      AND EXTRACT(DOW FROM date_valid_std) IN (6, 0)  -- 6=Saturday, 0=Sunday
)

SELECT
    city_name,
    country,
    date_valid_std AS date,
    ROUND(ABS(avg_temperature_air_2m_f - 65), 1) AS avg_temperature_fahrenheit,
    tot_precipitation_in,
    tot_snowfall_in,
    avg_wind_speed_10m_mph,
    avg_humidity_relative_2m_pct,
    storm_score
FROM city_extremes
WHERE rn = 1  -- keep only the most extreme record per city
ORDER BY storm_score DESC, avg_wind_speed_10m_mph DESC, ABS(avg_temperature_air_2m_f - 65) DESC
LIMIT 25;




