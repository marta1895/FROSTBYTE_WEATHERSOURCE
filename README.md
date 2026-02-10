# Project Objective
Weather Source LLC: frostbyte is a shared weather database from the Snowflake Marketplace that provides real-world historical weather data, including temperature, snowfall, precipitation, and snow depth by location and date.

## Goal:
In this project, the dataset is used to analyze weather trends over time and explore how environmental factors can be joined with business data for deeper insights.
Analyze historical weather patterns to uncover seasonal trends, geographic differences, and temperature variability using Snowflake SQL, and present insights via Tableau dashboards.

## Project Plan:
1. Data Source & Environment Setup (Snowflake)
2. Dataset Discovery (Schemas, Tables)
3. Table Structure Analysis (DESCRIBE TABLE)
4. Initial Data Quality Checks
5. Business Questions & SQL Analysis
6. Visualization in Tableau


# 1. Data Source & Environment Setup
## Data Source

• Snowflake Marketplace

• Dataset: WEATHER_SOURCE_LLC_FROSTBYTE

• Type: Public weather & climate data 

<img width="900" height="650" alt="image" src="https://github.com/user-attachments/assets/e3f4ec1b-5878-4199-a51a-94269c953c62" />

## Setting up the enviroment

After the database installation and before querying, we set the role, warehouse, database and schema context to ensure queries run in the correct dataset environment.

```sql
USE ROLE ACCOUNTADMIN; 
USE WAREHOUSE COMPUTE_WH;
USE DATABASE WEATHER_SOURCE_LLC_FROSTBYTE;
USE SCHEMA WEATHER_SOURCE_LLC_FROSTBYTE.ONPOINT_ID;
```
# 2. Dataset Discovery (Schemas, Tables)
## Checking all object types in the schema

Before analysis, I explored the schema to identify available objects and understand which tables (views) are in the dataset. This helped determine which tables are relevant for further analysis.

```sql
SHOW OBJECTS IN SCHEMA WEATHER_SOURCE_LLC_FROSTBYTE.ONPOINT_ID;
```
<img width="1335" height="385" alt="Screenshot 2026-02-10 at 11 54 21 AM" src="https://github.com/user-attachments/assets/12f4c5f8-610c-42b4-94f5-bf07ad1d6ae0" />
From the results, the schema contains 7 views (tables).

# 3. Table Structure Analysis (DESCRIBE TABLE)
## Checking the table structure

Then, DESCRIBE TABLE was used to inspect the table structure in detail. This step provides information about column names, data types, and nullability constraints, which helps understand how the data is structured before performing any analytical queries.

Example using the forecast_day table:

```sql
DESCRIBE TABLE ONPOINT_ID.forecast_day;
```
<img width="1333" height="672" alt="Screenshot 2026-02-10 at 12 08 01 PM" src="https://github.com/user-attachments/assets/f7df2233-f613-414a-99c3-8ddc74b6ed56" />

Additionaly, I used the simple SELECT all (*) to see the view of the table:
```sql
SELECT *
FROM WEATHER_SOURCE_LLC_FROSTBYTE.ONPOINT_ID.forecast_day
LIMIT 1000;
```
<img width="1324" height="376" alt="Screenshot 2026-02-10 at 12 13 34 PM" src="https://github.com/user-attachments/assets/1fe14b00-3ccc-4088-be71-e7e90302962d" />

# 4. Initial Data Quality Checks
## Checking the rows number and null values

```sql
-- Check the total number of rows in the table
SELECT COUNT(*) AS rows_nmbr FROM onpoint_id.forecast_day;
```
<img width="1334" height="144" alt="Screenshot 2026-02-10 at 12 06 55 PM" src="https://github.com/user-attachments/assets/2edaa50c-6da6-40bb-92e4-8bf55f37a770" />

Because the table contains 59 columns and a large number of rows, null checks were limited to key geographic fields that are most likely to be used in following analysis

```sql
-- Check non-null values for selected key columns
SELECT 
    COUNT(postal_code) AS postal_non_null,
    COUNT(city_name) AS city_non_null,
    COUNT(country) As country_non_null
FROM onpoint_id.forecast_day;
```
<img width="1326" height="143" alt="Screenshot 2026-02-10 at 12 16 09 PM" src="https://github.com/user-attachments/assets/67165488-ea2e-4239-87e6-4b5887a77172" />

# 5. Business Questions & SQL Analysis
## (1) Which cities provide the most complete weather data for trend analysis in the last year?
Before building long-term weather trend models, we want to identify cities with the most complete and reliable weather records over the past year, so our analyses and forecasts are based on consistent data rather than gaps or missing observations
```sql
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
```
<img width="1333" height="390" alt="Screenshot 2026-02-10 at 12 19 40 PM" src="https://github.com/user-attachments/assets/afad907f-a5bc-4252-b822-a21032e1bbed" />

## (2) How consistent is weather data coverage over time across the most monitored locations in the last 2 years?
We want to understand whether our most frequently monitored locations have stable weather data coverage over time, ensuring that year-over-year comparisons and trend analyses are not affected by irregular data collection
```sql
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
```
## (3) How do average monthly temperatures vary in high-activity cities in Europe and North America, and how could this influence seasonal menu planning in the last year?
Our food truck operates in multiple high-traffic cities across Europe and North America. By analyzing how monthly temperatures vary in these locations, we can adjust seasonal menus (hot vs. cold items) and staffing plans to better match local climate conditions

I divided this investogation into two separate queries to reduce data volume and improve performance; the results from both queries I will compore in the further visualization step
```sql
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
```
<img width="1333" height="697" alt="Screenshot 2026-02-10 at 12 28 31 PM" src="https://github.com/user-attachments/assets/e96f91eb-77d2-462e-9246-8770df39e4a8" />

```sql
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
```
<img width="1333" height="664" alt="Screenshot 2026-02-10 at 12 30 55 PM" src="https://github.com/user-attachments/assets/8733d9cb-4077-4701-ba04-f9f3f4bdbf83" />
