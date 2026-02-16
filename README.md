# Project Objective
Weather Source LLC: frostbyte is a shared weather database from the Snowflake Marketplace that provides real-world historical weather data, including temperature, snowfall, precipitation, and snow depth by location and date.

## Goal
In this project, the dataset is used to analyze weather trends over time and explore how environmental factors can be joined with business data for deeper insights.
Analyze historical weather patterns to uncover seasonal trends, geographic differences, and temperature variability using Snowflake SQL, and present insights via Python dashboards.

## Repository Structure
```text
FROSTBYTE_WEATHERSOURCE/
├── README.md                                  # Project overview, steps, and insights
├── sql/
│ └── Frostbyte_WeatherSource_Snowflake.sql    # SQL analysis queries from Snowflake
└── notebooks/
└── Frostbyte_WeatherSource.ipynb              # Python visualization
```

## Project Plan
1. Data Source & Environment Setup (Snowflake)
2. Dataset Discovery (Schemas, Tables)
3. Table Structure Analysis (DESCRIBE TABLE)
4. Initial Data Quality Checks
5. Business Questions & SQL Analysis
6. Visualization in Python


# 1. Data Source & Environment Setup
## Data Source

• Snowflake Marketplace

• Dataset: WEATHER_SOURCE_LLC_FROSTBYTE

• Type: Public weather & climate data 

<img width="900" height="650" alt="image" src="https://github.com/user-attachments/assets/e3f4ec1b-5878-4199-a51a-94269c953c62" />

## Setting up the environment

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

Additionally, I used the simple SELECT all (*) to see the view of the table:
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

Because the table contains 59 columns and a large number of rows, null checks were limited to key geographic fields that are most likely to be used in the following analysis

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
-- Find top 10 locations that have the most frequent weather records over 2025
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
    -- First filtering the necessary date range and aggregate the yearly observation counts. This step will reduce data volume and improve performance
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
-- Joining back to the yearly data to get the observation trend per year
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

I divided this investigation into two separate queries to reduce data volume and improve performance; the results from both queries I will compare in the further visualization step
```sql
-- Finding the average monthly temperature change in the high-activity cities of Europe region in 2025
SELECT DISTINCT
    CASE -- Normalizing city names to handle multiple variations
        WHEN city_name ILIKE '%paris%' THEN 'Paris'
        WHEN city_name ILIKE '%nice%' AND country = 'FR' THEN 'Nice' --added one more filter to ensure that the results will only be for Nice, FR since there are Nice locations in PL and GB as well
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
-- Finding the average monthly temperature change in the high-activity cities of the North America region in 2025
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

# 7. Visualization in Python

```python
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# 1. Which cities provide the most complete weather data for trend analysis in the last year?
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/01_business_question.csv')
print(df)
```
<img width="1102" height="196" alt="Screenshot 2026-02-12 at 2 08 59 PM" src="https://github.com/user-attachments/assets/9c8d85be-1ea2-46e2-8960-7ba655b1e68a" />

```python
# Creating a bar chart
plt.figure()
plt.bar(df["CITY_NAME"], df["RECORD_COUNT"])

# Setting labels and title
plt.xlabel("City")
plt.ylabel("Weather Records Count")
plt.title("Top 10 Locations with the Most Frequent Weather Records Over Time")

plt.xticks(rotation=45)

# Converting Y-axis values into millions format
def format_millions(x, pos):
    return f'{x/1_000_000:.1f}M'

plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(format_millions))
plt.tight_layout()
plt.show()
```
<img width="1103" height="469" alt="Screenshot 2026-02-12 at 2 10 00 PM" src="https://github.com/user-attachments/assets/91ae7ed3-d78d-48e9-afe5-a7206f16ecf7" />

```python
# 2. How consistent is weather data coverage over time across the most monitored locations in the last 2 years?
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/02_business_question.csv')
print(df)
```
<img width="1096" height="372" alt="Screenshot 2026-02-13 at 10 03 33 AM" src="https://github.com/user-attachments/assets/5b3fd433-b8fe-4dc9-ac9d-534831bf0735" />

```python
import numpy as np

# Pivot table to divide years into two separate columns: 2024 and 2025
df_pivot = df.pivot(index="CITY_NAME", columns="OBS_YEAR", values="OBSERVATION_COUNT").reset_index()

# Sorting by 2025 for better visibility
df_pivot = df_pivot.sort_values(by=2025, ascending=False)

df_pivot
```
<img width="1096" height="327" alt="Screenshot 2026-02-13 at 10 05 18 AM" src="https://github.com/user-attachments/assets/45113392-9619-420e-8877-d9eae8736444" />

```python
# Positions for bars
x = np.arange(len(df_pivot))  # city positions
width = 0.35 #setting the width of bars

# Creating figure
fig, ax = plt.subplots(figsize=(12,6))

# Creating bars and setting colors
bars1 = ax.bar(x - width/2, df_pivot[2024], width, label='2024', color='skyblue')
bars2 = ax.bar(x + width/2, df_pivot[2025], width, label='2025', color='salmon')

# Setting labels and title
ax.set_xlabel("City")
ax.set_ylabel("Weather Records Count")
ax.set_title("Top 10 Cities Weather Records Comparison: 2024 vs 2025")
ax.set_xticks(x)
ax.set_xticklabels(df_pivot["CITY_NAME"], rotation=45)

# Converting Y-axis values into millions format
def format_millions(y, pos):
    return f'{y/1_000_000:.1f}M'

ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_millions))

# Legend for 2024 and 2025 color defining
ax.legend()

plt.tight_layout()
plt.show()
```
<img width="1102" height="550" alt="Screenshot 2026-02-13 at 10 06 11 AM" src="https://github.com/user-attachments/assets/9a4290a6-e2d2-4bdf-99c8-7d3f622a018b" />

```python
# 3. How do average monthly temperatures vary in high-activity cities in Europe and North America, and how could this influence seasonal menu planning in the last year?
# 3.1 High-activity cities in Europe
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/03.1_business_question.csv')
print(df)
```
<img width="1096" height="259" alt="Screenshot 2026-02-13 at 10 07 24 AM" src="https://github.com/user-attachments/assets/55aca073-5835-4bfd-b2b2-3287037de30c" />

```python
# Getting unique EU cities
cities = df["CITY"].unique()

# Creating figure
plt.figure(figsize=(12,6))

# Plotting each city
for city in cities:
    city_data = df[df["CITY"] == city]
    plt.plot(city_data["MONTH"], city_data["AVG_MONTHLY_TEMP_C"],
             marker='o', label=city)

# Setting labels and title
plt.xlabel("Month")
plt.ylabel("Average Monthly Temperature (°C)")
plt.title("Monthly Temperature Change for High-Activity Cities in Europe")
plt.xticks(range(1,13))  # ensuring X axis shows months 1–12
plt.legend(title="City") # creating legend for cities for color defining
plt.grid(True)

plt.tight_layout()
plt.show()
```
<img width="1094" height="546" alt="Screenshot 2026-02-13 at 10 08 36 AM" src="https://github.com/user-attachments/assets/57ca7f1d-c889-4e2d-b41d-c1e71d98ce64" />

```python
# 3.1 High-activity cities in North America
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/03.2_business_question.csv')
print(df)
```
<img width="1096" height="252" alt="Screenshot 2026-02-13 at 10 09 30 AM" src="https://github.com/user-attachments/assets/a81a609b-a8b0-460a-bc4c-af5feb9c1853" />

```python
# Getting unique North America cities
cities = df["CITY"].unique()

# Creating figure
plt.figure(figsize=(12,6))

# Plotting each city
for city in cities:
    city_data = df[df["CITY"] == city]
    plt.plot(city_data["MONTH"], city_data["AVG_MONTHLY_TEMP_C"],
             marker='o', label=city)

# Setting labels and title
plt.xlabel("Month")
plt.ylabel("Average Monthly Temperature (°C)")
plt.title("Monthly Temperature Change for High-Activity Cities in North America")
plt.xticks(range(1,13))  # ensuring X axis shows months 1–12
plt.legend(title="City") # creating legend for cities for color defining
plt.grid(True)

plt.tight_layout()
plt.show()
```
<img width="1096" height="547" alt="Screenshot 2026-02-13 at 10 11 41 AM" src="https://github.com/user-attachments/assets/54664dfe-7d0c-4246-8d1a-7350a4add648" />

```python
# 4. Which locations experienced extreme winter conditions during this holiday season in the US, and how does it compare with the previous six years?
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/04_business_question.csv')
print(df)
```
<img width="1096" height="256" alt="Screenshot 2026-02-13 at 10 33 40 AM" src="https://github.com/user-attachments/assets/ba8f0b13-62a4-4c33-a94d-1c4ccc4b65b6" />

```python
# Pivot total snowfall to have a separate column for each year rather than rows
df_pivot_snowfall = df.pivot(index="CITY_NAME", columns="MONTH_YEAR", values="TOTAL_SNOWFALL_CM").reset_index()

df_pivot_snowfall
```
<img width="1096" height="324" alt="Screenshot 2026-02-13 at 10 34 53 AM" src="https://github.com/user-attachments/assets/5c462c6e-2fde-4361-bfac-26683b47520e" />

```python
# Plotting years
years = ['2019-12', '2020-12', '2022-12', '2023-12', '2025-12'] 

# Creating figure
x = np.arange(len(df_pivot_snowfall))  # positions for cities
width = 0.15  # setting the width of bars

fig, ax = plt.subplots(figsize=(12,6))

# Plotting bars for each year
for i, year in enumerate(years):
    ax.bar(x + i*width - width*(len(years)-1)/2, df_pivot_snowfall[year], width, label=str(year))

# Setting labels and title
ax.set_xlabel("City")
ax.set_ylabel("Total Snowfall (cm)")
ax.set_title("Top US Cities with Extreme Total Snowfall per Year (2019–2025)")
ax.set_xticks(x)
ax.set_xticklabels(df_pivot_snowfall["CITY_NAME"], rotation=45)



ax.legend(title="Year")

plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
```
<img width="1095" height="543" alt="Screenshot 2026-02-13 at 10 35 30 AM" src="https://github.com/user-attachments/assets/33093ca0-72cb-4d38-9a86-b9dc18aec8ac" />

```python
# 5. Which European capital cities have the highest short-term precipitation risk that may reduce foot traffic over the next week?
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/05_business_question.csv')
print(df)
```
<img width="1090" height="133" alt="Screenshot 2026-02-13 at 10 36 09 AM" src="https://github.com/user-attachments/assets/02db8503-1a32-463f-92e2-fb17267b0e34" />

```python
# Creating figure
plt.figure(figsize=(8, 5))

bars = plt.barh(
    df_sorted["CITY"],
    df_sorted["AVG_PROBABILITY_PCT"]
)

plt.xlabel("Average Probability (%)")
plt.title("European Capitals with the Highest Precipitation Next Week")

# Adding value of possible precipitation risk (%) labels at the end of each bar
for index, value in enumerate(df_sorted["AVG_PROBABILITY_PCT"]):
    plt.text(value + 1, index, f"{value:.1f}%", ha="right", fontsize=9, color='black', fontweight="bold")

plt.tight_layout()
plt.show()
```
<img width="1091" height="496" alt="Screenshot 2026-02-13 at 10 36 42 AM" src="https://github.com/user-attachments/assets/decedf62-3664-49b8-8910-908937cf92eb" />

```python
# 6. Which cities across selected countries are forecasted to experience adverse weather conditions (rain, snow, strong wind, or extreme temperatures) over the next weekend?
# Import .csv file
df = pd.read_csv('/Users/martanarozhnyak/Desktop/business_questions_outputs/06_business_question.csv')
df = df.head(15)
df["CITY_COUNTRY"] = df["CITY_NAME"].astype(str) + ", " + df["COUNTRY"].astype(str)
print(df)
```
```python
# Creating figure
plt.figure(figsize=(8, 5))

# Setting color palette based on storm score
colors = [
    "darkred" if score > 10.10
    else "firebrick" if score >= 10.00
    else "indianred"
    for score in df["STORM_SCORE"]
]

bars = plt.barh(
    df["CITY_COUNTRY"],
    df["STORM_SCORE"],
    color=colors
)

plt.xlabel("Storm Score")
plt.title("Location with Possible Adverse Weather Conditions Over Next Week")


plt.tight_layout()
plt.show()
```
<img width="1091" height="496" alt="Screenshot 2026-02-13 at 10 38 07 AM" src="https://github.com/user-attachments/assets/f891a316-f465-4330-a9eb-fc0c56174e25" />










