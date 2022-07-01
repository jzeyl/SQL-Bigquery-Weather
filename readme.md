# Objective

This portfolio project demonstrates SQL queries, Python, and visualization with Tableau. The queries are called called in python code as strings, but also supplied here in a separate SQL file for easier visualization. Dataframes are exported as csv and loaded into Tableau.

# Tools
SQL | Python | Tableau

# Data 
The SQL queries are running on the public weather data provided by NOAA, the [Global Surface Summary of the Day Weather Data in BigQuery](https://console.cloud.google.com/marketplace/details/noaa-public/gsod). 
I'm using this data to focust on the heatwave that occurred in western Canada in 2021. You can read more about the [heatwave on Wikipedia](https://en.wikipedia.org/wiki/2021_Western_North_America_heat_wave). 

# Queries
1. find all the stations in Canada (basic  query)
2. get temperatures in Lytton, BC and Kamloops, BC during June and July 2021 (JOIN, LIKE, BETWEEN)
3. get historical max temperature per month, historically, for Lytton stations (UNION)
4. check the date and temperature on Lytton's hottest day (nested query)
5. Count the days for all stations in Canada where the temperature was 35 or greater (window function)

# Dashboard
![](https://github.com/jzeyl/SQL-Bigquery-Weather/blob/main/Tableau%20heat%20wave.png)
