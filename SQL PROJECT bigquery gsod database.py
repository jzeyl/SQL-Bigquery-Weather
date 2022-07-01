#The objective is to identify interesting patterns including the 2021 heat wave in western north america in June-July of 2021
#The database includes a table with information on weather stations, and then a different
#table for each year of data collection. 


from google.cloud import bigquery
import os
from google.cloud.bigquery.client import Client
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


#connect to bigquery using my local API key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\jeffz\Downloads\planar-truck-322401-fa6532b913fc.json"

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the dataset (i.e., a database with several tables)
dataset_ref = client.dataset("noaa_gsod",project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the dataset
tables = list(client.list_tables(dataset_ref))

# Print names of all tables in the dataset
for table in tables:  
    print(table.table_id)

# Construct a reference to the "full" table
table_ref = dataset_ref.table("gsod2021") #"stations"

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema
for item in table.schema:
    print(item)

# Preview the first five lines of the "full" table
client.list_rows(table, max_results=5).to_dataframe()

#####QUERIES
 
########find all the stations in Canada########(basic  query)
wbaninCan = """
        SELECT usaf,wban, name, lat, lon
        FROM `bigquery-public-data.noaa_gsod.stations`
        WHERE country = 'CA' 
        """         
wbaninCan_df = client.query(wbaninCan).to_dataframe()

# save to cscv
wbaninCan_df.to_csv(r"C:\Users\jeffz\Desktop\data analysis projects\weather bigquery\weather bigquery\stationsincanada.csv")

#get temperatures in lytton, BC and kamloops, BC during June and july 2021 (JOIN, LIKE, BETWEEN)
lyttontempwave = """
SELECT date, stn,max,(max-32)*5/9 celsius, name, lat, lon
FROM `bigquery-public-data.noaa_gsod.gsod2021` dat
JOIN `bigquery-public-data.noaa_gsod.stations` station
ON dat.stn=station.usaf
AND dat.wban=station.wban
WHERE (date between '2021-06-01' and '2021-07-30') AND (name LIKE '%KAMLOOPS%' OR name LIKE '%LYTTON%')  
ORDER BY date
"""

lyttontempwave_df = client.query(lyttontempwave).to_dataframe()
lyttontempwave_df.to_csv(r"C:\Users\jeffz\Desktop\data analysis projects\weather bigquery\weather bigquery\lyttontempwave_df.csv")
   

#get historical max temperature per month, historically, for lytton stations (UNION)
unionmontlytemp = """
select year, mo, max(max-32)*5/9 celsius, stn 
 FROM (
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2011`
               UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2012`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2013`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2014`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2015`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2016`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2017`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2018`
                UNION ALL
       SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2019`
                UNION ALL
                SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2020` 
                UNION ALL
                SELECT  year, mo, temp, stn, max FROM `bigquery-public-data.noaa_gsod.gsod2021`   
) 
where stn = '718910' OR stn = '717650'
group by year, mo, stn
order by year, mo, stn
"""

union_years = client.query(unionmontlytemp).to_dataframe()
union_years.to_csv(r"C:\Users\jeffz\Desktop\data analysis projects\weather bigquery\weather bigquery\union_years.csv")


#query to check the validity of the data (nested query)
# if we query for the max  temperature of Lytton, we should get Jun 30, when the record was brooken
#query:  check lytton's hottest day (nested query)
#Here i use a subquery to extract more data from the row where the highest temperature was recorded in London for 2021
hottestday_lttn = """
select date, stn, max,(max-32)*5/9 celsius
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE max = (SELECT  max(max) as maxtemp
FROM `bigquery-public-data.noaa_gsod.gsod2021`
WHERE stn = '718910') AND stn = '718910' 
"""
#indeed, hottest day is on Jun 29-30 (48 C. Note the other Lytton station recorded the record 49 C)
hottestday_df_lttn = client.query(hottestday_lttn).to_dataframe()


#To show the geographic location of the heatwave ()
# Count the days for all stations in Canada where the temperature was 35 or greater(window function)
count_tempwave = """
SELECT date, stn,max,(max-32)*5/9  celsius, name, lat, lon, 
CASE WHEN (max-32)*5/9 > 34 then '35 or above' END heat,
COUNT(CASE WHEN (max-32)*5/9 > 34 then '35 or above' END) OVER(PARTITION BY name) daysover34
FROM `bigquery-public-data.noaa_gsod.gsod2021` dat
JOIN `bigquery-public-data.noaa_gsod.stations` station
ON dat.stn=station.usaf
AND dat.wban=station.wban
WHERE (date between '2021-06-25' and '2021-07-07') AND (country = 'CA') 
GROUP BY date, stn,max, name, lat, lon
ORDER BY date, stn,max, name, lat, lon
"""

count_tempwave_df = client.query(count_tempwave).to_dataframe()
count_tempwave_df.to_csv(r"C:\Users\jeffz\Desktop\data analysis projects\weather bigquery\weather bigquery\count_tempwave.csv")
